const express = require('express');
const multer = require('multer');
const { createClient } = require('@clickhouse/client');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');

const app = express();
const upload = multer({ dest: 'uploads/' });

app.use(express.json());

// Improved ClickHouse client helper with JWT support
async function getClickHouseClient(config, isTarget = false) {
  const prefix = isTarget ? 'target_' : '';
  const options = {
    host: config[`${prefix}host`] || 'localhost',
    username: config[`${prefix}username`] || 'default',
    password: config[`${prefix}password`] || '',
    database: config[`${prefix}database`] || 'default',
  };

  // Handle port with proper type conversion
  if (config[`${prefix}port`]) {
    options.port = parseInt(config[`${prefix}port`]);
  }

  // Handle JWT token if provided
  if (config[`${prefix}jwtToken`]) {
    options.password = `Bearer ${config[`${prefix}jwtToken`]}`;
  }

  // Handle secure connection
  options.protocol = config[`${prefix}secure`] ? 'https:' : 'http:';

  // Add ClickHouse settings for better performance
  options.clickhouse_settings = {
    async_insert: 1,
    wait_for_async_insert: 1,
  };

  return createClient(options);
}

// API to fetch tables from ClickHouse
app.post('/api/fetch-tables', async (req, res) => {
  try {
    const client = await getClickHouseClient(req.body);
    const result = await client.query({
      query: 'SHOW TABLES',
      format: 'JSONEachRow'
    });
    const tables = await result.json();
    res.json({ tables: tables.map(t => Object.values(t)[0]) });
  } catch (error) {
    console.error('Error fetching tables:', error);
    res.status(500).json({ error: 'Failed to fetch tables. Check connection parameters and authentication.' });
  }
});

// API to fetch columns from ClickHouse
app.post('/api/fetch-columns', async (req, res) => {
  try {
    const { table, ...config } = req.body;
    if (!table) {
      return res.status(400).json({ error: 'Table name is required' });
    }

    const client = await getClickHouseClient(config);
    const result = await client.query({
      query: `DESCRIBE TABLE ${table}`,
      format: 'JSONEachRow'
    });
    const columns = await result.json();
    
    const columnNames = columns.map(col => col.name);
    const columnTypes = {};
    columns.forEach(col => {
      columnTypes[col.name] = col.type;
    });

    res.json({ columns: columnNames, types: columnTypes });
  } catch (error) {
    console.error('Error fetching columns:', error);
    res.status(500).json({ error: 'Failed to fetch columns. Verify table exists and you have permissions.' });
  }
});

// API to fetch columns from flat file
app.post('/api/fetch-file-columns', upload.single('file'), async (req, res) => {
  try {
    const { delimiter = ',', hasHeader = true } = req.body;
    if (!req.file) {
      return res.status(400).json({ error: 'File is required' });
    }

    const results = [];
    const columnTypes = {};
    let columns = [];

    return new Promise((resolve, reject) => {
      fs.createReadStream(req.file.path)
        .pipe(csv({
          separator: delimiter,
          headers: hasHeader ? undefined : false,
          mapValues: ({ value }) => {
            // Enhanced type detection
            if (value === 'true' || value === 'false') return value === 'true';
            if (!isNaN(value) && value.trim() !== '') return Number(value);
            if (value.match(/^\d{4}-\d{2}-\d{2}/)) return value; // Date detection
            return value;
          }
        }))
        .on('headers', (headers) => {
          columns = headers;
        })
        .on('data', (data) => {
          results.push(data);
          // Infer types from first few rows
          if (results.length <= 10) {
            Object.keys(data).forEach(key => {
              const val = data[key];
              if (val === null || val === undefined) {
                columnTypes[key] = 'Nullable(String)';
              } else if (typeof val === 'number') {
                columnTypes[key] = 'Float64';
              } else if (typeof val === 'boolean') {
                columnTypes[key] = 'UInt8';
              } else if (typeof val === 'string' && val.match(/^\d{4}-\d{2}-\d{2}/)) {
                columnTypes[key] = 'Date';
              } else if (!isNaN(val) && val.toString().trim() !== '') {
                columnTypes[key] = 'Float64';
              } else {
                columnTypes[key] = 'String';
              }
            });
          }
        })
        .on('end', () => {
          if (!hasHeader && results.length > 0) {
            columns = Object.keys(results[0]).map((_, i) => `column_${i + 1}`);
          }
          fs.unlinkSync(req.file.path);
          res.json({ columns, types: columnTypes });
          resolve();
        })
        .on('error', (error) => {
          fs.unlinkSync(req.file.path);
          reject(error);
        });
    });
  } catch (error) {
    console.error('Error processing file:', error);
    res.status(500).json({ error: 'Failed to process file. Check file format and delimiter.' });
  }
});

// API to start data ingestion
app.post('/api/start-ingestion', upload.single('file'), async (req, res) => {
  try {
    const { sourceType, targetType, selectedColumns } = req.body;
    const columns = JSON.parse(selectedColumns);
    
    if (!sourceType || !targetType || !columns || columns.length === 0) {
      return res.status(400).json({ error: 'Invalid parameters' });
    }

    let recordCount = 0;

    if (sourceType === 'ClickHouse' && targetType === 'Flat File') {
      // ClickHouse to CSV export
      const client = await getClickHouseClient(req.body);
      const table = req.body.table;
      
      const query = `SELECT ${columns.map(col => col.name).join(', ')} FROM ${table}`;
      const result = await client.query({
        query: query,
        format: 'CSVWithNames'
      });
      
      const csvData = await result.text();
      const fileName = `export_${Date.now()}.csv`;
      const filePath = path.join(__dirname, 'exports', fileName);
      
      if (!fs.existsSync(path.join(__dirname, 'exports'))) {
        fs.mkdirSync(path.join(__dirname, 'exports'));
      }
      
      fs.writeFileSync(filePath, csvData);
      recordCount = csvData.split('\n').length - 1; // Subtract header
      
      res.json({
        recordCount,
        message: `Data exported to ${fileName}`,
        downloadUrl: `/exports/${fileName}`
      });
    } 
    else if (sourceType === 'Flat File' && targetType === 'ClickHouse') {
      // CSV to ClickHouse import
      if (!req.file) {
        return res.status(400).json({ error: 'File is required' });
      }

      const client = await getClickHouseClient(req.body, true);
      const table = req.body.target_table || `imported_${Date.now()}`;
      const delimiter = req.body.delimiter || ',';
      const hasHeader = req.body.hasHeader === 'true';
      
      // Create table with proper schema
      const createTableQuery = `
        CREATE TABLE IF NOT EXISTS ${table} (
          ${columns.map(col => `${col.name} ${col.type || 'String'}`).join(',\n')}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
      `;
      
      await client.command({
        query: createTableQuery
      });
      
      // Stream data into ClickHouse
      const stream = fs.createReadStream(req.file.path)
        .pipe(csv({
          separator: delimiter,
          headers: hasHeader ? columns.map(col => col.name) : false,
          mapValues: ({ header, index, value }) => {
            const col = columns.find(c => c.name === (hasHeader ? header : `column_${index + 1}`));
            if (!col) return null;
            
            // Handle null values
            if (value === '' || value === null) return null;
            
            // Convert based on column type
            if (col.type.includes('Int')) {
              return parseInt(value);
            } else if (col.type.includes('Float')) {
              return parseFloat(value);
            } else if (col.type.includes('Date')) {
              return value; // ClickHouse will parse the date string
            }
            return value;
          }
        }));
      
      let batch = [];
      const BATCH_SIZE = 10000;
      
      for await (const row of stream) {
        batch.push(row);
        if (batch.length >= BATCH_SIZE) {
          await client.insert({
            table: table,
            values: batch,
            columns: columns.map(col => col.name),
            format: 'JSONEachRow'
          });
          batch = [];
        }
      }
      
      // Insert remaining rows
      if (batch.length > 0) {
        await client.insert({
          table: table,
          values: batch,
          columns: columns.map(col => col.name),
          format: 'JSONEachRow'
        });
      }
      
      fs.unlinkSync(req.file.path);
      
      // Get final count
      const countResult = await client.query({
        query: `SELECT count() FROM ${table}`,
        format: 'JSONEachRow'
      });
      const countData = await countResult.json();
      recordCount = countData[0]['count()'];
      
      res.json({
        recordCount,
        message: `Data imported to ClickHouse table ${table}`,
        tableName: table
      });
    } else {
      res.status(400).json({ error: 'Unsupported source/target combination' });
    }
  } catch (error) {
    console.error('Error during ingestion:', error);
    res.status(500).json({ 
      error: 'Ingestion failed', 
      message: error.message,
      details: error.stack 
    });
  }
});

// Serve exported files
app.use('/exports', express.static(path.join(__dirname, 'exports')));

// Error handling middleware
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  res.status(500).json({
    error: 'Internal server error',
    message: err.message
  });
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});