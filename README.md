# 🚀 Data Ingestion Tool – ClickHouse & CSV Integration

A powerful, user-friendly web tool to facilitate **bidirectional data transfer** between **ClickHouse databases** and **CSV flat files**. Designed with scalability and simplicity in mind, this tool is ideal for developers, analysts, and data engineers who need quick and efficient data migration or export.

---

## 📌 Features

- 🔄 **ClickHouse → CSV Export**
- 📥 **CSV → ClickHouse Import**
- ✅ Column selection before ingestion
- ⚡ Stream-based large dataset handling
- 🔐 JWT/password authentication for ClickHouse
- 🎛️ User-friendly, step-by-step UI workflow

---

## 🛠️ Technologies Used

| Layer     | Tech Stack                         |
|-----------|------------------------------------|
| Frontend  | React, Axios, CSS                  |
| Backend   | Node.js, Express, Multer           |
| Database  | ClickHouse                         |
| Parsing   | csv-parser (Node.js stream-based)  |
| ClickHouse Client | `@clickhouse/client`       |

---

## 📂 Project Structure

```bash
├── client/                 # React frontend
│   ├── components/
│   ├── App.js
│   └── ...
├── server/                 # Node.js backend
│   ├── routes/
│   ├── controllers/
│   └── server.js
├── uploads/               # Uploaded CSV files
├── README.md
└── package.json
