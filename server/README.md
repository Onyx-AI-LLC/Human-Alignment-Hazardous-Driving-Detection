# Backend — Data Collection Web App

This is the Node.js/Express backend for the HAHD driving simulation web application. It handles user authentication, session management, and stores participant survey responses and gaze data in MongoDB.

## Tech Stack
- **Node.js** with **Express**
- **MongoDB** (via Mongoose)
- **AWS S3** for video asset retrieval

## Setup

### Prerequisites
- Node.js v18+
- A running MongoDB instance (local or Atlas)
- AWS credentials with S3 read access

### Install Dependencies

```bash
cd server
npm install
```

### Configure Environment Variables

Create a `.env` file in the project root with:

```
MONGO_URI=mongodb+srv://<user>:<password>@<cluster>.mongodb.net/<dbname>
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=us-east-1
S3_BUCKET_NAME=hahd-primary-data-storage
PORT=3000
```

### Run the Server

```bash
node server.js
```

The API will be available at `http://localhost:3000`.

## Directory Structure

```
server/
├── config/       # Database and AWS configuration
├── middleware/   # Auth and request middleware
├── models/       # Mongoose data models
├── routes/       # Express route handlers
├── services/     # Business logic (S3, data processing)
├── utils/        # Helper utilities
└── server.js     # Entry point
```

## API Overview

| Method | Route | Description |
|--------|-------|-------------|
| POST | `/api/users/register` | Register a new participant |
| POST | `/api/users/login` | Authenticate a participant |
| GET | `/api/videos` | Fetch simulation video list from S3 |
| POST | `/api/responses` | Submit gaze and survey response data |
