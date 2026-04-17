# Frontend — Driving Simulation Web App

This is the React/TypeScript frontend for the HAHD data collection application. Participants use this web app to watch 15-second driving clips and answer behavioral survey questions while eye-tracking data is recorded.

## Tech Stack
- **React 18** with **TypeScript**
- **Vite** for bundling and dev server
- **Tailwind CSS** / **shadcn/ui** for UI components

## Setup

### Prerequisites
- Node.js v18+
- Backend server running (see [server README](../server/README.md))

### Install Dependencies

```bash
cd frontend
npm install
```

### Configure Environment

Create a `.env` file in the `frontend/` directory:

```
VITE_API_URL=http://localhost:3000
```

### Run Development Server

```bash
npm run dev
```

The app will be available at `http://localhost:5173`.

### Build for Production

```bash
npm run build
```

## Directory Structure

```
frontend/
├── public/           # Static assets
├── src/
│   ├── assets/       # Images and media (videos stored in S3, not committed)
│   ├── components/   # Reusable React components
│   ├── pages/        # Page-level components
│   └── main.tsx      # App entry point
├── index.html
├── vite.config.ts
└── package.json
```
