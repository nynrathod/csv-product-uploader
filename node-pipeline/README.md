# CSV Product Uploader

A full-stack application for uploading, processing, and browsing CSV files containing product data with real-time exchange rates.

## Features

- **CSV Upload** - Upload CSV files up to 50MB (supports 200k+ rows)
- **Real-time Processing** - Asynchronous background job processing with progress tracking
- **Exchange Rates** - Captures 5+ currency exchange rates at upload time
- **Product Browse** - View uploaded products in a searchable, sortable table
- **Filtering & Sorting** - Filter by name, price range, expiration date
- **Authentication** - Simple user authentication with JWT tokens

## Tech Stack

**Backend:**

- NestJS (TypeScript)
- PostgreSQL + TypeORM
- Bull (Job Queue)
- Axios (HTTP Client)

**Frontend:**

- React 19 + TypeScript
- Vite
- TailwindCSS
- React Dropzone

## Quick Start

### Prerequisites

- Node.js 18+
- PostgreSQL 12+
- npm or yarn(recommanded)

### Setup

1. **Clone the repo**

   ```bash
   git clone https://github.com/nynrathod/csv-product-uploader
   cd csv-product-uploader
   ```

2. **Backend Setup**

   ```bash
   cd server
   npm install
   cp .env.example .env
   # Update .env with your database credentials
   npm run start:dev
   ```

3. **Frontend Setup**

   ```bash
   cd client
   yarn install
   npm run dev
   ```

4. **Access App**
   - Frontend: http://localhost:5173
   - Backend API: http://localhost:3000

## Project Structure

```
csv-product-uploader/
├── server/
│   ├── src/
│   │   ├── auth/              # JWT authentication
│   │   ├── products/          # Product endpoints & service
│   │   ├── upload/            # File upload handling
│   │   ├── jobs/              # CSV processing queue
│   │   └── exchange-rates/    # Currency conversion
│   └── package.json
├── client/
│   ├── src/
│   │   ├── components/        # React components
│   │   ├── pages/             # Page views
│   │   ├── hooks/             # Custom hooks
│   │   ├── services/          # API calls
│   │   └── types/             # TypeScript types
│   └── package.json
└── README.md
```

## Key Features Explained

### Upload Processing

- Files are validated and queued for background processing
- Real-time progress updates via Server-Sent Events (SSE)
- Automatic exchange rate snapshot at upload time
- Batch processing (500 rows per batch) for performance

### Product Retrieval

- Products returned with exchange rates from upload time
- Support for filtering by name, price range, expiration date
- Sorting by name, price, or expiration date
- Pagination (configurable limit: 10-100 items)

### Exchange Rates

- 5 currencies supported: USD, EUR, GBP, JPY, AUD
- Rates captured at upload time (not real-time)
- Cached for 24 hours to reduce API calls
- Fallback rates if API unavailable
