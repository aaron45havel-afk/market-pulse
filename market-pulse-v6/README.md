# Market Pulse

Real estate market timing dashboard (CA, NV, RI, AZ) + high-margin stock screener.

## Setup

1. Get a free FRED API key at https://fred.stlouisfed.org/docs/api/api_key.html
2. Copy `.env.example` to `.env` and add your key
3. Install dependencies: `pip install -r requirements.txt`
4. Run: `uvicorn main:app --reload`

## Deploy to Railway

1. Push to GitHub
2. Connect repo in Railway dashboard
3. Add `FRED_API_KEY` environment variable in Railway settings
4. Deploy

## Data Sources

- **Real Estate**: Federal Reserve Economic Data (FRED) — median prices, inventory, days on market, mortgage rates
- **Finance**: Yahoo Finance via yfinance — S&P 500 financials, margins, moving averages
