# Market Pulse

Real estate market timing dashboard (CA, NV, RI, AZ, WA, UT, TN, TX) + MBA mortgage-application tracking + high-margin stock screener.

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
- **Mortgage Applications (buyer demand, weekly)**: Mortgage Bankers Association (MBA) Weekly Application Survey — set `MBA_PURCHASE_SERIES` env var to the series ID for your MBA feed (e.g. the series key from an MBA, Haver, or alternate data provider) to surface the Purchase Application Index card and chart
- **Finance**: Yahoo Finance via yfinance — S&P 500 financials, margins, moving averages
