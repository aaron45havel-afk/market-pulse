# Market Pulse

Real estate market timing dashboard (CA, NV, RI, AZ, WA, UT, TN, TX, IN, CO) + national buyer-demand leading indicator (FRED) + high-margin stock screener.

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
- **Buyer-demand leading indicator**: configurable via `MBA_PURCHASE_SERIES` env var (any FRED series ID). Recommended default: `HSN1F` (New One-Family Houses Sold, seasonally-adjusted annual rate, thousands, monthly from U.S. Census Bureau). The ideal source — MBA's Weekly Mortgage Application Purchase Index — isn't available on FRED's free tier; swap to that series ID if you have a feed via MBA, Haver, or Bloomberg
- **Finance**: Yahoo Finance via yfinance — S&P 500 financials, margins, moving averages
