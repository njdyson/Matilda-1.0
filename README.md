# Matilda

Matilda is a virtual trading wallet web app with play-money positions and live quote support via Alpaca.

## Features

- Virtual cash wallet (starts at `$100,000`)
- Buy/sell paper trades with validation
- Portfolio, equity, and realized/unrealized P/L
- Recent trade activity log
- Local persistence using browser `localStorage`
- Live market quotes from Alpaca via secure backend route (`/api/quotes`)
- Clickable symbols with a basic candlestick chart (`1Min` candles)
- Mock AI research panel with sentiment score and buy/sell sizing recommendation
- Automatic simulation fallback if live API is unavailable

## Alpaca setup

1. Create or use your Alpaca account and API keys.
2. Copy `.env.example` to `.env`.
3. Set:
- `ALPACA_KEY_ID`
- `ALPACA_SECRET_KEY`
- Optional: `ALPACA_FEED=iex` (default)

## Run locally

```powershell
node server.js
```

Then open `http://localhost:3000`.

## API routes

- `GET /api/health` - server status and whether Alpaca credentials are configured
- `GET /api/quotes?symbols=AAPL,MSFT` - normalized quote payload used by frontend
- `GET /api/candles?symbol=AAPL&timeframe=1Min&limit=50` - normalized OHLC candles for charting

## Notes

- Keep keys in `.env` only. Do not put API secrets in frontend code.
- Alpaca free data is typically IEX feed, not full SIP consolidated tape.
