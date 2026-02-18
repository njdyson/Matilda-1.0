# Matilda

Matilda is a virtual trading wallet web app with play-money positions and live quote support via Alpaca.

## Features

- Virtual cash wallet (starts at `$100,000`)
- Buy/sell paper trades with validation
- Portfolio, equity, and realized/unrealized P/L
- Recent trade activity log
- Local persistence using browser `localStorage`
- Optional backend runtime state persisted to `data/autobot-runtime.json` for background Autobot continuity
- Live market quotes from Alpaca via secure backend route (`/api/quotes`)
- Symbol search + add-to-watchlist flow backed by Alpaca assets (`/api/symbol-search`)
- Clickable symbols with a basic candlestick chart (toggle `Mins` / `Days`)
- AI research panel with OpenAI/DeepSeek sentiment+brief refresh
- Horizon setting in header, model/provider settings in AI accordion
- Finnhub company profile section separated above the AI brief
- Local cache for AI research + company profile to reduce repeated API calls
- Autobot panel with toggle, latest "thinking feed", and next recommendation
- Auto Trade runs on cadence when enabled; "Think Now" triggers an immediate backend cycle
- Scheduled Autobot trade execution runs on backend runtime (single executor) for foreground/background consistency
- Backend Autobot runtime sync + heartbeat: server can continue cycles when the UI window is closed
- Automatic simulation fallback if live API is unavailable

## Alpaca setup

1. Create or use your Alpaca account and API keys.
2. Copy `.env.example` to `.env`.
3. Set:
- `ALPACA_KEY_ID`
- `ALPACA_SECRET_KEY`
- `FINNHUB_API_KEY` (for company profile metadata in AI panel)
- `OPENAI_API_KEY` (for OpenAI provider and web-search-backed research)
- `DEEPSEEK_API_KEY` (for DeepSeek provider)
- `OPENAI_USAGE_API_KEY` (optional org-admin key for OpenAI daily cost endpoint)
- Optional: `ALPACA_FEED=iex` (default)
- Optional: `ALPACA_TRADING_URL=https://api.alpaca.markets` (default; used for symbol search)
- Optional: `FINNHUB_BASE_URL=https://finnhub.io/api/v1` (default)
- Optional: `OPENAI_BASE_URL=https://api.openai.com/v1` (default)
- Optional: `OPENAI_MODEL=gpt-4.1-mini` (default)
- Optional: `DEEPSEEK_BASE_URL=https://api.deepseek.com/v1` (default)
- Optional: `DEEPSEEK_MODEL=deepseek-chat` (default)

## Run locally

```powershell
node server.js
```

Then open `http://localhost:3000`.

## API routes

- `GET /api/health` - server status and whether Alpaca credentials are configured
- `GET /api/quotes?symbols=AAPL,MSFT` - normalized quote payload used by frontend
- `GET /api/candles?symbol=AAPL&timeframe=1Min&limit=50` - normalized OHLC candles for charting
- `GET /api/symbol-search?query=apple&limit=8` - Alpaca-backed symbol lookup (tradable US equities)
- `GET /api/company?symbol=AAPL` - Finnhub company profile metadata (name, industry, country, currency, IPO, market cap)
- `GET /api/account/snapshot` - Alpaca paper account + positions snapshot (used for wallet sync)
- `POST /api/trades` - Submit Alpaca paper market order (`symbol`, `side`, `qty`) and return updated snapshot
- `POST /api/ai/research` - provider-powered sentiment + confidence + action + brief (`openai` or `deepseek`)
- `POST /api/ai/autobot` - provider-powered next-trade recommendation from whole-portfolio + snapshot context
- `GET /api/openai/costs/daily` - OpenAI daily spend total for current UTC day
- `GET /api/autobot/runtime/state` - current persisted backend runtime snapshot
- `POST /api/autobot/runtime/sync` - sync frontend wallet/settings/AI cache heartbeat to backend runtime
- `POST /api/autobot/runtime/run` - manually trigger one backend runtime cycle
- `GET /api/health` now includes `openAiConfigured` and `deepseekConfigured` key sanity checks

## Notes

- Keep keys in `.env` only. Do not put API secrets in frontend code.
- Alpaca free data is typically IEX feed, not full SIP consolidated tape.
- AI and company responses are cached in browser storage to avoid unnecessary calls on each page load.
- For true background behavior, keep `server.js` running continuously (local service/PM2/cloud host).
- In broker mode, manual/app trades are submitted to Alpaca paper account and wallet positions are synced from Alpaca.
