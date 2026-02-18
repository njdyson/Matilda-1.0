const fs = require("fs");
const fsp = require("fs/promises");
const http = require("http");
const path = require("path");

loadDotEnv(path.join(__dirname, ".env"));

const PORT = Number(process.env.PORT) || 3000;
const ROOT_DIR = __dirname;

const ALPACA_MARKET_DATA_URL =
  process.env.ALPACA_MARKET_DATA_URL || "https://data.alpaca.markets";
const ALPACA_TRADING_URL = process.env.ALPACA_TRADING_URL || "https://api.alpaca.markets";
const ALPACA_FEED = (process.env.ALPACA_FEED || "iex").toLowerCase();
const ALPACA_KEY_ID = process.env.ALPACA_KEY_ID || process.env.APCA_API_KEY_ID || "";
const ALPACA_SECRET_KEY =
  process.env.ALPACA_SECRET_KEY || process.env.APCA_API_SECRET_KEY || "";
const FINNHUB_BASE_URL = process.env.FINNHUB_BASE_URL || "https://finnhub.io/api/v1";
const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY || "";
const OPENAI_BASE_URL = process.env.OPENAI_BASE_URL || "https://api.openai.com/v1";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_DEFAULT_MODEL = process.env.OPENAI_MODEL || "gpt-4.1-mini";

const DEFAULT_SYMBOLS = ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AMD"];
const MAX_WATCHLIST_SYMBOLS = 20;
const DEFAULT_MARKET_SCOUT_INTERVAL_MINS = 2;
const MARKET_SCOUT_MAX_RECENT_MESSAGES = 40;
const MARKET_SCOUT_HOT_REFRESH_MINS = 180;
const MARKET_SCOUT_HOT_NEWS_LOOKBACK_DAYS = 1;
const MARKET_SCOUT_MAX_HOT_SYMBOLS = 30;
const RESEARCH_NEWS_LOOKBACK_DAYS = 3;
const MARKET_SCOUT_UNIVERSE_SYMBOLS = [
  "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AMD", "AVGO", "BRK.B", "JPM", "JNJ",
  "V", "XOM", "UNH", "PG", "HD", "MA", "COST", "ABBV", "NFLX", "BAC", "KO", "PEP", "MRK", "CSCO",
  "WMT", "DIS", "CRM", "ORCL", "TMO", "ACN", "ABT", "CVX", "WFC", "MCD", "LIN", "DHR", "INTC",
  "QCOM", "TXN", "AMGN", "IBM", "CAT", "GE", "GS", "NOW", "ISRG", "SPGI", "PLD", "ADBE", "AMAT",
  "BKNG", "PGR", "RTX", "BA", "MS", "UPS", "HON", "LOW", "SYK", "SCHW", "BLK", "LMT", "GILD",
  "MDT", "AXP", "C", "PANW", "DE", "VRTX", "ADP", "MMC", "TJX", "ZTS", "MO", "SBUX", "TMUS",
  "CMCSA", "PYPL", "NKE", "F", "GM", "UBER", "SNOW", "MU", "ANET", "KLAC", "CRWD", "MELI", "INTU",
  "COP", "ETN", "APH", "CI", "SO", "DUK", "T", "PFE", "CSX", "NSC", "FDX", "MAR", "ADSK", "CDNS",
  "REGN", "EOG", "HCA", "ELV", "AON", "ITW", "USB", "EW", "MCO", "RSG", "PNC",
];
const ASSET_SEARCH_LIMIT_MAX = 25;
const ASSET_CACHE_TTL_MS = 6 * 60 * 60 * 1000;
const COMPANY_PROFILE_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const RUNTIME_STATE_FILE = path.join(ROOT_DIR, "data", "autobot-runtime.json");
const RUNTIME_TICK_MS = 15000;
const ALPACA_CLOCK_CACHE_TTL_MS = 30 * 1000;
const DEFAULT_AUTO_TRADE_ACTIONS_PER_CYCLE = 3;
const MAX_AUTO_TRADE_ACTIONS_PER_CYCLE = 5;
let cachedAssetUniverse = {
  fetchedAt: 0,
  assets: [],
};
let cachedAlpacaClock = {
  fetchedAt: 0,
  clock: null,
};
const companyProfileCache = new Map();
let runtimeState = loadRuntimeStateFromDisk();
let isRuntimeCycleRunning = false;

const MIME_TYPES = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".ico": "image/x-icon",
};

const server = http.createServer(async (req, res) => {
  const method = (req.method || "GET").toUpperCase();
  const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
  const pathname = url.pathname;

  try {
    if (method === "GET" && pathname === "/api/health") {
      return json(res, 200, {
        ok: true,
        alpacaConfigured: Boolean(ALPACA_KEY_ID && ALPACA_SECRET_KEY),
        openAiConfigured: Boolean(OPENAI_API_KEY),
        feed: ALPACA_FEED,
        date: new Date().toISOString(),
      });
    }

    if (method === "GET" && pathname === "/api/quotes") {
      if (!ALPACA_KEY_ID || !ALPACA_SECRET_KEY) {
        return json(res, 503, {
          error:
            "Alpaca keys are not configured. Set ALPACA_KEY_ID and ALPACA_SECRET_KEY in your .env file.",
        });
      }

      const requested = parseSymbols(url.searchParams.get("symbols"));
      const symbols = requested.length > 0 ? requested : DEFAULT_SYMBOLS;

      try {
        const quotes = await fetchAlpacaQuotes(symbols);
        return json(res, 200, {
          source: `alpaca-${ALPACA_FEED}`,
          asOf: new Date().toISOString(),
          quotes,
        });
      } catch (error) {
        return json(res, 502, {
          error: error instanceof Error ? error.message : "Unable to fetch quotes from Alpaca.",
        });
      }
    }

    if (method === "GET" && pathname === "/api/candles") {
      if (!ALPACA_KEY_ID || !ALPACA_SECRET_KEY) {
        return json(res, 503, {
          error:
            "Alpaca keys are not configured. Set ALPACA_KEY_ID and ALPACA_SECRET_KEY in your .env file.",
        });
      }

      const symbol = parseSingleSymbol(url.searchParams.get("symbol"));
      if (!symbol) {
        return json(res, 400, {
          error: "Query parameter `symbol` is required. Example: /api/candles?symbol=AAPL",
        });
      }

      const timeframe = parseTimeframe(url.searchParams.get("timeframe"));
      const limit = parseLimit(url.searchParams.get("limit"));

      try {
        const candles = await fetchAlpacaCandles(symbol, timeframe, limit);
        return json(res, 200, {
          source: `alpaca-${ALPACA_FEED}`,
          asOf: new Date().toISOString(),
          symbol,
          timeframe,
          candles,
        });
      } catch (error) {
        return json(res, 502, {
          error: error instanceof Error ? error.message : "Unable to fetch candles from Alpaca.",
        });
      }
    }

    if (method === "GET" && pathname === "/api/symbol-search") {
      if (!ALPACA_KEY_ID || !ALPACA_SECRET_KEY) {
        return json(res, 503, {
          error:
            "Alpaca keys are not configured. Set ALPACA_KEY_ID and ALPACA_SECRET_KEY in your .env file.",
        });
      }

      const query = parseSearchQuery(url.searchParams.get("query"));
      if (!query) {
        return json(res, 400, {
          error: "Query parameter `query` is required. Example: /api/symbol-search?query=apple",
        });
      }

      const limit = parseSearchLimit(url.searchParams.get("limit"));

      try {
        const results = await searchAlpacaAssets(query, limit);
        return json(res, 200, {
          source: "alpaca-assets",
          asOf: new Date().toISOString(),
          query,
          results,
        });
      } catch (error) {
        return json(res, 502, {
          error: error instanceof Error ? error.message : "Unable to search Alpaca assets.",
        });
      }
    }

    if (method === "GET" && pathname === "/api/company") {
      if (!FINNHUB_API_KEY) {
        return json(res, 503, {
          error: "Finnhub key is not configured. Set FINNHUB_API_KEY in your .env file.",
        });
      }

      const symbol = parseSingleSymbol(url.searchParams.get("symbol"));
      if (!symbol) {
        return json(res, 400, {
          error: "Query parameter `symbol` is required. Example: /api/company?symbol=AAPL",
        });
      }

      try {
        const cached = getCachedCompanyProfile(symbol);
        if (cached) {
          return json(res, 200, {
            source: "finnhub-profile2-cache",
            asOf: new Date().toISOString(),
            company: cached,
          });
        }

        const company = await fetchFinnhubCompanyProfile(symbol);
        if (!company) {
          return json(res, 404, {
            error: `No Finnhub company profile found for ${symbol}.`,
          });
        }

        setCachedCompanyProfile(symbol, company);
        return json(res, 200, {
          source: "finnhub-profile2",
          asOf: new Date().toISOString(),
          company,
        });
      } catch (error) {
        return json(res, 502, {
          error:
            error instanceof Error
              ? error.message
              : "Unable to load company profile from Finnhub.",
        });
      }
    }

    if (method === "GET" && pathname === "/api/account/snapshot") {
      if (!ALPACA_KEY_ID || !ALPACA_SECRET_KEY) {
        return json(res, 503, {
          error:
            "Alpaca keys are not configured. Set ALPACA_KEY_ID and ALPACA_SECRET_KEY in your .env file.",
        });
      }

      try {
        const snapshot = await fetchAlpacaAccountSnapshot();
        runtimeState = applyAlpacaSnapshotToRuntimeState(runtimeState, snapshot);
        await persistRuntimeStateToDisk(runtimeState);
        return json(res, 200, {
          source: "alpaca-paper",
          asOf: new Date().toISOString(),
          snapshot,
        });
      } catch (error) {
        return json(res, 502, {
          error:
            error instanceof Error ? error.message : "Unable to load Alpaca account snapshot.",
        });
      }
    }

    if (method === "POST" && pathname === "/api/trades") {
      if (!ALPACA_KEY_ID || !ALPACA_SECRET_KEY) {
        return json(res, 503, {
          error:
            "Alpaca keys are not configured. Set ALPACA_KEY_ID and ALPACA_SECRET_KEY in your .env file.",
        });
      }

      let requestBody;
      try {
        requestBody = await readJsonBody(req);
      } catch (error) {
        return json(res, 400, {
          error: error instanceof Error ? error.message : "Invalid JSON request body.",
        });
      }
      if (!requestBody || typeof requestBody !== "object") {
        return json(res, 400, {
          error: "Request body must be valid JSON.",
        });
      }

      const symbol = parseSingleSymbol(requestBody.symbol);
      const side = parseTradeSide(requestBody.side);
      const qty = parseTradeQuantity(requestBody.qty ?? requestBody.shares);
      if (!symbol || !side || qty <= 0) {
        return json(res, 400, {
          error: "Fields `symbol`, `side` (buy|sell), and integer `qty` are required.",
        });
      }

      try {
        const openOrders = await fetchAlpacaOpenOrders();
        const conflictingOrder = findConflictingOpenOrder(openOrders, symbol, side);
        if (conflictingOrder) {
          const details = describeOpenOrder(conflictingOrder);
          return json(res, 409, {
            error: `An open ${side.toUpperCase()} order for ${symbol} already exists (${details}). Wait for fill/cancel before submitting another.`,
          });
        }

        const order = await submitAlpacaMarketOrder({
          symbol,
          side,
          qty,
        });
        const snapshot = await fetchAlpacaAccountSnapshot();
        runtimeState = applyAlpacaSnapshotToRuntimeState(runtimeState, snapshot);
        await persistRuntimeStateToDisk(runtimeState);
        return json(res, 200, {
          source: "alpaca-paper",
          asOf: new Date().toISOString(),
          order,
          snapshot,
        });
      } catch (error) {
        return json(res, 502, {
          error:
            error instanceof Error ? error.message : "Unable to submit Alpaca paper order.",
        });
      }
    }

    if (method === "POST" && pathname === "/api/ai/research") {
      if (!OPENAI_API_KEY) {
        return json(res, 503, {
          error: "OpenAI key is not configured. Set OPENAI_API_KEY in your .env file.",
        });
      }

      let requestBody;
      try {
        requestBody = await readJsonBody(req);
      } catch (error) {
        return json(res, 400, {
          error: error instanceof Error ? error.message : "Invalid JSON request body.",
        });
      }
      if (!requestBody || typeof requestBody !== "object") {
        return json(res, 400, {
          error: "Request body must be valid JSON.",
        });
      }

      const symbol = parseSingleSymbol(requestBody.symbol);
      if (!symbol) {
        return json(res, 400, {
          error: "Request body field `symbol` is required.",
        });
      }

      const provider = String(requestBody.provider || "openai")
        .trim()
        .toLowerCase();
      if (provider !== "openai") {
        return json(res, 400, {
          error: "Only provider `openai` is currently supported.",
        });
      }

      const model = parseAiModel(requestBody.model, OPENAI_DEFAULT_MODEL);
      const horizon = parseAiHorizon(requestBody.horizon);
      const quote = normalizeAiQuoteInput(requestBody.quote);
      const company = normalizeAiCompanyInput(requestBody.company);
      const marketContext = normalizeAiMarketContextInput(requestBody.marketContext);
      const position = normalizeAiPositionInput(requestBody.position);

      try {
        const research = await fetchOpenAiResearch({
          symbol,
          model,
          horizon,
          quote,
          company,
          marketContext,
          position,
        });

        return json(res, 200, {
          source: "openai-chat-completions",
          asOf: new Date().toISOString(),
          symbol,
          provider,
          model,
          horizon,
          research,
        });
      } catch (error) {
        return json(res, 502, {
          error:
            error instanceof Error ? error.message : "Unable to generate AI research via OpenAI.",
        });
      }
    }

    if (method === "POST" && pathname === "/api/ai/autobot") {
      if (!OPENAI_API_KEY) {
        return json(res, 503, {
          error: "OpenAI key is not configured. Set OPENAI_API_KEY in your .env file.",
        });
      }

      let requestBody;
      try {
        requestBody = await readJsonBody(req);
      } catch (error) {
        return json(res, 400, {
          error: error instanceof Error ? error.message : "Invalid JSON request body.",
        });
      }

      if (!requestBody || typeof requestBody !== "object") {
        return json(res, 400, {
          error: "Request body must be valid JSON.",
        });
      }

      const provider = String(requestBody.provider || "openai")
        .trim()
        .toLowerCase();
      if (provider !== "openai") {
        return json(res, 400, {
          error: "Only provider `openai` is currently supported.",
        });
      }

      const model = parseAiModel(requestBody.model, OPENAI_DEFAULT_MODEL);
      const horizon = parseAiHorizon(requestBody.horizon);
      const maxActions = clampInteger(
        requestBody.maxTradesPerCycle,
        1,
        MAX_AUTO_TRADE_ACTIONS_PER_CYCLE,
        DEFAULT_AUTO_TRADE_ACTIONS_PER_CYCLE
      );
      const context = normalizeAutobotContextInput(requestBody.context);
      if (!Array.isArray(context.snapshot) || context.snapshot.length === 0) {
        return json(res, 400, {
          error: "Autobot context requires at least one symbol in `context.snapshot`.",
        });
      }

      try {
        const plan = await fetchOpenAiAutobotRecommendation({
          model,
          horizon,
          context,
          maxActions,
        });
        const recommendations = Array.isArray(plan?.recommendations)
          ? plan.recommendations.slice(0, maxActions)
          : [];
        const recommendation =
          recommendations[0] ||
          normalizeAutobotRecommendationResult(
            {
              action: "HOLD",
              symbol: "",
              shares: 0,
              reason: "No actionable recommendation returned.",
              thought: "",
            },
            context
          );

        return json(res, 200, {
          source: "openai-autobot",
          asOf: new Date().toISOString(),
          provider,
          model,
          horizon,
          recommendation,
          recommendations,
          thought: cleanModelText(plan?.thought, 1000, recommendation.reason),
        });
      } catch (error) {
        return json(res, 502, {
          error:
            error instanceof Error
              ? error.message
              : "Unable to generate Autobot recommendation via OpenAI.",
        });
      }
    }

    if (method === "GET" && pathname === "/api/autobot/runtime/state") {
      return json(res, 200, {
        asOf: new Date().toISOString(),
        runtime: exportRuntimeState(runtimeState),
      });
    }

    if (method === "POST" && pathname === "/api/autobot/runtime/sync") {
      let requestBody;
      try {
        requestBody = await readJsonBody(req);
      } catch (error) {
        return json(res, 400, {
          error: error instanceof Error ? error.message : "Invalid JSON request body.",
        });
      }

      if (!requestBody || typeof requestBody !== "object") {
        return json(res, 400, {
          error: "Request body must be valid JSON.",
        });
      }

      runtimeState = mergeRuntimeStateFromClient(runtimeState, requestBody);
      await persistRuntimeStateToDisk(runtimeState);
      return json(res, 200, {
        ok: true,
        asOf: new Date().toISOString(),
        runtime: exportRuntimeState(runtimeState),
      });
    }

    if (method === "POST" && pathname === "/api/autobot/runtime/run") {
      try {
        const outcome = await runRuntimeCycle("manual");
        return json(res, 200, {
          ok: true,
          asOf: new Date().toISOString(),
          outcome,
          runtime: exportRuntimeState(runtimeState),
        });
      } catch (error) {
        return json(res, 502, {
          error: error instanceof Error ? error.message : "Unable to run runtime cycle.",
        });
      }
    }

    if (method !== "GET" && method !== "HEAD") {
      return json(res, 405, { error: "Method not allowed." });
    }

    return serveStatic(pathname, res, method === "HEAD");
  } catch (error) {
    return json(res, 500, {
      error: error instanceof Error ? error.message : "Unexpected server error.",
    });
  }
});

server.listen(PORT, () => {
  console.log(`Matilda server running on http://localhost:${PORT}`);
});

setInterval(() => {
  void runRuntimeTick();
}, RUNTIME_TICK_MS);

async function serveStatic(pathname, res, headOnly) {
  const safePath = sanitizePath(pathname);
  const absolute = path.join(ROOT_DIR, safePath);

  if (await isFile(absolute)) {
    return sendFile(res, absolute, headOnly);
  }

  const indexPath = path.join(ROOT_DIR, "index.html");
  return sendFile(res, indexPath, headOnly);
}

function sanitizePath(pathname) {
  const normalized = path.posix.normalize(pathname || "/");
  const raw = normalized === "/" ? "/index.html" : normalized;
  const withoutTraversal = raw.replace(/^(\.\.(\/|\\|$))+/, "");
  return withoutTraversal.startsWith("/") ? withoutTraversal.slice(1) : withoutTraversal;
}

async function sendFile(res, absolutePath, headOnly) {
  const ext = path.extname(absolutePath).toLowerCase();
  const mime = MIME_TYPES[ext] || "application/octet-stream";

  try {
    const buffer = await fsp.readFile(absolutePath);
    res.statusCode = 200;
    res.setHeader("Content-Type", mime);
    res.setHeader("Cache-Control", "no-store");
    res.setHeader("Content-Length", buffer.length);
    if (headOnly) {
      res.end();
      return;
    }
    res.end(buffer);
  } catch {
    json(res, 404, { error: "Not found." });
  }
}

async function isFile(filePath) {
  try {
    const stat = await fsp.stat(filePath);
    return stat.isFile();
  } catch {
    return false;
  }
}

async function fetchAlpacaQuotes(symbols) {
  const joined = symbols.join(",");
  const url = new URL("/v2/stocks/snapshots", ALPACA_MARKET_DATA_URL);
  url.searchParams.set("symbols", joined);
  url.searchParams.set("feed", ALPACA_FEED);

  const response = await fetch(url, {
    method: "GET",
    headers: {
      "APCA-API-KEY-ID": ALPACA_KEY_ID,
      "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    const body = await safeReadText(response);
    throw new Error(`Alpaca request failed (${response.status}): ${body.slice(0, 180)}`);
  }

  const payload = await response.json();
  const normalized = normalizeSnapshots(payload, symbols);

  if (normalized.length === 0) {
    throw new Error("Alpaca returned no quote data for the requested symbols.");
  }

  return normalized;
}

async function fetchAlpacaCandles(symbol, timeframe, limit) {
  const url = new URL("/v2/stocks/bars", ALPACA_MARKET_DATA_URL);
  const { startIso, endIso } = buildAlpacaBarsWindow(timeframe, limit);
  url.searchParams.set("symbols", symbol);
  url.searchParams.set("timeframe", timeframe);
  url.searchParams.set("start", startIso);
  url.searchParams.set("end", endIso);
  url.searchParams.set("limit", String(limit));
  url.searchParams.set("feed", ALPACA_FEED);

  const response = await fetch(url, {
    method: "GET",
    headers: {
      "APCA-API-KEY-ID": ALPACA_KEY_ID,
      "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    const body = await safeReadText(response);
    throw new Error(`Alpaca candles failed (${response.status}): ${body.slice(0, 180)}`);
  }

  const payload = await response.json();
  const normalized = normalizeBars(payload, symbol);

  if (normalized.length === 0) {
    throw new Error(`Alpaca returned no candle data for ${symbol}.`);
  }

  return normalized.slice(-limit);
}

async function fetchAlpacaAccountSnapshot() {
  const [account, positions, openOrders] = await Promise.all([
    fetchAlpacaAccount(),
    fetchAlpacaPositions(),
    fetchAlpacaOpenOrders(),
  ]);
  return {
    account,
    positions,
    openOrders,
  };
}

async function fetchAlpacaAccount() {
  const url = new URL("/v2/account", ALPACA_TRADING_URL);
  const response = await fetch(url, {
    method: "GET",
    headers: {
      "APCA-API-KEY-ID": ALPACA_KEY_ID,
      "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    const body = await safeReadText(response);
    throw new Error(`Alpaca account failed (${response.status}): ${body.slice(0, 220)}`);
  }
  const payload = await response.json();
  return {
    accountNumber: String(payload?.account_number || "").trim(),
    status: String(payload?.status || "").trim(),
    currency: String(payload?.currency || "USD").trim() || "USD",
    cash: round2(asNumber(payload?.cash) || 0),
    buyingPower: round2(asNumber(payload?.buying_power) || 0),
    equity: round2(asNumber(payload?.equity) || 0),
    lastEquity: round2(asNumber(payload?.last_equity) || 0),
    portfolioValue: round2(asNumber(payload?.portfolio_value) || 0),
    daytradeCount: Math.max(0, Math.floor(asNumber(payload?.daytrade_count) || 0)),
  };
}

async function fetchAlpacaPositions() {
  const url = new URL("/v2/positions", ALPACA_TRADING_URL);
  const response = await fetch(url, {
    method: "GET",
    headers: {
      "APCA-API-KEY-ID": ALPACA_KEY_ID,
      "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    const body = await safeReadText(response);
    throw new Error(`Alpaca positions failed (${response.status}): ${body.slice(0, 220)}`);
  }
  const payload = await response.json();
  if (!Array.isArray(payload)) return [];
  return payload
    .map((row) => {
      const symbol = parseSingleSymbol(row?.symbol);
      if (!symbol) return null;
      const side = String(row?.side || "long")
        .trim()
        .toLowerCase();
      if (side === "short") return null;
      const qtyRaw = asNumber(row?.qty);
      const qty = Number.isFinite(qtyRaw) ? Math.floor(Math.max(0, qtyRaw)) : 0;
      if (qty <= 0) return null;
      return {
        symbol,
        qty,
        side: side || "long",
        avgEntryPrice: round2(asNumber(row?.avg_entry_price) || 0),
        currentPrice: round2(asNumber(row?.current_price) || 0),
        marketValue: round2(asNumber(row?.market_value) || 0),
        costBasis: round2(asNumber(row?.cost_basis) || 0),
        unrealizedPnl: round2(asNumber(row?.unrealized_pl) || 0),
        unrealizedPct: round2((asNumber(row?.unrealized_plpc) || 0) * 100),
      };
    })
    .filter(Boolean)
    .slice(0, 200);
}

async function fetchAlpacaOpenOrders() {
  const url = new URL("/v2/orders", ALPACA_TRADING_URL);
  url.searchParams.set("status", "open");
  url.searchParams.set("direction", "desc");
  url.searchParams.set("limit", "200");
  url.searchParams.set("nested", "false");

  const response = await fetch(url, {
    method: "GET",
    headers: {
      "APCA-API-KEY-ID": ALPACA_KEY_ID,
      "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    const body = await safeReadText(response);
    throw new Error(`Alpaca open orders failed (${response.status}): ${body.slice(0, 220)}`);
  }
  const payload = await response.json();
  if (!Array.isArray(payload)) return [];
  return payload.map((row) => normalizeAlpacaOrder(row)).filter(Boolean).slice(0, 200);
}

async function fetchAlpacaClock() {
  if (
    cachedAlpacaClock.clock &&
    Date.now() - cachedAlpacaClock.fetchedAt < ALPACA_CLOCK_CACHE_TTL_MS
  ) {
    return cachedAlpacaClock.clock;
  }

  const url = new URL("/v2/clock", ALPACA_TRADING_URL);
  const response = await fetch(url, {
    method: "GET",
    headers: {
      "APCA-API-KEY-ID": ALPACA_KEY_ID,
      "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    const body = await safeReadText(response);
    throw new Error(`Alpaca clock failed (${response.status}): ${body.slice(0, 220)}`);
  }
  const payload = await response.json();
  const clock = normalizeAlpacaClock(payload);
  cachedAlpacaClock = {
    fetchedAt: Date.now(),
    clock,
  };
  return clock;
}

function parseTradeSide(raw) {
  const value = String(raw || "")
    .trim()
    .toLowerCase();
  if (value === "buy" || value === "sell") return value;
  return "";
}

function parseTradeQuantity(raw) {
  const value = Number(raw);
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.floor(value));
}

async function submitAlpacaMarketOrder({ symbol, side, qty }) {
  const url = new URL("/v2/orders", ALPACA_TRADING_URL);
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "APCA-API-KEY-ID": ALPACA_KEY_ID,
      "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      symbol,
      side,
      type: "market",
      time_in_force: "day",
      qty: String(qty),
    }),
  });
  if (!response.ok) {
    const body = await safeReadText(response);
    throw new Error(`Alpaca order failed (${response.status}): ${body.slice(0, 240)}`);
  }
  const initial = await response.json();
  const id = String(initial?.id || "").trim();
  if (!id) {
    throw new Error("Alpaca order response missing order id.");
  }
  const finalized = await waitForAlpacaOrderTerminal(id, 12000, 1000);
  return normalizeAlpacaOrder(finalized || initial);
}

async function waitForAlpacaOrderTerminal(orderId, timeoutMs, pollMs) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const order = await fetchAlpacaOrderById(orderId);
    const status = String(order?.status || "")
      .trim()
      .toLowerCase();
    if (
      status === "filled" ||
      status === "canceled" ||
      status === "expired" ||
      status === "rejected" ||
      status === "suspended" ||
      status === "done_for_day"
    ) {
      return order;
    }
    await sleep(pollMs);
  }
  return fetchAlpacaOrderById(orderId);
}

async function fetchAlpacaOrderById(orderId) {
  const url = new URL(`/v2/orders/${encodeURIComponent(orderId)}`, ALPACA_TRADING_URL);
  const response = await fetch(url, {
    method: "GET",
    headers: {
      "APCA-API-KEY-ID": ALPACA_KEY_ID,
      "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    const body = await safeReadText(response);
    throw new Error(`Alpaca order lookup failed (${response.status}): ${body.slice(0, 220)}`);
  }
  return response.json();
}

function normalizeAlpacaOrder(order) {
  const side = parseTradeSide(order?.side) || "buy";
  const symbol = parseSingleSymbol(order?.symbol);
  const status = String(order?.status || "").trim().toLowerCase();
  const qtyRaw = asNumber(order?.qty);
  const filledQtyRaw = asNumber(order?.filled_qty);
  return {
    id: String(order?.id || "").trim(),
    clientOrderId: String(order?.client_order_id || "").trim(),
    symbol,
    side,
    status,
    qty: Number.isFinite(qtyRaw) && qtyRaw > 0 ? Math.floor(qtyRaw) : 0,
    filledQty: Number.isFinite(filledQtyRaw) && filledQtyRaw > 0 ? Math.floor(filledQtyRaw) : 0,
    filledAvgPrice: round2(asNumber(order?.filled_avg_price) || 0),
    submittedAt: String(order?.submitted_at || "").trim(),
    filledAt: String(order?.filled_at || "").trim(),
  };
}

function normalizeAlpacaClock(clock) {
  const safe = clock && typeof clock === "object" ? clock : {};
  return {
    isOpen: Boolean(safe.is_open),
    timestamp: String(safe.timestamp || "").trim(),
    nextOpen: String(safe.next_open || "").trim(),
    nextClose: String(safe.next_close || "").trim(),
  };
}

function isAlpacaOrderAcceptedStatus(status) {
  const value = String(status || "")
    .trim()
    .toLowerCase();
  return (
    value === "new" ||
    value === "accepted" ||
    value === "pending_new" ||
    value === "accepted_for_bidding" ||
    value === "partially_filled" ||
    value === "filled" ||
    value === "calculated"
  );
}

function isAlpacaOpenOrderStatus(status) {
  const value = String(status || "")
    .trim()
    .toLowerCase();
  return (
    value === "new" ||
    value === "accepted" ||
    value === "pending_new" ||
    value === "accepted_for_bidding" ||
    value === "partially_filled" ||
    value === "pending_replace" ||
    value === "pending_cancel" ||
    value === "stopped"
  );
}

function hasConflictingOpenOrder(openOrders, symbol, side) {
  return Boolean(findConflictingOpenOrder(openOrders, symbol, side));
}

function findConflictingOpenOrder(openOrders, symbol, side) {
  const cleanSymbol = parseSingleSymbol(symbol);
  const cleanSide = parseTradeSide(side);
  if (!cleanSymbol || !cleanSide || !Array.isArray(openOrders)) return null;
  for (const row of openOrders) {
    const rowSymbol = parseSingleSymbol(row?.symbol);
    const rowSide = parseTradeSide(row?.side);
    const status = String(row?.status || "").trim().toLowerCase();
    if (!(rowSymbol === cleanSymbol && rowSide === cleanSide && isAlpacaOpenOrderStatus(status))) {
      continue;
    }
    const qtyRaw = asNumber(row?.qty);
    const filledQtyRaw = asNumber(row?.filledQty ?? row?.filled_qty);
    const qty = Number.isFinite(qtyRaw) && qtyRaw > 0 ? Math.floor(qtyRaw) : 0;
    const filledQty = Number.isFinite(filledQtyRaw) && filledQtyRaw > 0 ? Math.floor(filledQtyRaw) : 0;
    return {
      id: String(row?.id || "").trim(),
      symbol: rowSymbol,
      side: rowSide,
      status,
      qty,
      filledQty,
      submittedAt: String(row?.submittedAt || row?.submitted_at || "").trim(),
    };
  }
  return null;
}

function describeOpenOrder(order) {
  const qtyRaw = asNumber(order?.qty);
  const filledQtyRaw = asNumber(order?.filledQty);
  const qty = Number.isFinite(qtyRaw) && qtyRaw > 0 ? Math.floor(qtyRaw) : 0;
  const filledQty = Number.isFinite(filledQtyRaw) && filledQtyRaw > 0 ? Math.floor(filledQtyRaw) : 0;
  const remaining = qty > 0 ? Math.max(0, qty - filledQty) : 0;
  const status = String(order?.status || "").trim().toLowerCase();
  const bits = [];
  if (qty > 0) {
    if (filledQty > 0 && remaining > 0) {
      bits.push(`${remaining}/${qty} shares remaining`);
    } else {
      bits.push(`${qty} shares`);
    }
  }
  if (status) {
    bits.push(`status ${status}`);
  }
  return bits.join(", ") || "order still open";
}

function buildClosedMarketAutobotMessage(clock) {
  const nextOpen = String(clock?.nextOpen || "").trim();
  if (nextOpen) {
    return `US markets closed, reopen at ${formatUsMarketTime(nextOpen)} (${formatGmtTime(nextOpen)}).`;
  }
  return "US markets closed, reopen at next session.";
}

function formatUsMarketTime(value) {
  const ms = Date.parse(String(value || "").trim());
  if (!Number.isFinite(ms)) return String(value || "").trim() || "next session";
  try {
    return new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      month: "short",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
      timeZoneName: "short",
    }).format(new Date(ms));
  } catch {
    return new Date(ms).toISOString();
  }
}

function formatGmtTime(value) {
  const ms = Date.parse(String(value || "").trim());
  if (!Number.isFinite(ms)) return "GMT unavailable";
  try {
    return new Intl.DateTimeFormat("en-GB", {
      timeZone: "Etc/GMT",
      month: "short",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
      timeZoneName: "short",
    }).format(new Date(ms));
  } catch {
    return "GMT unavailable";
  }
}

function buildAlpacaBarsWindow(timeframe, limit) {
  const endMs = Date.now();
  const timeframeMs = timeframeToMs(timeframe);
  const baseSpanMs = timeframeMs * Math.max(1, limit);
  const minLookbackMs = 7 * 24 * 60 * 60 * 1000;
  const maxLookbackMs = 5 * 365 * 24 * 60 * 60 * 1000;
  const lookbackMs = Math.min(maxLookbackMs, Math.max(minLookbackMs, baseSpanMs * 4));
  const startMs = endMs - lookbackMs;

  return {
    startIso: new Date(startMs).toISOString(),
    endIso: new Date(endMs).toISOString(),
  };
}

function timeframeToMs(timeframe) {
  switch (String(timeframe || "").trim()) {
    case "1Min":
      return 60 * 1000;
    case "5Min":
      return 5 * 60 * 1000;
    case "15Min":
      return 15 * 60 * 1000;
    case "1Hour":
      return 60 * 60 * 1000;
    case "1Day":
      return 24 * 60 * 60 * 1000;
    case "1Month":
      return 30 * 24 * 60 * 60 * 1000;
    default:
      return 60 * 1000;
  }
}

function normalizeSnapshots(payload, symbols) {
  const snapshotMap = getSnapshotMap(payload);
  const rows = [];

  for (const symbol of symbols) {
    const snapshot = snapshotMap[symbol];
    if (!snapshot || typeof snapshot !== "object") continue;

    const latestTrade = snapshot.latestTrade || snapshot.latest_trade;
    const minuteBar = snapshot.minuteBar || snapshot.minute_bar;
    const dailyBar = snapshot.dailyBar || snapshot.daily_bar;
    const prevDailyBar =
      snapshot.prevDailyBar || snapshot.previousDailyBar || snapshot.prev_daily_bar;

    const price = asNumber(latestTrade?.p) ?? asNumber(minuteBar?.c) ?? asNumber(dailyBar?.c);
    const lastClose = asNumber(prevDailyBar?.c) ?? asNumber(dailyBar?.o) ?? price;

    if (!price || !lastClose) continue;

    rows.push({
      symbol,
      price: round2(price),
      lastClose: round2(lastClose),
    });
  }

  return rows;
}

function getSnapshotMap(payload) {
  if (!payload || typeof payload !== "object") return {};

  if (payload.snapshots && typeof payload.snapshots === "object") {
    return payload.snapshots;
  }

  return payload;
}

function normalizeBars(payload, symbol) {
  if (!payload || typeof payload !== "object") return [];
  const barSet =
    payload?.bars && typeof payload.bars === "object" && Array.isArray(payload.bars[symbol])
      ? payload.bars[symbol]
      : [];

  return barSet
    .map((bar) => {
      const o = asNumber(bar?.o);
      const h = asNumber(bar?.h);
      const l = asNumber(bar?.l);
      const c = asNumber(bar?.c);
      const t = String(bar?.t || "");
      const v = asNumber(bar?.v) || 0;
      if (!o || !h || !l || !c || !t) return null;
      return {
        t,
        o: round2(o),
        h: round2(h),
        l: round2(l),
        c: round2(c),
        v: Math.round(v),
      };
    })
    .filter(Boolean)
    .sort((a, b) => Date.parse(a.t) - Date.parse(b.t));
}

function parseSymbols(raw) {
  if (!raw) return [];

  return String(raw)
    .split(",")
    .map((part) => part.trim().toUpperCase())
    .filter(Boolean)
    .filter((symbol, index, list) => list.indexOf(symbol) === index)
    .slice(0, 50);
}

function parseSingleSymbol(raw) {
  if (!raw) return "";

  const symbol = String(raw).trim().toUpperCase();
  if (!symbol) return "";
  if (!/^[A-Z0-9.-]{1,12}$/.test(symbol)) return "";
  return symbol;
}

function parseTimeframe(raw) {
  const value = String(raw || "1Min").trim();
  const allowed = new Set(["1Min", "5Min", "15Min", "1Hour", "1Day", "1Month"]);
  return allowed.has(value) ? value : "1Min";
}

function parseLimit(raw) {
  const value = Number(raw);
  if (!Number.isFinite(value)) return 50;
  return Math.max(10, Math.min(500, Math.floor(value)));
}

function parseSearchQuery(raw) {
  if (!raw) return "";
  return String(raw).trim().slice(0, 64);
}

function parseSearchLimit(raw) {
  const value = Number(raw);
  if (!Number.isFinite(value)) return 12;
  return Math.max(1, Math.min(ASSET_SEARCH_LIMIT_MAX, Math.floor(value)));
}

async function searchAlpacaAssets(query, limit) {
  const upper = query.toUpperCase();
  const assets = await getCachedAssetUniverse();
  const matches = [];

  for (const asset of assets) {
    if (!asset.tradable) continue;
    const symbol = asset.symbol;
    const name = asset.name;
    const nameUpper = name.toUpperCase();

    let score = Number.POSITIVE_INFINITY;
    if (symbol === upper) score = 0;
    else if (symbol.startsWith(upper)) score = 1;
    else if (symbol.includes(upper)) score = 2;
    else if (nameUpper.startsWith(upper)) score = 3;
    else if (nameUpper.includes(upper)) score = 4;
    if (!Number.isFinite(score)) continue;

    matches.push({
      score,
      symbol,
      name,
      exchange: asset.exchange,
      tradable: asset.tradable,
    });
  }

  return matches
    .sort((a, b) => {
      if (a.score !== b.score) return a.score - b.score;
      return a.symbol.localeCompare(b.symbol);
    })
    .slice(0, limit)
    .map(({ symbol, name, exchange, tradable }) => ({
      symbol,
      name,
      exchange,
      tradable,
    }));
}

async function fetchFinnhubCompanyProfile(symbol) {
  const normalized = parseSingleSymbol(symbol);
  if (!normalized) return null;

  const url = buildFinnhubUrl("stock/profile2");
  url.searchParams.set("symbol", normalized);
  url.searchParams.set("token", FINNHUB_API_KEY);

  const response = await fetch(url, {
    method: "GET",
    headers: {
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    const body = await safeReadText(response);
    throw new Error(`Finnhub company profile failed (${response.status}): ${body.slice(0, 180)}`);
  }

  const payload = await response.json();
  if (!payload || typeof payload !== "object" || Object.keys(payload).length === 0) {
    return null;
  }

  const name = String(payload?.name || "").trim();
  const exchange = String(payload?.exchange || "").trim();
  const ticker = parseSingleSymbol(payload?.ticker) || normalized;
  if (!name && !exchange) return null;

  return {
    symbol: ticker,
    name,
    exchange,
    industry: String(payload?.finnhubIndustry || "").trim(),
    country: String(payload?.country || "").trim(),
    currency: String(payload?.currency || "").trim(),
    ipo: String(payload?.ipo || "").trim(),
    logo: String(payload?.logo || "").trim(),
    phone: String(payload?.phone || "").trim(),
    weburl: String(payload?.weburl || "").trim(),
    marketCapitalization: asNumber(payload?.marketCapitalization),
    shareOutstanding: asNumber(payload?.shareOutstanding),
  };
}

function getCachedCompanyProfile(symbol) {
  const entry = companyProfileCache.get(symbol);
  if (!entry) return null;
  if (Date.now() - entry.fetchedAt > COMPANY_PROFILE_CACHE_TTL_MS) {
    companyProfileCache.delete(symbol);
    return null;
  }
  return entry.company;
}

function setCachedCompanyProfile(symbol, company) {
  companyProfileCache.set(symbol, {
    fetchedAt: Date.now(),
    company,
  });
}

function loadRuntimeStateFromDisk() {
  try {
    if (!fs.existsSync(RUNTIME_STATE_FILE)) return createInitialRuntimeState();
    const raw = fs.readFileSync(RUNTIME_STATE_FILE, "utf8");
    const parsed = JSON.parse(raw);
    return normalizeRuntimeState(parsed);
  } catch {
    return createInitialRuntimeState();
  }
}

async function persistRuntimeStateToDisk(state) {
  const normalized = normalizeRuntimeState(state);
  const dir = path.dirname(RUNTIME_STATE_FILE);
  await fsp.mkdir(dir, { recursive: true });
  await fsp.writeFile(RUNTIME_STATE_FILE, `${JSON.stringify(normalized, null, 2)}\n`, "utf8");
}

function createInitialRuntimeState() {
  const now = Date.now();
  const watchlist = [...DEFAULT_SYMBOLS].slice(0, MAX_WATCHLIST_SYMBOLS);
  const prices = {};
  for (const symbol of watchlist) {
    prices[symbol] = {
      price: 100,
      lastClose: 100,
    };
  }

  return {
    updatedAt: now,
    lastClientHeartbeatAt: 0,
    aiSettings: {
      provider: "openai",
      model: OPENAI_DEFAULT_MODEL,
      horizon: "swing",
      autobotIntervalMins: 30,
      maxTradesPerCycle: DEFAULT_AUTO_TRADE_ACTIONS_PER_CYCLE,
      aiResearchAutoRefreshEnabled: true,
      aiResearchAutoRefreshMins: 60,
      addConfidenceMin: 80,
      addSentimentMin: 45,
      sellConfidenceMin: 60,
      sellSentimentMax: -5,
    },
    wallet: {
      cash: 100000,
      realizedPnl: 0,
      selectedSymbol: watchlist[0],
      watchlist,
      lockedSymbols: [],
      prices,
      positions: {},
      transactions: [],
    },
    aiResearchCache: {},
    aiRefreshMeta: buildDefaultRuntimeAiRefreshMeta(watchlist, now),
    marketScout: createDefaultRuntimeMarketScoutState(now),
    autobot: {
      enabled: false,
      lastRunAt: 0,
      latestThought: "",
      latestRecommendation: null,
      latestAutoAction: null,
      lastStatus: "Disabled",
      lastError: "",
    },
  };
}

function createDefaultRuntimeMarketScoutState(now) {
  return {
    enabled: true,
    intervalMins: DEFAULT_MARKET_SCOUT_INTERVAL_MINS,
    lastRunAt: 0,
    lastHotRefreshAt: 0,
    cursor: 0,
    lastSymbol: "",
    latestMessage: "Research ticker idle.",
    hotSummary: "",
    hotSymbols: [],
    recentMessages: [],
    checkedAtBySymbol: {},
    createdAt: normalizeTimestamp(now, Date.now()),
  };
}

function applyAlpacaSnapshotToRuntimeState(state, snapshot) {
  const normalized = normalizeRuntimeState(state);
  const account = snapshot && typeof snapshot === "object" ? snapshot.account : null;
  const positions = Array.isArray(snapshot?.positions) ? snapshot.positions : [];
  const currentWatchlist = Array.isArray(normalized.wallet.watchlist)
    ? normalized.wallet.watchlist.map((symbol) => parseSingleSymbol(symbol)).filter(Boolean)
    : [];
  const positionSymbols = positions.map((row) => parseSingleSymbol(row?.symbol)).filter(Boolean);
  const watchlist = Array.from(new Set([...currentWatchlist, ...positionSymbols])).slice(
    0,
    MAX_WATCHLIST_SYMBOLS
  );

  const prices = { ...(normalized.wallet.prices || {}) };
  const nextPositions = {};
  for (const symbol of watchlist) {
    const existing = prices[symbol];
    if (!existing || typeof existing !== "object") {
      prices[symbol] = {
        price: 100,
        lastClose: 100,
      };
    }
  }
  for (const row of positions) {
    const symbol = parseSingleSymbol(row?.symbol);
    const sharesRaw = asNumber(row?.qty);
    const shares = Number.isFinite(sharesRaw) && sharesRaw > 0 ? Math.floor(sharesRaw) : 0;
    if (!symbol || shares <= 0) continue;
    const avgEntryPrice = asNumber(row?.avgEntryPrice);
    const currentPrice = asNumber(row?.currentPrice);
    nextPositions[symbol] = {
      shares,
      avgCost: round2(avgEntryPrice && avgEntryPrice > 0 ? avgEntryPrice : currentPrice || 0),
    };
    const price = currentPrice && currentPrice > 0 ? round2(currentPrice) : prices[symbol]?.price || 100;
    prices[symbol] = {
      price,
      lastClose: prices[symbol]?.lastClose && prices[symbol].lastClose > 0 ? prices[symbol].lastClose : price,
    };
  }

  normalized.wallet.watchlist = watchlist.length > 0 ? watchlist : [...DEFAULT_SYMBOLS];
  normalized.wallet.lockedSymbols = Array.from(
    new Set(
      (Array.isArray(normalized.wallet.lockedSymbols) ? normalized.wallet.lockedSymbols : [])
        .map((symbol) => parseSingleSymbol(symbol))
        .filter((symbol) => symbol && normalized.wallet.watchlist.includes(symbol))
    )
  );
  normalized.wallet.prices = prices;
  normalized.wallet.positions = nextPositions;
  if (account && typeof account === "object") {
    const cash = asNumber(account.cash);
    if (cash !== null && cash >= 0) {
      normalized.wallet.cash = round2(cash);
    }
  }

  if (
    !normalized.wallet.selectedSymbol ||
    !normalized.wallet.watchlist.includes(normalized.wallet.selectedSymbol)
  ) {
    normalized.wallet.selectedSymbol = normalized.wallet.watchlist[0] || DEFAULT_SYMBOLS[0];
  }

  normalized.updatedAt = Date.now();
  return normalizeRuntimeState(normalized);
}

function buildDefaultRuntimeAiRefreshMeta(watchlist, now) {
  const out = {};
  const intervalMs = 60 * 60 * 1000;
  for (const symbol of Array.isArray(watchlist) ? watchlist : []) {
    const clean = parseSingleSymbol(symbol);
    if (!clean) continue;
    out[clean] = {
      addedAt: Math.max(1, now - getSymbolRefreshOffsetMs(clean, intervalMs)),
      lastUpdatedAt: 0,
      lastAttemptAt: 0,
    };
  }
  return out;
}

function getSymbolRefreshOffsetMs(symbol, intervalMs) {
  if (!symbol || !Number.isFinite(intervalMs) || intervalMs <= 1) return 0;
  let hash = 0;
  for (let i = 0; i < symbol.length; i += 1) {
    hash = (hash * 31 + symbol.charCodeAt(i)) >>> 0;
  }
  return hash % Math.floor(intervalMs);
}

function exportRuntimeState(state) {
  const normalized = normalizeRuntimeState(state);
  const intervalMs = normalized.aiSettings.autobotIntervalMins * 60 * 1000;
  const nextRunAt = normalized.autobot.enabled
    ? normalized.autobot.lastRunAt > 0
      ? normalized.autobot.lastRunAt + intervalMs
      : Date.now()
    : 0;
  return {
    updatedAt: normalized.updatedAt,
    lastClientHeartbeatAt: normalized.lastClientHeartbeatAt,
    aiSettings: normalized.aiSettings,
    wallet: normalized.wallet,
    aiResearchCache: normalized.aiResearchCache,
    aiRefreshMeta: normalized.aiRefreshMeta,
    marketScout: normalized.marketScout,
    autobot: normalized.autobot,
    nextRunAt,
    serverSideAutobotActive: Boolean(normalized.autobot.enabled),
  };
}

function mergeRuntimeStateFromClient(current, payload) {
  const safePayload = payload && typeof payload === "object" ? payload : {};
  const next = normalizeRuntimeState(current);
  const clientRuntimeUpdatedAt = normalizeTimestamp(safePayload.clientRuntimeUpdatedAt, 0);
  const canOverwriteState =
    clientRuntimeUpdatedAt <= 0 || clientRuntimeUpdatedAt >= normalizeTimestamp(next.updatedAt, 0);
  let changed = false;

  if (safePayload.aiSettings && typeof safePayload.aiSettings === "object") {
    next.aiSettings = normalizeRuntimeAiSettings(safePayload.aiSettings, next.aiSettings);
    changed = true;
  }

  if (canOverwriteState && safePayload.wallet && typeof safePayload.wallet === "object") {
    next.wallet = normalizeRuntimeWallet(safePayload.wallet, next.wallet);
    changed = true;
  }

  if (canOverwriteState && safePayload.aiResearchCache && typeof safePayload.aiResearchCache === "object") {
    next.aiResearchCache = normalizeRuntimeAiResearchCache(safePayload.aiResearchCache);
    changed = true;
  }

  if (canOverwriteState && safePayload.aiRefreshMeta && typeof safePayload.aiRefreshMeta === "object") {
    next.aiRefreshMeta = normalizeRuntimeAiRefreshMeta(safePayload.aiRefreshMeta, next.wallet.watchlist);
    changed = true;
  } else {
    next.aiRefreshMeta = normalizeRuntimeAiRefreshMeta(next.aiRefreshMeta, next.wallet.watchlist);
  }

  if (safePayload.marketScout && typeof safePayload.marketScout === "object") {
    next.marketScout = normalizeRuntimeMarketScoutState(
      {
        ...next.marketScout,
        enabled:
          typeof safePayload.marketScout.enabled === "boolean"
            ? safePayload.marketScout.enabled
            : next.marketScout.enabled,
        intervalMins:
          safePayload.marketScout.intervalMins !== undefined
            ? safePayload.marketScout.intervalMins
            : next.marketScout.intervalMins,
      },
      next.marketScout
    );
    changed = true;
  }

  if (safePayload.autobot && typeof safePayload.autobot === "object") {
    const autobot = normalizeRuntimeAutobotState({
      ...next.autobot,
      enabled: Boolean(safePayload.autobot.enabled),
    });
    next.autobot = {
      ...autobot,
      lastStatus: autobot.enabled ? autobot.lastStatus || "Enabled" : "Disabled",
    };
    changed = true;
  }

  const heartbeatRaw = Number(safePayload.clientHeartbeatAt);
  const heartbeat =
    Number.isFinite(heartbeatRaw) && heartbeatRaw > 0 ? Math.floor(heartbeatRaw) : Date.now();
  next.lastClientHeartbeatAt = heartbeat;
  if (changed) {
    next.updatedAt = Date.now();
  }
  return normalizeRuntimeState(next);
}

async function runRuntimeTick() {
  if (isRuntimeCycleRunning) return;
  try {
    await runRuntimeMarketScoutTick();
  } catch (error) {
    const state = normalizeRuntimeState(runtimeState);
    const message = error instanceof Error ? cleanModelText(error.message, 220, "Scout cycle failed.") : "Scout cycle failed.";
    state.marketScout = appendRuntimeScoutMessage(
      state.marketScout,
      `Research ticker error: ${message}`,
      Date.now()
    );
    state.updatedAt = Date.now();
    runtimeState = state;
    await persistRuntimeStateToDisk(runtimeState);
  }

  if (!runtimeState.autobot.enabled) return;
  const intervalMs = runtimeState.aiSettings.autobotIntervalMins * 60 * 1000;
  const now = Date.now();
  if (runtimeState.autobot.lastRunAt > 0 && now - runtimeState.autobot.lastRunAt < intervalMs) {
    return;
  }

  try {
    await runRuntimeCycle("background");
  } catch (error) {
    runtimeState.autobot = normalizeRuntimeAutobotState({
      ...runtimeState.autobot,
      lastStatus: "Runtime cycle failed",
      lastError: error instanceof Error ? cleanModelText(error.message, 240, "Runtime cycle failed.") : "Runtime cycle failed.",
    });
    runtimeState.updatedAt = Date.now();
    await persistRuntimeStateToDisk(runtimeState);
  }
}

async function runRuntimeCycle(source) {
  if (isRuntimeCycleRunning) {
    return {
      ran: false,
      reason: "busy",
    };
  }

  isRuntimeCycleRunning = true;
  try {
    const now = Date.now();
    const state = normalizeRuntimeState(runtimeState);
    const isManualRun = source === "manual";
    if (!state.autobot.enabled && !isManualRun) {
      runtimeState = state;
      return {
        ran: false,
        reason: "disabled",
      };
    }
    if (!Array.isArray(state.wallet.watchlist) || state.wallet.watchlist.length === 0) {
      state.autobot = normalizeRuntimeAutobotState({
        ...state.autobot,
        lastStatus: "No symbols in watchlist",
      });
      state.updatedAt = now;
      runtimeState = state;
      await persistRuntimeStateToDisk(runtimeState);
      return {
        ran: false,
        reason: "empty-watchlist",
      };
    }

    state.autobot = normalizeRuntimeAutobotState({
      ...state.autobot,
      lastStatus: source === "manual" ? "Manual cycle running..." : "Background cycle running...",
      lastError: "",
    });

    if (ALPACA_KEY_ID && ALPACA_SECRET_KEY) {
      let marketClock = null;
      try {
        marketClock = await fetchAlpacaClock();
      } catch {
        const holdReason = "US market status unavailable, skipping auto trade.";
        state.autobot = normalizeRuntimeAutobotState({
          ...state.autobot,
          lastRunAt: now,
          latestThought: holdReason,
          latestRecommendation: {
            action: "HOLD",
            side: "hold",
            symbol: "",
            shares: 0,
            reason: holdReason,
            thought: holdReason,
          },
          latestAutoAction: {
            type: "HOLD",
            symbol: "",
            shares: 0,
            reason: holdReason,
            executed: false,
            at: now,
          },
          lastStatus: "Auto hold (market status unavailable)",
          lastError: "",
        });
        state.updatedAt = Date.now();
        runtimeState = state;
        await persistRuntimeStateToDisk(runtimeState);
        return {
          ran: true,
          status: state.autobot.lastStatus,
          recommendation: state.autobot.latestRecommendation,
          latestAutoAction: state.autobot.latestAutoAction,
        };
      }

      if (marketClock && marketClock.isOpen === false) {
        const holdReason = buildClosedMarketAutobotMessage(marketClock);
        state.autobot = normalizeRuntimeAutobotState({
          ...state.autobot,
          lastRunAt: now,
          latestThought: holdReason,
          latestRecommendation: {
            action: "HOLD",
            side: "hold",
            symbol: "",
            shares: 0,
            reason: holdReason,
            thought: holdReason,
          },
          latestAutoAction: {
            type: "HOLD",
            symbol: "",
            shares: 0,
            reason: holdReason,
            executed: false,
            at: now,
          },
          lastStatus: "Auto hold (market closed)",
          lastError: "",
        });
        state.updatedAt = Date.now();
        runtimeState = state;
        await persistRuntimeStateToDisk(runtimeState);
        return {
          ran: true,
          status: state.autobot.lastStatus,
          recommendation: state.autobot.latestRecommendation,
          latestAutoAction: state.autobot.latestAutoAction,
        };
      }
    }

    if (ALPACA_KEY_ID && ALPACA_SECRET_KEY) {
      try {
        const snapshot = await fetchAlpacaAccountSnapshot();
        const synced = applyAlpacaSnapshotToRuntimeState(state, snapshot);
        state.wallet = synced.wallet;
        state.aiRefreshMeta = normalizeRuntimeAiRefreshMeta(state.aiRefreshMeta, state.wallet.watchlist);
      } catch {
        // Continue with cached runtime state if broker sync is temporarily unavailable.
      }
    }

    await refreshRuntimeQuotes(state);

    if (state.aiSettings.aiResearchAutoRefreshEnabled) {
      await refreshRuntimeResearchIfDue(state);
    }

    const context = buildRuntimeAutobotContext(state);
    const maxActions = clampInteger(
      state.aiSettings.maxTradesPerCycle,
      1,
      MAX_AUTO_TRADE_ACTIONS_PER_CYCLE,
      DEFAULT_AUTO_TRADE_ACTIONS_PER_CYCLE
    );
    const plan = await fetchOpenAiAutobotRecommendation({
      model: state.aiSettings.model,
      horizon: state.aiSettings.horizon,
      context,
      maxActions,
    });
    const recommendations = Array.isArray(plan?.recommendations)
      ? plan.recommendations.slice(0, maxActions)
      : [];
    const primaryRecommendation =
      recommendations[0] ||
      normalizeAutobotRecommendationResult(
        {
          action: "HOLD",
          symbol: "",
          shares: 0,
          reason: "No strong edge across watchlist.",
          thought: "",
        },
        context
      );

    const actionSummaryParts = [];
    let lastTradeRecommendation = null;
    let lastTradeReason = "";
    let latestAutoAction = state.autobot.latestAutoAction;
    let status = "Cycle complete";
    let attemptedTrades = 0;
    let executedTrades = 0;
    const actionableCount = recommendations.filter(
      (item) => item && item.side !== "hold" && item.shares > 0
    ).length;

    if (actionableCount <= 0) {
      latestAutoAction = {
        type: "HOLD",
        symbol: primaryRecommendation.symbol || "",
        shares: 0,
        reason: primaryRecommendation.reason || "No strong edge. Holding.",
        executed: false,
        at: now,
      };
      actionSummaryParts.push("HOLD [no trade]");
      status = "Auto hold (no trade)";
    } else {
      for (const recommendation of recommendations) {
        if (!recommendation || recommendation.side === "hold" || recommendation.shares <= 0) {
          continue;
        }
        attemptedTrades += 1;
        const tradeResult = await executeRuntimeTrade(state, recommendation);
        const executed = Boolean(tradeResult?.executed);
        if (executed) executedTrades += 1;
        const actionReason =
          !executed && tradeResult?.reason
            ? `${recommendation.reason || "Model-selected action."} (${tradeResult.reason})`
            : recommendation.reason || "Model-selected action.";
        latestAutoAction = {
          type: recommendation.side.toUpperCase(),
          symbol: recommendation.symbol,
          shares: recommendation.shares,
          reason: actionReason,
          executed,
          at: Date.now(),
        };
        actionSummaryParts.push(
          `${recommendation.side.toUpperCase()} ${recommendation.shares} ${recommendation.symbol} [${
            executed ? "executed" : "no trade"
          }]`
        );
        lastTradeRecommendation = recommendation;
        lastTradeReason = String(tradeResult?.reason || "").trim();
      }

      if (attemptedTrades <= 0) {
        latestAutoAction = {
          type: "HOLD",
          symbol: primaryRecommendation.symbol || "",
          shares: 0,
          reason: primaryRecommendation.reason || "No strong edge. Holding.",
          executed: false,
          at: now,
        };
        status = "Auto hold (no trade)";
      } else if (attemptedTrades === 1 && lastTradeRecommendation) {
        status =
          executedTrades === 1
            ? `Auto executed ${lastTradeRecommendation.side.toUpperCase()} ${lastTradeRecommendation.shares} ${lastTradeRecommendation.symbol}`
            : lastTradeReason
              ? `Auto skipped ${lastTradeRecommendation.side.toUpperCase()} ${lastTradeRecommendation.symbol}: ${lastTradeReason}`
              : `Auto suggested ${lastTradeRecommendation.side.toUpperCase()} but execution failed`;
      } else {
        status =
          executedTrades === attemptedTrades
            ? `Auto executed ${executedTrades} planned trades`
            : `Auto executed ${executedTrades}/${attemptedTrades} planned trades`;
      }
    }

    const modelThought = cleanModelText(plan?.thought, 1000, "");
    const cycleSummary = actionSummaryParts.join(" | ");
    const latestThought = cleanModelText(
      [modelThought, cycleSummary].filter(Boolean).join(modelThought && cycleSummary ? "\n" : ""),
      1000,
      primaryRecommendation.reason || "No thought available."
    );

    state.autobot = normalizeRuntimeAutobotState({
      ...state.autobot,
      lastRunAt: now,
      latestThought,
      latestRecommendation: primaryRecommendation,
      latestAutoAction,
      lastStatus: status,
      lastError: "",
    });
    state.updatedAt = Date.now();
    runtimeState = state;
    await persistRuntimeStateToDisk(runtimeState);
    return {
      ran: true,
      status,
      recommendation: primaryRecommendation,
      recommendations,
      latestAutoAction,
    };
  } finally {
    isRuntimeCycleRunning = false;
  }
}

async function runRuntimeMarketScoutTick() {
  const state = normalizeRuntimeState(runtimeState);
  const now = Date.now();
  const settings = state.aiSettings;
  const scout = normalizeRuntimeMarketScoutState(state.marketScout, state.marketScout);
  if (!scout.enabled) return false;

  const intervalMs = Math.max(1, scout.intervalMins) * 60 * 1000;
  if (scout.lastRunAt > 0 && now - scout.lastRunAt < intervalMs) {
    return false;
  }

  const refreshedScout = await refreshRuntimeScoutHotSymbolsIfDue(state, scout, now);
  const activeScout = normalizeRuntimeMarketScoutState(refreshedScout, scout);
  const pick = pickNextRuntimeScoutSymbol(
    state.wallet.watchlist,
    activeScout.cursor,
    activeScout.hotSymbols
  );
  if (!pick.symbol) {
    state.marketScout = appendRuntimeScoutMessage(
      {
        ...activeScout,
        lastRunAt: now,
        cursor: 0,
      },
      `Research ticker idle: no off-watchlist symbols left in universe.`,
      now
    );
    state.updatedAt = now;
    runtimeState = normalizeRuntimeState(state);
    await persistRuntimeStateToDisk(runtimeState);
    return true;
  }

  const symbol = pick.symbol;
  const checkedAtBySymbol = {
    ...(activeScout.checkedAtBySymbol && typeof activeScout.checkedAtBySymbol === "object"
      ? activeScout.checkedAtBySymbol
      : {}),
    [symbol]: now,
  };
  let nextScout = {
    ...activeScout,
    lastRunAt: now,
    cursor: pick.nextCursor,
    lastSymbol: symbol,
    checkedAtBySymbol,
  };

  let message = `Researched ${symbol}, no action.`;

  let quote = null;
  if (ALPACA_KEY_ID && ALPACA_SECRET_KEY) {
    try {
      const rows = await fetchAlpacaQuotes([symbol]);
      const row = Array.isArray(rows) ? rows.find((item) => parseSingleSymbol(item?.symbol) === symbol) || rows[0] : null;
      const price = asNumber(row?.price);
      const lastClose = asNumber(row?.lastClose);
      if (price && price > 0) {
        quote = {
          price: round2(price),
          lastClose: lastClose && lastClose > 0 ? round2(lastClose) : round2(price),
        };
      }
    } catch {
      quote = null;
    }
  }

  if (!quote) {
    message = `Researched ${symbol}, no live quote, no action.`;
  } else if (!OPENAI_API_KEY) {
    message = `Researched ${symbol}, AI key unavailable, no action.`;
  } else {
    try {
      let company = getCachedCompanyProfile(symbol);
      if (!company && FINNHUB_API_KEY) {
        try {
          company = await fetchFinnhubCompanyProfile(symbol);
          if (company) setCachedCompanyProfile(symbol, company);
        } catch {
          company = null;
        }
      }

      const research = await fetchOpenAiResearch({
        symbol,
        model: settings.model,
        horizon: settings.horizon,
        quote,
        company,
        marketContext: buildRuntimeScoutMarketContext(state, symbol, quote, settings),
        position: normalizeAiPositionInput({
          shares: 0,
          avgCost: null,
          marketPrice: quote.price,
          marketValue: 0,
          unrealizedPnl: 0,
          unrealizedPct: 0,
        }),
      });

      const key = buildRuntimeResearchCacheKey(symbol, settings);
      const fetchedAt = Date.now();
      state.aiResearchCache[key] = {
        symbol,
        provider: settings.provider,
        model: settings.model,
        horizon: settings.horizon,
        fetchedAt,
        profile: research,
      };
      state.aiResearchCache = normalizeRuntimeAiResearchCache(state.aiResearchCache);

      const sentiment = clampNumber(Math.round(asNumber(research?.sentiment) || 0), -100, 100, 0);
      const confidence = clampNumber(Math.round(asNumber(research?.confidence) || 0), 0, 100, 0);
      const sentimentLabel = formatSignedNumber(sentiment);
      let actionText = "No action";
      let removedForLimit = "";

      if (shouldScoutAddToWatchlist(research, settings)) {
        const addResult = addRuntimeScoutSymbolToWatchlist(state, symbol, quote, settings, {
          requireBetterSentimentWhenFull: true,
          incomingSentiment: sentiment,
        });
        if (addResult.added) {
          removedForLimit = addResult.removedSymbol || "";
          actionText = removedForLimit
            ? `Added to watchlist (removed ${removedForLimit} to keep max ${MAX_WATCHLIST_SYMBOLS})`
            : "Added to watchlist";
        } else if (addResult.reason === "watchlist-full-no-removable") {
          actionText = "No action (watchlist full and no removable symbol)";
        } else if (addResult.reason === "watchlist-full-not-better") {
          actionText = "No action (watchlist full and sentiment not better than weakest symbol)";
        }
      }

      message = `Researched ${symbol}, Sentiment ${sentimentLabel}, ${actionText}.`;
      nextScout = {
        ...nextScout,
        lastSymbol: symbol,
      };
    } catch (error) {
      const details = error instanceof Error ? cleanModelText(error.message, 140, "AI failure") : "AI failure";
      message = `Researched ${symbol}, AI error (${details}), no action.`;
    }
  }

  state.marketScout = appendRuntimeScoutMessage(nextScout, message, now);
  state.updatedAt = Date.now();
  runtimeState = normalizeRuntimeState(state);
  await persistRuntimeStateToDisk(runtimeState);
  return true;
}

async function refreshRuntimeQuotes(state) {
  if (!ALPACA_KEY_ID || !ALPACA_SECRET_KEY) return;
  const symbols = state.wallet.watchlist;
  if (!Array.isArray(symbols) || symbols.length === 0) return;
  const quotes = await fetchAlpacaQuotes(symbols);
  for (const row of quotes) {
    const symbol = parseSingleSymbol(row?.symbol);
    if (!symbol || !state.wallet.prices[symbol]) continue;
    const price = asNumber(row?.price);
    const lastClose = asNumber(row?.lastClose);
    if (!price || price <= 0) continue;
    state.wallet.prices[symbol] = {
      price: round2(price),
      lastClose: lastClose && lastClose > 0 ? round2(lastClose) : round2(price),
    };
  }
}

async function refreshRuntimeResearchIfDue(state) {
  if (!OPENAI_API_KEY) return;
  const settings = state.aiSettings;
  const symbol = getNextDueRuntimeResearchSymbol(state, settings);
  if (!symbol) return;

  const now = Date.now();
  const meta = state.aiRefreshMeta[symbol] || {
    addedAt: now,
    lastUpdatedAt: 0,
    lastAttemptAt: 0,
  };
  state.aiRefreshMeta[symbol] = {
    ...meta,
    lastAttemptAt: now,
  };

  const quote = state.wallet.prices[symbol] || null;
  let company = getCachedCompanyProfile(symbol);
  if (!company && FINNHUB_API_KEY) {
    try {
      company = await fetchFinnhubCompanyProfile(symbol);
      if (company) setCachedCompanyProfile(symbol, company);
    } catch {
      company = null;
    }
  }
  const position = buildRuntimePositionContext(state, symbol);
  const marketContext = buildRuntimeResearchMarketContext(state, symbol, settings);
  const research = await fetchOpenAiResearch({
    symbol,
    model: settings.model,
    horizon: settings.horizon,
    quote,
    company,
    marketContext,
    position,
  });

  const key = buildRuntimeResearchCacheKey(symbol, settings);
  const fetchedAt = Date.now();
  state.aiResearchCache[key] = {
    symbol,
    provider: settings.provider,
    model: settings.model,
    horizon: settings.horizon,
    fetchedAt,
    profile: research,
  };
  state.aiResearchCache = normalizeRuntimeAiResearchCache(state.aiResearchCache);
  state.aiRefreshMeta[symbol] = {
    ...state.aiRefreshMeta[symbol],
    lastUpdatedAt: fetchedAt,
    lastAttemptAt: fetchedAt,
  };
}

function getNextDueRuntimeResearchSymbol(state, settings) {
  const symbols = Array.isArray(state.wallet.watchlist) ? state.wallet.watchlist : [];
  if (symbols.length === 0) return "";
  const now = Date.now();
  const intervalMs = settings.aiResearchAutoRefreshMins * 60 * 1000;
  const retryMs = 5 * 60 * 1000;
  let chosen = "";
  let earliestDueAt = Number.POSITIVE_INFINITY;
  for (const symbol of symbols) {
    const clean = parseSingleSymbol(symbol);
    if (!clean) continue;
    const meta = state.aiRefreshMeta[clean] || {
      addedAt: now - getSymbolRefreshOffsetMs(clean, intervalMs),
      lastUpdatedAt: 0,
      lastAttemptAt: 0,
    };
    const key = buildRuntimeResearchCacheKey(clean, settings);
    const entry = state.aiResearchCache[key];
    const fetchedAt = asNumber(entry?.fetchedAt) || 0;
    const anchor = Math.max(asNumber(meta.lastUpdatedAt) || 0, fetchedAt, asNumber(meta.addedAt) || 0);
    const dueAt = anchor + intervalMs;
    if (dueAt > now) continue;
    if (meta.lastAttemptAt > 0 && now - meta.lastAttemptAt < retryMs) continue;
    if (dueAt < earliestDueAt) {
      earliestDueAt = dueAt;
      chosen = clean;
    }
  }
  return chosen;
}

function buildRuntimeAutobotContext(state) {
  const positions = [];
  for (const [symbol, pos] of Object.entries(state.wallet.positions)) {
    const shares = Math.max(0, Math.floor(asNumber(pos?.shares) || 0));
    if (shares <= 0) continue;
    const avgCost = asNumber(pos?.avgCost) || 0;
    const quote = state.wallet.prices[symbol];
    const marketPrice = quote && quote.price > 0 ? quote.price : avgCost;
    const invested = round2(shares * avgCost);
    const marketValue = round2(shares * marketPrice);
    const unrealizedPnl = round2((marketPrice - avgCost) * shares);
    const unrealizedPct = invested > 0 ? round2((unrealizedPnl / invested) * 100) : 0;
    positions.push({
      symbol,
      shares,
      avgCost: round2(avgCost),
      invested,
      marketPrice: round2(marketPrice),
      marketValue,
      unrealizedPnl,
      unrealizedPct,
    });
  }

  const snapshot = [];
  const settings = state.aiSettings;
  for (const symbol of state.wallet.watchlist) {
    const quote = state.wallet.prices[symbol] || null;
    const price = quote && quote.price > 0 ? round2(quote.price) : null;
    const lastClose = quote && quote.lastClose > 0 ? round2(quote.lastClose) : null;
    const research = getRuntimeResearchEntry(state, symbol, settings);
    const profile = research?.profile || null;
    const dayChange = price !== null && lastClose !== null ? round2(price - lastClose) : null;
    const dayPct = dayChange !== null && lastClose && lastClose > 0 ? round2((dayChange / lastClose) * 100) : null;
    snapshot.push({
      symbol,
      price,
      lastClose,
      dayChange,
      dayPct,
      hasResearch: Boolean(profile),
      research: profile
        ? {
            sentiment: profile.sentiment,
            confidence: profile.confidence,
            action: parseAiAction(profile.action),
            thesis: cleanModelText(profile.thesis, 240, ""),
            catalyst: cleanModelText(profile.catalyst, 200, ""),
            risk: cleanModelText(profile.risk, 200, ""),
            brief: cleanModelText(profile.brief, 360, ""),
          }
        : null,
    });
  }

  const cash = round2(state.wallet.cash);
  const realizedPnl = round2(state.wallet.realizedPnl);
  const portfolioValue = round2(
    Object.entries(state.wallet.positions).reduce((sum, [symbol, pos]) => {
      const shares = Math.max(0, Math.floor(asNumber(pos?.shares) || 0));
      if (shares <= 0) return sum;
      const quote = state.wallet.prices[symbol];
      const price = quote && quote.price > 0 ? quote.price : asNumber(pos?.avgCost) || 0;
      return sum + shares * price;
    }, 0)
  );
  const unrealizedPnl = round2(
    Object.entries(state.wallet.positions).reduce((sum, [symbol, pos]) => {
      const shares = Math.max(0, Math.floor(asNumber(pos?.shares) || 0));
      if (shares <= 0) return sum;
      const avgCost = asNumber(pos?.avgCost) || 0;
      const quote = state.wallet.prices[symbol];
      const price = quote && quote.price > 0 ? quote.price : avgCost;
      return sum + (price - avgCost) * shares;
    }, 0)
  );
  const totalEquity = round2(cash + portfolioValue);

  return normalizeAutobotContextInput({
    cash,
    realizedPnl,
    portfolioValue,
    unrealizedPnl,
    totalEquity,
    selectedSymbol: state.wallet.selectedSymbol,
    positions,
    snapshot,
  });
}

function buildRuntimeResearchMarketContext(state, symbol, settings) {
  const watchlistRows = [];
  for (const watchSymbol of state.wallet.watchlist.slice(0, 24)) {
    const quote = state.wallet.prices[watchSymbol];
    const pos = state.wallet.positions[watchSymbol];
    const shares = Math.max(0, Math.floor(asNumber(pos?.shares) || 0));
    const avgCost = asNumber(pos?.avgCost);
    const price = asNumber(quote?.price);
    const lastClose = asNumber(quote?.lastClose);
    const dayChange =
      price && lastClose && lastClose > 0 ? round2(price - lastClose) : null;
    const dayPct =
      dayChange !== null && lastClose && lastClose > 0 ? round2((dayChange / lastClose) * 100) : null;
    const research = getRuntimeResearchEntry(state, watchSymbol, settings);
    const profile = research?.profile || null;
    watchlistRows.push({
      symbol: watchSymbol,
      price,
      lastClose,
      dayChange,
      dayPct,
      shares,
      avgCost: shares > 0 && avgCost && avgCost > 0 ? round2(avgCost) : null,
      marketValue: shares > 0 && price && price > 0 ? round2(shares * price) : 0,
      sentiment: profile ? profile.sentiment : null,
      confidence: profile ? profile.confidence : null,
      action: profile ? profile.action : "HOLD",
    });
  }

  const quote = state.wallet.prices[symbol] || null;
  return normalizeAiMarketContextInput({
    symbol,
    horizon: settings.horizon,
    quote: quote
      ? {
          price: quote.price,
          lastClose: quote.lastClose,
        }
      : null,
    portfolio: {
      cash: state.wallet.cash,
      portfolioValue: round2(
        Object.entries(state.wallet.positions).reduce((sum, [rowSymbol, pos]) => {
          const shares = Math.max(0, Math.floor(asNumber(pos?.shares) || 0));
          if (shares <= 0) return sum;
          const rowQuote = state.wallet.prices[rowSymbol];
          const price = rowQuote && rowQuote.price > 0 ? rowQuote.price : asNumber(pos?.avgCost) || 0;
          return sum + shares * price;
        }, 0)
      ),
      unrealizedPnl: round2(
        Object.entries(state.wallet.positions).reduce((sum, [rowSymbol, pos]) => {
          const shares = Math.max(0, Math.floor(asNumber(pos?.shares) || 0));
          if (shares <= 0) return sum;
          const avg = asNumber(pos?.avgCost) || 0;
          const rowQuote = state.wallet.prices[rowSymbol];
          const price = rowQuote && rowQuote.price > 0 ? rowQuote.price : avg;
          return sum + (price - avg) * shares;
        }, 0)
      ),
      realizedPnl: state.wallet.realizedPnl,
      totalEquity: round2(
        state.wallet.cash +
          Object.entries(state.wallet.positions).reduce((sum, [rowSymbol, pos]) => {
            const shares = Math.max(0, Math.floor(asNumber(pos?.shares) || 0));
            if (shares <= 0) return sum;
            const rowQuote = state.wallet.prices[rowSymbol];
            const price =
              rowQuote && rowQuote.price > 0 ? rowQuote.price : asNumber(pos?.avgCost) || 0;
            return sum + shares * price;
          }, 0)
      ),
    },
    watchlist: watchlistRows,
    candleSummary: null,
  });
}

function buildRuntimeScoutMarketContext(state, symbol, quote, settings) {
  const baseSymbol = parseSingleSymbol(state.wallet?.watchlist?.[0]) || parseSingleSymbol(symbol);
  const base = buildRuntimeResearchMarketContext(state, baseSymbol || symbol, settings);
  return normalizeAiMarketContextInput({
    ...base,
    symbol: parseSingleSymbol(symbol) || base.symbol,
    quote: quote && quote.price > 0
      ? {
          price: round2(quote.price),
          lastClose: quote.lastClose > 0 ? round2(quote.lastClose) : round2(quote.price),
        }
      : null,
  });
}

async function refreshRuntimeScoutHotSymbolsIfDue(state, scoutState, now) {
  const scout = normalizeRuntimeMarketScoutState(scoutState, scoutState);
  const refreshMs = Math.max(1, MARKET_SCOUT_HOT_REFRESH_MINS) * 60 * 1000;
  if (scout.lastHotRefreshAt > 0 && now - scout.lastHotRefreshAt < refreshMs) {
    return scout;
  }

  const nextBase = {
    ...scout,
    lastHotRefreshAt: now,
  };

  if (!OPENAI_API_KEY) {
    return normalizeRuntimeMarketScoutState(nextBase, scout);
  }

  try {
    const hot = await fetchOpenAiHotStockSymbols({
      model: state.aiSettings?.model || OPENAI_DEFAULT_MODEL,
    });
    let hotSymbols = Array.isArray(hot.symbols) ? hot.symbols : [];

    if (ALPACA_KEY_ID && ALPACA_SECRET_KEY && hotSymbols.length > 0) {
      try {
        const assets = await getCachedAssetUniverse();
        const tradable = new Set(
          (Array.isArray(assets) ? assets : [])
            .filter((asset) => asset && asset.tradable)
            .map((asset) => parseSingleSymbol(asset.symbol))
            .filter(Boolean)
        );
        if (tradable.size > 0) {
          hotSymbols = hotSymbols.filter((symbol) => tradable.has(symbol));
        }
      } catch {
        // Keep discovered symbols even if tradable-filter lookup fails.
      }
    }

    const coreSet = new Set(getMarketScoutUniverse());
    hotSymbols = hotSymbols
      .map((symbol) => parseSingleSymbol(symbol))
      .filter(Boolean)
      .filter((symbol, index, list) => list.indexOf(symbol) === index)
      .filter((symbol) => !coreSet.has(symbol))
      .slice(0, MARKET_SCOUT_MAX_HOT_SYMBOLS);

    const hotSummary = cleanModelText(
      hot.summary,
      260,
      hotSymbols.length > 0
        ? `Hot list refreshed with ${hotSymbols.length} symbols from recent signals.`
        : "Hot list refreshed: no new symbols."
    );

    return normalizeRuntimeMarketScoutState(
      {
        ...nextBase,
        hotSymbols,
        hotSummary,
      },
      scout
    );
  } catch (error) {
    const details =
      error instanceof Error
        ? cleanModelText(error.message, 180, "hot scan failed")
        : "hot scan failed";
    return normalizeRuntimeMarketScoutState(
      {
        ...nextBase,
        hotSummary: `Hot list refresh failed: ${details}`,
      },
      scout
    );
  }
}

function getMarketScoutUniverse(hotSymbolsInput = []) {
  const out = [];
  for (const raw of MARKET_SCOUT_UNIVERSE_SYMBOLS) {
    const symbol = parseSingleSymbol(raw);
    if (!symbol) continue;
    if (out.includes(symbol)) continue;
    out.push(symbol);
  }
  for (const raw of Array.isArray(hotSymbolsInput) ? hotSymbolsInput : []) {
    const symbol = parseSingleSymbol(raw);
    if (!symbol) continue;
    if (out.includes(symbol)) continue;
    out.push(symbol);
  }
  return out;
}

function pickNextRuntimeScoutSymbol(watchlist, cursor, hotSymbolsInput = []) {
  const watchSet = new Set(
    (Array.isArray(watchlist) ? watchlist : []).map((item) => parseSingleSymbol(item)).filter(Boolean)
  );
  const candidates = getMarketScoutUniverse(hotSymbolsInput).filter((symbol) => !watchSet.has(symbol));
  if (candidates.length === 0) {
    return { symbol: "", nextCursor: 0 };
  }

  const idx = ((Math.floor(Number(cursor) || 0) % candidates.length) + candidates.length) % candidates.length;
  return {
    symbol: candidates[idx],
    nextCursor: (idx + 1) % candidates.length,
  };
}

function appendRuntimeScoutMessage(scoutState, message, at) {
  const scout = normalizeRuntimeMarketScoutState(scoutState, scoutState);
  const clean = cleanModelText(message, 260, "Research ticker idle.");
  const recent = [clean, ...(Array.isArray(scout.recentMessages) ? scout.recentMessages : [])].slice(
    0,
    MARKET_SCOUT_MAX_RECENT_MESSAGES
  );
  return normalizeRuntimeMarketScoutState(
    {
      ...scout,
      latestMessage: clean,
      recentMessages: recent,
      lastRunAt: normalizeTimestamp(at, scout.lastRunAt),
    },
    scout
  );
}

function formatSignedNumber(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "N/A";
  if (num > 0) return `+${Math.round(num)}`;
  return String(Math.round(num));
}

function shouldScoutAddToWatchlist(research, settings) {
  const sentiment = Number(research?.sentiment);
  if (!Number.isFinite(sentiment)) return false;
  return sentiment > 0;
}

function addRuntimeScoutSymbolToWatchlist(state, symbol, quote, settings, options = {}) {
  const clean = parseSingleSymbol(symbol);
  if (!clean) {
    return {
      added: false,
      reason: "invalid-symbol",
      removedSymbol: "",
    };
  }
  if (state.wallet.watchlist.includes(clean)) {
    return {
      added: false,
      reason: "already-watched",
      removedSymbol: "",
    };
  }

  let removedSymbol = "";
  if (state.wallet.watchlist.length >= MAX_WATCHLIST_SYMBOLS) {
    const requireBetterSentimentWhenFull = Boolean(options.requireBetterSentimentWhenFull);
    const incomingSentimentRaw = Number(options.incomingSentiment);
    const incomingSentiment = Number.isFinite(incomingSentimentRaw) ? incomingSentimentRaw : null;
    const removal = pickRuntimeScoutRemovalCandidate(state, settings, {
      requireLowSentiment: false,
      excludeSymbols: new Set([clean]),
    });
    if (!removal) {
      return {
        added: false,
        reason: "watchlist-full-no-removable",
        removedSymbol: "",
      };
    }
    if (requireBetterSentimentWhenFull && incomingSentiment !== null) {
      const weakestSentiment =
        Number.isFinite(Number(removal.sentiment)) ? Number(removal.sentiment) : -101;
      if (incomingSentiment <= weakestSentiment) {
        return {
          added: false,
          reason: "watchlist-full-not-better",
          removedSymbol: "",
        };
      }
    }
    if (!removeRuntimeWatchlistSymbol(state, removal.symbol)) {
      return {
        added: false,
        reason: "watchlist-full-no-removable",
        removedSymbol: "",
      };
    }
    removedSymbol = removal.symbol;
  }

  const nextWatchlist = Array.from(new Set([...state.wallet.watchlist, clean])).slice(
    0,
    MAX_WATCHLIST_SYMBOLS
  );
  state.wallet.watchlist = nextWatchlist;
  state.wallet.lockedSymbols = Array.from(
    new Set(
      (Array.isArray(state.wallet.lockedSymbols) ? state.wallet.lockedSymbols : [])
        .map((item) => parseSingleSymbol(item))
        .filter((item) => item && state.wallet.watchlist.includes(item))
    )
  );
  const price = Number(quote?.price);
  const lastClose = Number(quote?.lastClose);
  const normalizedPrice = Number.isFinite(price) && price > 0 ? round2(price) : 0;
  if (normalizedPrice > 0) {
    state.wallet.prices[clean] = {
      price: normalizedPrice,
      lastClose:
        Number.isFinite(lastClose) && lastClose > 0 ? round2(lastClose) : normalizedPrice,
    };
  }
  state.aiRefreshMeta = normalizeRuntimeAiRefreshMeta(state.aiRefreshMeta, state.wallet.watchlist);
  return {
    added: true,
    reason: "added",
    removedSymbol,
  };
}

function removeRuntimeWatchlistSymbol(state, symbol) {
  const clean = parseSingleSymbol(symbol);
  if (!clean) return false;
  const current = Array.isArray(state.wallet.watchlist) ? state.wallet.watchlist : [];
  if (!current.includes(clean)) return false;
  if (current.length <= 1) return false;
  const lockedSet = new Set(
    (Array.isArray(state.wallet.lockedSymbols) ? state.wallet.lockedSymbols : [])
      .map((item) => parseSingleSymbol(item))
      .filter(Boolean)
  );
  if (lockedSet.has(clean)) return false;
  const heldShares = Math.max(0, Math.floor(asNumber(state.wallet.positions?.[clean]?.shares) || 0));
  if (heldShares > 0) return false;

  state.wallet.watchlist = current.filter((item) => parseSingleSymbol(item) !== clean);
  state.wallet.lockedSymbols = Array.from(
    new Set(
      (Array.isArray(state.wallet.lockedSymbols) ? state.wallet.lockedSymbols : [])
        .map((item) => parseSingleSymbol(item))
        .filter((item) => item && item !== clean && state.wallet.watchlist.includes(item))
    )
  );
  delete state.wallet.prices[clean];
  delete state.wallet.positions[clean];
  if (state.wallet.selectedSymbol === clean) {
    state.wallet.selectedSymbol = state.wallet.watchlist[0] || DEFAULT_SYMBOLS[0];
  }
  state.aiRefreshMeta = normalizeRuntimeAiRefreshMeta(state.aiRefreshMeta, state.wallet.watchlist);
  return true;
}

function pickRuntimeScoutRemovalCandidate(state, settings, options = {}) {
  const requireLowSentiment = Boolean(options.requireLowSentiment);
  const sellSentimentMax = Number.isFinite(Number(settings?.sellSentimentMax))
    ? Number(settings.sellSentimentMax)
    : -5;
  const sellConfidenceMin = Number.isFinite(Number(settings?.sellConfidenceMin))
    ? Number(settings.sellConfidenceMin)
    : 60;
  const excludeSymbols = options.excludeSymbols instanceof Set ? options.excludeSymbols : new Set();
  const lockedSet = new Set(
    (Array.isArray(state.wallet.lockedSymbols) ? state.wallet.lockedSymbols : [])
      .map((item) => parseSingleSymbol(item))
      .filter(Boolean)
  );
  const watchlist = Array.isArray(state.wallet.watchlist) ? state.wallet.watchlist : [];
  let best = null;
  for (const rawSymbol of watchlist) {
    const symbol = parseSingleSymbol(rawSymbol);
    if (!symbol || excludeSymbols.has(symbol) || lockedSet.has(symbol)) continue;
    const heldShares = Math.max(0, Math.floor(asNumber(state.wallet.positions?.[symbol]?.shares) || 0));
    if (heldShares > 0) continue;
    const profile = getRuntimeResearchEntry(state, symbol, settings)?.profile || null;
    const sentiment = asNumber(profile?.sentiment);
    const confidence = asNumber(profile?.confidence);
    if (requireLowSentiment) {
      if (!Number.isFinite(sentiment) || !Number.isFinite(confidence)) continue;
      if (!(sentiment <= sellSentimentMax && confidence >= sellConfidenceMin)) {
        continue;
      }
    }
    const score = Number.isFinite(sentiment) ? sentiment : -101;
    if (!best || score < best.score) {
      best = {
        symbol,
        score,
        sentiment: Number.isFinite(sentiment) ? sentiment : null,
        confidence: Number.isFinite(confidence) ? confidence : null,
      };
    }
  }
  return best;
}

function getRuntimeResearchEntry(state, symbol, settings) {
  const key = buildRuntimeResearchCacheKey(symbol, settings);
  const entry = state.aiResearchCache[key];
  if (!entry || typeof entry !== "object") return null;
  const fetchedAt = asNumber(entry.fetchedAt);
  if (!fetchedAt || fetchedAt <= 0) return null;
  return {
    symbol: parseSingleSymbol(entry.symbol) || symbol,
    provider: String(entry.provider || "openai"),
    model: parseAiModel(entry.model, settings.model),
    horizon: parseAiHorizon(entry.horizon),
    fetchedAt: Math.floor(fetchedAt),
    profile: normalizeAiResearchResult(entry.profile || {}),
  };
}

function buildRuntimeResearchCacheKey(symbol, settings) {
  const cleanSymbol = parseSingleSymbol(symbol);
  const provider = String(settings?.provider || "openai")
    .trim()
    .toLowerCase();
  const model = parseAiModel(settings?.model, OPENAI_DEFAULT_MODEL);
  const horizon = parseAiHorizon(settings?.horizon);
  return `${cleanSymbol}|${provider}|${model}|${horizon}`;
}

function buildRuntimePositionContext(state, symbol) {
  const clean = parseSingleSymbol(symbol);
  const pos = clean ? state.wallet.positions[clean] : null;
  const quote = clean ? state.wallet.prices[clean] : null;
  const shares = Math.max(0, Math.floor(asNumber(pos?.shares) || 0));
  const avgCost = asNumber(pos?.avgCost);
  const marketPrice = asNumber(quote?.price) || avgCost || null;
  const marketValue = shares > 0 && marketPrice ? round2(shares * marketPrice) : 0;
  const unrealizedPnl = shares > 0 && avgCost && marketPrice ? round2((marketPrice - avgCost) * shares) : 0;
  const unrealizedPct =
    shares > 0 && avgCost ? round2((unrealizedPnl / (shares * avgCost)) * 100) : 0;
  return normalizeAiPositionInput({
    shares,
    avgCost,
    marketPrice,
    marketValue,
    unrealizedPnl,
    unrealizedPct,
  });
}

async function executeRuntimeTrade(state, recommendation) {
  const symbol = parseSingleSymbol(recommendation?.symbol);
  const side = String(recommendation?.side || "").trim().toLowerCase();
  const sharesRaw = asNumber(recommendation?.shares);
  const shares = Number.isFinite(sharesRaw) && sharesRaw > 0 ? Math.floor(sharesRaw) : 0;
  if (!symbol || shares <= 0 || (side !== "buy" && side !== "sell")) {
    return {
      executed: false,
      reason: "invalid recommendation payload",
    };
  }
  const quote = state.wallet.prices[symbol];
  const price = asNumber(quote?.price);
  if (!price || price <= 0) {
    return {
      executed: false,
      reason: "no live quote price",
    };
  }
  if (!ALPACA_KEY_ID || !ALPACA_SECRET_KEY) {
    return {
      executed: false,
      reason: "broker credentials unavailable",
    };
  }

  try {
    let marketClock = null;
    try {
      marketClock = await fetchAlpacaClock();
    } catch {
      marketClock = null;
    }
    if (marketClock && marketClock.isOpen === false) {
      return {
        executed: false,
        reason: marketClock.nextOpen
          ? `market closed (next open ${marketClock.nextOpen})`
          : "market closed",
      };
    }

    const openOrders = await fetchAlpacaOpenOrders();
    const conflictingOrder = findConflictingOpenOrder(openOrders, symbol, side);
    if (conflictingOrder) {
      return {
        executed: false,
        reason: `open ${side} order already exists (${describeOpenOrder(conflictingOrder)})`,
      };
    }

    const priorAvgCost = asNumber(state.wallet.positions[symbol]?.avgCost) || 0;
    const order = await submitAlpacaMarketOrder({
      symbol,
      side,
      qty: shares,
    });
    const accepted = isAlpacaOrderAcceptedStatus(order.status);
    if (accepted) {
      const filledQty = Math.max(0, Math.floor(asNumber(order.filledQty) || 0));
      const filledPrice = asNumber(order.filledAvgPrice) || price;
      if (filledQty > 0 && filledPrice > 0) {
        const tx = {
          side: side.toUpperCase(),
          symbol,
          shares: filledQty,
          price: round2(filledPrice),
          total: round2(filledQty * filledPrice),
        };
        if (side === "sell" && priorAvgCost > 0) {
          tx.realized = round2((filledPrice - priorAvgCost) * filledQty);
        }
        pushRuntimeTransaction(state, tx);
      }
      const snapshot = await fetchAlpacaAccountSnapshot();
      const synced = applyAlpacaSnapshotToRuntimeState(state, snapshot);
      state.wallet = synced.wallet;
      state.aiRefreshMeta = normalizeRuntimeAiRefreshMeta(state.aiRefreshMeta, state.wallet.watchlist);
    }
    return {
      executed: accepted,
      reason: accepted ? "order accepted" : "order not accepted",
    };
  } catch {
    return {
      executed: false,
      reason: "broker order request failed",
    };
  }
}

function pushRuntimeTransaction(state, tx) {
  state.wallet.transactions.unshift({
    ...tx,
    timestamp: new Date().toISOString(),
    id: `${Date.now()}_${Math.random().toString(16).slice(2, 8)}`,
  });
  state.wallet.transactions = state.wallet.transactions.slice(0, 100);
}

function normalizeRuntimeState(input) {
  const safe = input && typeof input === "object" ? input : {};
  const fallback = createInitialRuntimeState();
  const wallet = normalizeRuntimeWallet(safe.wallet, fallback.wallet);
  return {
    updatedAt: normalizeTimestamp(safe.updatedAt, fallback.updatedAt),
    lastClientHeartbeatAt: normalizeTimestamp(safe.lastClientHeartbeatAt, 0),
    aiSettings: normalizeRuntimeAiSettings(safe.aiSettings, fallback.aiSettings),
    wallet,
    aiResearchCache: normalizeRuntimeAiResearchCache(safe.aiResearchCache),
    aiRefreshMeta: normalizeRuntimeAiRefreshMeta(safe.aiRefreshMeta, wallet.watchlist),
    marketScout: normalizeRuntimeMarketScoutState(safe.marketScout, fallback.marketScout),
    autobot: normalizeRuntimeAutobotState(safe.autobot),
  };
}

function normalizeRuntimeAiSettings(input, fallback) {
  const safe = input && typeof input === "object" ? input : {};
  const base = fallback && typeof fallback === "object" ? fallback : createInitialRuntimeState().aiSettings;
  const provider = String(safe.provider || base.provider || "openai")
    .trim()
    .toLowerCase();
  const autoEnabled =
    typeof safe.aiResearchAutoRefreshEnabled === "boolean"
      ? safe.aiResearchAutoRefreshEnabled
      : Boolean(base.aiResearchAutoRefreshEnabled);
  return {
    provider: provider === "openai" ? "openai" : "openai",
    model: parseAiModel(safe.model, base.model || OPENAI_DEFAULT_MODEL),
    horizon: parseAiHorizon(safe.horizon || base.horizon),
    autobotIntervalMins: clampInteger(safe.autobotIntervalMins, 5, 240, base.autobotIntervalMins || 30),
    maxTradesPerCycle: clampInteger(
      safe.maxTradesPerCycle,
      1,
      MAX_AUTO_TRADE_ACTIONS_PER_CYCLE,
      base.maxTradesPerCycle || DEFAULT_AUTO_TRADE_ACTIONS_PER_CYCLE
    ),
    aiResearchAutoRefreshEnabled: autoEnabled,
    aiResearchAutoRefreshMins: clampInteger(
      safe.aiResearchAutoRefreshMins,
      15,
      720,
      base.aiResearchAutoRefreshMins || 60
    ),
    addConfidenceMin: clampInteger(safe.addConfidenceMin, 50, 95, base.addConfidenceMin || 80),
    addSentimentMin: clampInteger(safe.addSentimentMin, 0, 90, base.addSentimentMin || 45),
    sellConfidenceMin: clampInteger(safe.sellConfidenceMin, 50, 95, base.sellConfidenceMin || 60),
    sellSentimentMax: clampInteger(safe.sellSentimentMax, -60, 20, base.sellSentimentMax || -5),
  };
}

function normalizeRuntimeMarketScoutState(input, fallback) {
  const safe = input && typeof input === "object" ? input : {};
  const base =
    fallback && typeof fallback === "object"
      ? fallback
      : createDefaultRuntimeMarketScoutState(Date.now());
  const checkedAtBySymbol = {};
  const incomingChecked =
    safe.checkedAtBySymbol && typeof safe.checkedAtBySymbol === "object"
      ? safe.checkedAtBySymbol
      : base.checkedAtBySymbol && typeof base.checkedAtBySymbol === "object"
        ? base.checkedAtBySymbol
        : {};
  for (const [symbol, ts] of Object.entries(incomingChecked)) {
    const clean = parseSingleSymbol(symbol);
    if (!clean) continue;
    checkedAtBySymbol[clean] = normalizeTimestamp(ts, 0);
  }

  const recentMessages = Array.isArray(safe.recentMessages)
    ? safe.recentMessages
        .map((item) => cleanModelText(item, 260, ""))
        .filter(Boolean)
        .slice(0, MARKET_SCOUT_MAX_RECENT_MESSAGES)
    : Array.isArray(base.recentMessages)
      ? base.recentMessages
          .map((item) => cleanModelText(item, 260, ""))
          .filter(Boolean)
          .slice(0, MARKET_SCOUT_MAX_RECENT_MESSAGES)
      : [];
  const hotSymbols = (
    Array.isArray(safe.hotSymbols)
      ? safe.hotSymbols
      : Array.isArray(base.hotSymbols)
        ? base.hotSymbols
        : []
  )
    .map((item) => parseSingleSymbol(item))
    .filter(Boolean)
    .filter((symbol, index, list) => list.indexOf(symbol) === index)
    .slice(0, MARKET_SCOUT_MAX_HOT_SYMBOLS);

  const universeSize = Math.max(1, getMarketScoutUniverse(hotSymbols).length);
  const baseCursor = clampInteger(base.cursor, 0, Math.max(0, universeSize - 1), 0);
  return {
    enabled: typeof safe.enabled === "boolean" ? safe.enabled : Boolean(base.enabled),
    intervalMins: clampInteger(
      safe.intervalMins,
      1,
      120,
      clampInteger(base.intervalMins, 1, 120, DEFAULT_MARKET_SCOUT_INTERVAL_MINS)
    ),
    lastRunAt: normalizeTimestamp(safe.lastRunAt, normalizeTimestamp(base.lastRunAt, 0)),
    lastHotRefreshAt: normalizeTimestamp(safe.lastHotRefreshAt, normalizeTimestamp(base.lastHotRefreshAt, 0)),
    cursor: clampInteger(safe.cursor, 0, Math.max(0, universeSize - 1), baseCursor),
    lastSymbol: parseSingleSymbol(safe.lastSymbol) || parseSingleSymbol(base.lastSymbol) || "",
    latestMessage: cleanModelText(safe.latestMessage, 260, base.latestMessage || "Research ticker idle."),
    hotSummary: cleanModelText(safe.hotSummary, 260, cleanModelText(base.hotSummary, 260, "")),
    hotSymbols,
    recentMessages,
    checkedAtBySymbol,
    createdAt: normalizeTimestamp(safe.createdAt, normalizeTimestamp(base.createdAt, Date.now())),
  };
}

function normalizeRuntimeWallet(input, fallback) {
  const safe = input && typeof input === "object" ? input : {};
  const base = fallback && typeof fallback === "object" ? fallback : createInitialRuntimeState().wallet;
  const fallbackWatchlist = Array.isArray(base.watchlist) ? base.watchlist : DEFAULT_SYMBOLS;
  const watchlist = Array.isArray(safe.watchlist)
    ? safe.watchlist
        .map((symbol) => parseSingleSymbol(symbol))
        .filter(Boolean)
        .filter((symbol, index, rows) => rows.indexOf(symbol) === index)
        .slice(0, MAX_WATCHLIST_SYMBOLS)
    : fallbackWatchlist.slice(0, MAX_WATCHLIST_SYMBOLS);
  if (watchlist.length === 0) watchlist.push(...DEFAULT_SYMBOLS.slice(0, 8));

  const prices = {};
  const incomingPrices = safe.prices && typeof safe.prices === "object" ? safe.prices : {};
  const fallbackPrices = base.prices && typeof base.prices === "object" ? base.prices : {};
  for (const symbol of watchlist) {
    const raw = incomingPrices[symbol] && typeof incomingPrices[symbol] === "object"
      ? incomingPrices[symbol]
      : fallbackPrices[symbol] || {};
    const price = asNumber(raw.price);
    const lastClose = asNumber(raw.lastClose);
    const cleanPrice = price && price > 0 ? round2(price) : 100;
    prices[symbol] = {
      price: cleanPrice,
      lastClose: lastClose && lastClose > 0 ? round2(lastClose) : cleanPrice,
    };
  }

  const positions = {};
  const incomingPositions = safe.positions && typeof safe.positions === "object" ? safe.positions : {};
  for (const symbol of watchlist) {
    const row = incomingPositions[symbol];
    if (!row || typeof row !== "object") continue;
    const sharesRaw = asNumber(row.shares);
    const shares = Number.isFinite(sharesRaw) && sharesRaw > 0 ? Math.floor(sharesRaw) : 0;
    const avgCost = asNumber(row.avgCost);
    if (shares <= 0 || !avgCost || avgCost <= 0) continue;
    positions[symbol] = {
      shares,
      avgCost: round2(avgCost),
    };
  }

  const selectedSymbol = parseSingleSymbol(safe.selectedSymbol);
  const lockedSymbolsRaw = Array.isArray(safe.lockedSymbols)
    ? safe.lockedSymbols
    : Array.isArray(base.lockedSymbols)
      ? base.lockedSymbols
      : [];
  const watchSet = new Set(watchlist);
  const lockedSymbols = Array.from(
    new Set(
      lockedSymbolsRaw
        .map((symbol) => parseSingleSymbol(symbol))
        .filter((symbol) => symbol && watchSet.has(symbol))
    )
  );
  return {
    cash: clampNumber(safe.cash, 0, Number.MAX_SAFE_INTEGER, base.cash || 100000),
    realizedPnl: asNumber(safe.realizedPnl) ? round2(asNumber(safe.realizedPnl)) : 0,
    selectedSymbol: selectedSymbol && watchlist.includes(selectedSymbol) ? selectedSymbol : watchlist[0],
    watchlist,
    lockedSymbols,
    prices,
    positions,
    transactions: normalizeRuntimeTransactions(safe.transactions),
  };
}

function normalizeRuntimeTransactions(input) {
  if (!Array.isArray(input)) return [];
  const rows = [];
  for (const raw of input) {
    if (!raw || typeof raw !== "object") continue;
    const side = String(raw.side || "").trim().toUpperCase();
    if (!(side === "BUY" || side === "SELL")) continue;
    const symbol = parseSingleSymbol(raw.symbol);
    const sharesRaw = asNumber(raw.shares);
    const shares = Number.isFinite(sharesRaw) && sharesRaw > 0 ? Math.floor(sharesRaw) : 0;
    const price = asNumber(raw.price);
    const total = asNumber(raw.total);
    if (!symbol || shares <= 0 || !price || price <= 0 || !total || total <= 0) continue;
    const ts = String(raw.timestamp || "");
    const id = String(raw.id || `${Date.now()}_${Math.random().toString(16).slice(2, 7)}`);
    rows.push({
      id: cleanModelText(id, 40, id),
      timestamp: ts || new Date().toISOString(),
      side,
      symbol,
      shares,
      price: round2(price),
      total: round2(total),
      realized: side === "SELL" && asNumber(raw.realized) ? round2(asNumber(raw.realized)) : undefined,
    });
  }
  return rows.slice(0, 100);
}

function normalizeRuntimeAiResearchCache(input) {
  if (!input || typeof input !== "object") return {};
  const rows = [];
  for (const raw of Object.values(input)) {
    if (!raw || typeof raw !== "object") continue;
    const symbol = parseSingleSymbol(raw.symbol);
    if (!symbol) continue;
    const provider = String(raw.provider || "openai")
      .trim()
      .toLowerCase();
    const model = parseAiModel(raw.model, OPENAI_DEFAULT_MODEL);
    const horizon = parseAiHorizon(raw.horizon);
    const fetchedAt = normalizeTimestamp(raw.fetchedAt, 0);
    if (fetchedAt <= 0) continue;
    const profile = normalizeAiResearchResult(raw.profile || {});
    const key = `${symbol}|${provider}|${model}|${horizon}`;
    rows.push({
      key,
      value: {
        symbol,
        provider: provider === "openai" ? "openai" : "openai",
        model,
        horizon,
        fetchedAt,
        profile,
      },
    });
  }
  rows.sort((a, b) => b.value.fetchedAt - a.value.fetchedAt);
  const out = {};
  for (const row of rows.slice(0, 300)) {
    out[row.key] = row.value;
  }
  return out;
}

function normalizeRuntimeAiRefreshMeta(input, watchlist) {
  const safe = input && typeof input === "object" ? input : {};
  const symbols = Array.isArray(watchlist) ? watchlist : [];
  const now = Date.now();
  const intervalMs = 60 * 60 * 1000;
  const out = {};
  for (const rawSymbol of symbols) {
    const symbol = parseSingleSymbol(rawSymbol);
    if (!symbol) continue;
    const row = safe[symbol] && typeof safe[symbol] === "object" ? safe[symbol] : {};
    const addedAt = normalizeTimestamp(row.addedAt, Math.max(1, now - getSymbolRefreshOffsetMs(symbol, intervalMs)));
    const lastUpdatedAt = normalizeTimestamp(row.lastUpdatedAt, 0);
    const lastAttemptAt = normalizeTimestamp(row.lastAttemptAt, 0);
    out[symbol] = {
      addedAt,
      lastUpdatedAt,
      lastAttemptAt,
    };
  }
  return out;
}

function normalizeRuntimeAutobotState(input) {
  const safe = input && typeof input === "object" ? input : {};
  const recommendation = normalizeRuntimeAutobotRecommendation(safe.latestRecommendation);
  const latestAction = safe.latestAutoAction && typeof safe.latestAutoAction === "object"
    ? {
      type: parseAiAction(safe.latestAutoAction.type),
      symbol: parseSingleSymbol(safe.latestAutoAction.symbol) || "",
        shares: Math.max(0, Math.floor(asNumber(safe.latestAutoAction.shares) || 0)),
        reason: cleanModelText(safe.latestAutoAction.reason, 360, "No reason provided."),
        executed: Boolean(safe.latestAutoAction.executed),
        at: normalizeTimestamp(safe.latestAutoAction.at, 0),
      }
    : null;

  return {
    enabled: Boolean(safe.enabled),
    lastRunAt: normalizeTimestamp(safe.lastRunAt, 0),
    latestThought: cleanModelText(safe.latestThought, 1000, ""),
    latestRecommendation: recommendation,
    latestAutoAction: latestAction,
    lastStatus: cleanModelText(safe.lastStatus, 180, "Disabled"),
    lastError: cleanModelText(safe.lastError, 240, ""),
  };
}

function normalizeRuntimeAutobotRecommendation(input) {
  if (!input || typeof input !== "object") return null;
  const action = parseAiAction(input.action || input.side);
  const symbol = parseSingleSymbol(input.symbol) || "";
  const requestedShares = asNumber(input.shares);
  const shares =
    action === "HOLD" ? 0 : Number.isFinite(requestedShares) && requestedShares > 0 ? Math.floor(requestedShares) : 0;
  return {
    action,
    side: action === "BUY" ? "buy" : action === "SELL" ? "sell" : "hold",
    symbol,
    shares,
    reason: cleanModelText(input.reason, 360, "No reason provided."),
    thought: cleanModelText(input.thought, 1000, cleanModelText(input.reason, 360, "No reason provided.")),
  };
}

function clampInteger(raw, min, max, fallback) {
  const num = Number(raw);
  if (!Number.isFinite(num)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(num)));
}

function normalizeTimestamp(raw, fallback) {
  const num = Number(raw);
  if (!Number.isFinite(num) || num <= 0) return fallback;
  return Math.floor(num);
}

function parseAiModel(raw, fallback) {
  const model = String(raw || fallback || OPENAI_DEFAULT_MODEL).trim();
  if (!model) return OPENAI_DEFAULT_MODEL;
  if (!/^[A-Za-z0-9._:-]{1,80}$/.test(model)) return OPENAI_DEFAULT_MODEL;
  return model;
}

function parseAiHorizon(raw) {
  const value = String(raw || "swing")
    .trim()
    .toLowerCase();
  if (value === "short" || value === "swing" || value === "long") return value;
  return "swing";
}

function normalizeAiQuoteInput(raw) {
  if (!raw || typeof raw !== "object") return null;
  const price = asNumber(raw.price);
  const lastClose = asNumber(raw.lastClose);
  if (!price || price <= 0) return null;
  return {
    price: round2(price),
    lastClose: lastClose && lastClose > 0 ? round2(lastClose) : round2(price),
  };
}

function normalizeAiCompanyInput(raw) {
  if (!raw || typeof raw !== "object") return null;
  return {
    name: String(raw.name || "").trim().slice(0, 160),
    exchange: String(raw.exchange || "").trim().slice(0, 40),
    industry: String(raw.industry || "").trim().slice(0, 120),
    country: String(raw.country || "").trim().slice(0, 80),
    currency: String(raw.currency || "").trim().slice(0, 24),
    ipo: String(raw.ipo || "").trim().slice(0, 24),
    marketCapitalization: asNumber(raw.marketCapitalization),
    shareOutstanding: asNumber(raw.shareOutstanding),
  };
}

function normalizeAiPositionInput(raw) {
  if (!raw || typeof raw !== "object") {
    return {
      shares: 0,
      avgCost: null,
      marketPrice: null,
      marketValue: 0,
      unrealizedPnl: 0,
      unrealizedPct: 0,
    };
  }

  const sharesRaw = asNumber(raw.shares);
  const shares = Number.isFinite(sharesRaw) && sharesRaw > 0 ? Math.floor(sharesRaw) : 0;
  const avgCost = asNumber(raw.avgCost);
  const marketPrice = asNumber(raw.marketPrice);
  const marketValue = asNumber(raw.marketValue);
  const unrealizedPnl = asNumber(raw.unrealizedPnl);
  const unrealizedPct = asNumber(raw.unrealizedPct);

  return {
    shares,
    avgCost: shares > 0 && avgCost && avgCost > 0 ? round2(avgCost) : null,
    marketPrice: marketPrice && marketPrice > 0 ? round2(marketPrice) : null,
    marketValue: marketValue && marketValue > 0 ? round2(marketValue) : 0,
    unrealizedPnl: unrealizedPnl ? round2(unrealizedPnl) : 0,
    unrealizedPct: unrealizedPct ? round2(unrealizedPct) : 0,
  };
}

function normalizeAiMarketContextInput(raw) {
  const safe = raw && typeof raw === "object" ? raw : {};
  const symbol = parseSingleSymbol(safe.symbol);
  const horizon = parseAiHorizon(safe.horizon);
  const quote = normalizeAiQuoteInput(safe.quote);

  const portfolioRaw = safe.portfolio && typeof safe.portfolio === "object" ? safe.portfolio : {};
  const portfolio = {
    cash: asNumber(portfolioRaw.cash),
    portfolioValue: asNumber(portfolioRaw.portfolioValue),
    unrealizedPnl: asNumber(portfolioRaw.unrealizedPnl),
    realizedPnl: asNumber(portfolioRaw.realizedPnl),
    totalEquity: asNumber(portfolioRaw.totalEquity),
  };

  const watchlist = Array.isArray(safe.watchlist)
    ? safe.watchlist
        .map((row) => {
          const rowSymbol = parseSingleSymbol(row?.symbol);
          if (!rowSymbol) return null;
          const sharesRaw = asNumber(row?.shares);
          const shares = Number.isFinite(sharesRaw) && sharesRaw > 0 ? Math.floor(sharesRaw) : 0;
          return {
            symbol: rowSymbol,
            price: asNumber(row?.price),
            lastClose: asNumber(row?.lastClose),
            dayChange: asNumber(row?.dayChange),
            dayPct: asNumber(row?.dayPct),
            shares,
            avgCost: asNumber(row?.avgCost),
            marketValue: asNumber(row?.marketValue),
            sentiment: asNumber(row?.sentiment),
            confidence: asNumber(row?.confidence),
            action: parseAiAction(row?.action),
          };
        })
        .filter(Boolean)
        .slice(0, 24)
    : [];

  const candleRaw = safe.candleSummary && typeof safe.candleSummary === "object" ? safe.candleSummary : null;
  const candleSummary = candleRaw
    ? {
        timeframe: parseTimeframe(candleRaw.timeframe),
        points: clampInteger(candleRaw.points, 2, 500, 0),
        start: String(candleRaw.start || "").slice(0, 40),
        end: String(candleRaw.end || "").slice(0, 40),
        open: asNumber(candleRaw.open),
        close: asNumber(candleRaw.close),
        high: asNumber(candleRaw.high),
        low: asNumber(candleRaw.low),
        changePct: asNumber(candleRaw.changePct),
        avgRangePct: asNumber(candleRaw.avgRangePct),
        closeSeries: Array.isArray(candleRaw.closeSeries)
          ? candleRaw.closeSeries
              .map((v) => asNumber(v))
              .filter((v) => Number.isFinite(v))
              .slice(-30)
          : [],
      }
    : null;

  return {
    symbol,
    horizon,
    quote,
    portfolio,
    watchlist,
    candleSummary,
  };
}

function normalizeAutobotContextInput(raw) {
  const safe = raw && typeof raw === "object" ? raw : {};
  const cash = asNumber(safe.cash);
  const realizedPnl = asNumber(safe.realizedPnl);
  const portfolioValue = asNumber(safe.portfolioValue);
  const unrealizedPnl = asNumber(safe.unrealizedPnl);
  const totalEquity = asNumber(safe.totalEquity);
  const selectedSymbol = parseSingleSymbol(safe.selectedSymbol);

  const positions = Array.isArray(safe.positions)
    ? safe.positions
        .map((row) => {
          const symbol = parseSingleSymbol(row?.symbol);
          const sharesRaw = asNumber(row?.shares);
          const shares = Number.isFinite(sharesRaw) && sharesRaw > 0 ? Math.floor(sharesRaw) : 0;
          if (!symbol || shares <= 0) return null;
          return {
            symbol,
            shares,
            avgCost: asNumber(row?.avgCost),
            invested: asNumber(row?.invested),
            marketPrice: asNumber(row?.marketPrice),
            marketValue: asNumber(row?.marketValue),
            unrealizedPnl: asNumber(row?.unrealizedPnl),
            unrealizedPct: asNumber(row?.unrealizedPct),
          };
        })
        .filter(Boolean)
        .slice(0, 120)
    : [];

  const snapshot = Array.isArray(safe.snapshot)
    ? safe.snapshot
        .map((row) => {
          const symbol = parseSingleSymbol(row?.symbol);
          if (!symbol) return null;
          return {
            symbol,
            price: asNumber(row?.price),
            lastClose: asNumber(row?.lastClose),
            dayChange: asNumber(row?.dayChange),
            dayPct: asNumber(row?.dayPct),
            hasResearch: Boolean(row?.hasResearch),
            research:
              row?.research && typeof row.research === "object"
                ? {
                    sentiment: asNumber(row.research.sentiment),
                    confidence: asNumber(row.research.confidence),
                    action: parseAiAction(row.research.action),
                    thesis: cleanModelText(row.research.thesis, 260, ""),
                    catalyst: cleanModelText(row.research.catalyst, 220, ""),
                    risk: cleanModelText(row.research.risk, 220, ""),
                    brief: cleanModelText(row.research.brief, 420, ""),
                  }
                : null,
          };
        })
        .filter(Boolean)
        .slice(0, 120)
    : [];

  return {
    cash: cash && cash >= 0 ? round2(cash) : 0,
    realizedPnl: realizedPnl ? round2(realizedPnl) : 0,
    portfolioValue: portfolioValue && portfolioValue >= 0 ? round2(portfolioValue) : 0,
    unrealizedPnl: unrealizedPnl ? round2(unrealizedPnl) : 0,
    totalEquity: totalEquity && totalEquity >= 0 ? round2(totalEquity) : 0,
    selectedSymbol,
    positions,
    snapshot,
  };
}

async function fetchOpenAiResearch({ symbol, model, horizon, quote, company, marketContext, position }) {
  const systemPrompt = [
    "You are a concise equity research assistant.",
    "Use the provided input data and live web search results only.",
    `Use only news published within the last ${RESEARCH_NEWS_LOOKBACK_DAYS} days.`,
    "Ignore older articles entirely.",
    "Recommendation must be aware of current holdings context.",
    "If shares > 0, interpret BUY as add-to-position, HOLD as maintain, SELL as reduce/exit.",
    "If shares = 0, interpret HOLD as watch/no-entry.",
    "Return a single JSON object with keys:",
    "sentiment (-100..100), confidence (0..100), action (BUY|HOLD|SELL), buyCashPct (0..1), trimPositionPct (0..1), thesis, catalyst, risk, brief.",
    "Keep thesis/catalyst/risk/brief factual and concise.",
    "Do not include markdown or any text outside JSON.",
  ].join(" ");
  const userPrompt = buildOpenAiResearchUserPrompt({
    symbol,
    horizon,
    quote,
    company,
    marketContext,
    position,
  });
  let parsed;
  try {
    parsed = await fetchOpenAiJsonObjectWithRecentWebSearch({
      model,
      systemPrompt,
      userPrompt,
    });
  } catch (error) {
    const details =
      error instanceof Error ? cleanModelText(error.message, 220, "Web search unavailable.") : "Web search unavailable.";
    throw new Error(`Research web search failed: ${details}`);
  }
  return normalizeAiResearchResult(parsed);
}

async function fetchOpenAiAutobotRecommendation({ model, horizon, context, maxActions }) {
  const cappedMaxActions = clampInteger(
    maxActions,
    1,
    MAX_AUTO_TRADE_ACTIONS_PER_CYCLE,
    DEFAULT_AUTO_TRADE_ACTIONS_PER_CYCLE
  );
  const systemPrompt = [
    "You are an autonomous trading assistant for a virtual paper-trading wallet.",
    "Use the provided context only.",
    `Generate an ordered trade plan with up to ${cappedMaxActions} actions for this cycle.`,
    "Recommendation must be position-aware: do not SELL symbols with zero shares.",
    "If no strong edge exists, return one HOLD action with shares=0.",
    "Return one JSON object with keys:",
    `actions (array of 1..${cappedMaxActions} items), thought.`,
    "Each actions item must have keys: action (BUY|SELL|HOLD), symbol, shares, reason.",
    "If action is HOLD, set shares to 0.",
    "Do not include markdown or any extra text.",
  ].join(" ");

  const userPrompt = buildOpenAiAutobotUserPrompt({ horizon, context, maxActions: cappedMaxActions });
  const parsed = await fetchOpenAiJsonObject({ model, systemPrompt, userPrompt });
  return normalizeAutobotRecommendationPlanResult(parsed, context, cappedMaxActions);
}

async function fetchOpenAiHotStockSymbols({ model }) {
  const systemPrompt = [
    "You are a US equity market scanner.",
    "Use only live web search results.",
    `Use only sources from the last ${MARKET_SCOUT_HOT_NEWS_LOOKBACK_DAYS} day(s). Ignore older sources.`,
    "Prioritize fresh catalysts, unusual volume, momentum, and heavy news/social discussion.",
    "Prefer common US tradable equities.",
    "Return exactly one JSON object with keys: symbols (array of tickers), summary (short string).",
    "Do not include markdown or any text outside JSON.",
  ].join(" ");

  const userPrompt = [
    `Current UTC date/time: ${new Date().toISOString()}`,
    "Build a dynamic list of currently hot US stock symbols for watchlist discovery.",
    `Return 8 to ${MARKET_SCOUT_MAX_HOT_SYMBOLS} symbols when available.`,
    "Output exactly one JSON object with required keys.",
  ].join("\n");

  const parsed = await fetchOpenAiJsonObjectWithRecentWebSearch({
    model,
    systemPrompt,
    userPrompt,
  });

  const rawSymbols = Array.isArray(parsed?.symbols)
    ? parsed.symbols
    : typeof parsed?.symbols === "string"
      ? parseSymbols(parsed.symbols)
      : Array.isArray(parsed?.tickers)
        ? parsed.tickers
        : [];
  const symbols = rawSymbols
    .map((symbol) => parseSingleSymbol(symbol))
    .filter(Boolean)
    .filter((symbol, index, list) => list.indexOf(symbol) === index)
    .slice(0, MARKET_SCOUT_MAX_HOT_SYMBOLS);

  return {
    symbols,
    summary: cleanModelText(parsed?.summary, 260, ""),
  };
}

async function fetchOpenAiJsonObject({ model, systemPrompt, userPrompt }) {
  const chatResponse = await fetch(buildOpenAiUrl("chat/completions"), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify({
      model,
      response_format: { type: "json_object" },
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userPrompt },
      ],
    }),
  });

  if (chatResponse.ok) {
    const payload = await chatResponse.json();
    const messageContent = payload?.choices?.[0]?.message?.content;
    return parseJsonObjectFromModelOutput(messageContent);
  }

  const chatErrorBody = await safeReadText(chatResponse);
  if (!shouldTryResponsesFallback(chatResponse.status, chatErrorBody, model)) {
    throw new Error(
      `OpenAI chat/completions failed (${chatResponse.status}): ${chatErrorBody.slice(0, 240)}`
    );
  }

  const responsesResponse = await fetch(buildOpenAiUrl("responses"), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify({
      model,
      input: [
        {
          role: "system",
          content: [{ type: "input_text", text: systemPrompt }],
        },
        {
          role: "user",
          content: [{ type: "input_text", text: userPrompt }],
        },
      ],
    }),
  });

  if (!responsesResponse.ok) {
    const responsesBody = await safeReadText(responsesResponse);
    throw new Error(
      `OpenAI responses failed (${responsesResponse.status}): ${responsesBody.slice(0, 240)}`
    );
  }

  const responsesPayload = await responsesResponse.json();
  const responseText = extractTextFromResponsesPayload(responsesPayload);
  return parseJsonObjectFromModelOutput(responseText);
}

async function fetchOpenAiJsonObjectWithRecentWebSearch({
  model,
  systemPrompt,
  userPrompt,
}) {
  const response = await fetch(buildOpenAiUrl("responses"), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify({
      model,
      input: [
        {
          role: "system",
          content: [{ type: "input_text", text: systemPrompt }],
        },
        {
          role: "user",
          content: [{ type: "input_text", text: userPrompt }],
        },
      ],
      tools: [
        {
          type: "web_search_preview",
          user_location: {
            type: "approximate",
            country: "US",
          },
          search_context_size: "medium",
        },
      ],
      tool_choice: "auto",
    }),
  });

  if (!response.ok) {
    const body = await safeReadText(response);
    throw new Error(`OpenAI web search responses failed (${response.status}): ${body.slice(0, 240)}`);
  }

  const payload = await response.json();
  const responseText = extractTextFromResponsesPayload(payload);
  if (!responseText) {
    throw new Error("OpenAI web search returned empty response.");
  }
  return parseJsonObjectFromModelOutput(responseText);
}

function shouldTryResponsesFallback(status, bodyText, model) {
  if (status === 404 || status === 405 || status === 422) return true;
  const lower = String(bodyText || "").toLowerCase();
  const lowerModel = String(model || "").toLowerCase();
  if (status === 400 && lower.includes("unsupported")) return true;
  if (status === 400 && lowerModel.startsWith("gpt-5")) return true;
  return false;
}

function extractTextFromResponsesPayload(payload) {
  if (payload && typeof payload.output_text === "string" && payload.output_text.trim()) {
    return payload.output_text;
  }

  const output = Array.isArray(payload?.output) ? payload.output : [];
  const chunks = [];
  for (const item of output) {
    const contents = Array.isArray(item?.content) ? item.content : [];
    for (const contentItem of contents) {
      const text = String(contentItem?.text || "").trim();
      if (!text) continue;
      chunks.push(text);
    }
  }

  return chunks.join("\n").trim();
}

function buildOpenAiResearchUserPrompt({ symbol, horizon, quote, company, marketContext, position }) {
  const horizonNotes = {
    short:
      "short: prioritize near-term catalysts (days to weeks), technical sentiment, and event risk.",
    swing:
      "swing: prioritize 1-3 month drivers, earnings trajectory, positioning, and valuation sensitivity.",
    long: "long: prioritize multi-quarter durability, competitive moat, and structural risks.",
  };

  return [
    `Current UTC date/time: ${new Date().toISOString()}`,
    `News recency requirement: only use articles from the last ${RESEARCH_NEWS_LOOKBACK_DAYS} days.`,
    `Symbol: ${symbol}`,
    `Time horizon: ${horizon}`,
    horizonNotes[horizon] || horizonNotes.swing,
    "",
    "Quote context:",
    quote
      ? `price=${quote.price}, lastClose=${quote.lastClose}`
      : "price and lastClose unavailable",
    "",
    "Company context:",
    company
      ? JSON.stringify(company)
      : "company profile unavailable",
    "",
    "Market context:",
    marketContext
      ? JSON.stringify(marketContext)
      : "market context unavailable",
    "",
    "Current position context:",
    position && position.shares > 0
      ? `shares=${position.shares}, avgCost=${position.avgCost}, marketPrice=${position.marketPrice}, marketValue=${position.marketValue}, unrealizedPnl=${position.unrealizedPnl}, unrealizedPct=${position.unrealizedPct}`
      : "shares=0 (no current position)",
    "",
    "Output exactly one JSON object with required keys.",
    "In brief/thesis, explicitly state whether to buy, hold, sell, or add relative to current position.",
  ].join("\n");
}

function buildOpenAiAutobotUserPrompt({ horizon, context, maxActions }) {
  const horizonNotes = {
    short: "Short horizon: prioritize near-term catalysts and risk control.",
    swing: "Swing horizon: prioritize 1-3 month setups and earnings/regime risks.",
    long: "Long horizon: prioritize durable fundamentals and multi-quarter risk/reward.",
  };

  const universeSymbols = Array.isArray(context.snapshot)
    ? context.snapshot.map((row) => parseSingleSymbol(row?.symbol)).filter(Boolean)
    : [];
  const compactContext = {
    horizon,
    cash: context.cash,
    realizedPnl: context.realizedPnl,
    portfolioValue: context.portfolioValue,
    unrealizedPnl: context.unrealizedPnl,
    totalEquity: context.totalEquity,
    universeSymbols,
    universeSize: universeSymbols.length,
    positions: context.positions,
    snapshot: context.snapshot,
  };

  return [
    `Time horizon: ${horizon}`,
    horizonNotes[horizon] || horizonNotes.swing,
    "",
    "Trading context JSON:",
    JSON.stringify(compactContext),
    "",
    "Task:",
    `Evaluate all symbols in snapshot and return an ordered trade plan for this cycle (max ${maxActions} actions).`,
    "Only include actions that should be attempted now; omit weak ideas.",
    "If there is no strong edge, return exactly one HOLD action with shares=0.",
    "For BUY/SELL choose an integer share size that is realistic versus cash/holdings in context.",
    "Do not bias toward any specific symbol because of UI selection or row order.",
  ].join("\n");
}

function parseJsonObjectFromModelOutput(rawContent) {
  if (rawContent && typeof rawContent === "object" && !Array.isArray(rawContent)) {
    return rawContent;
  }

  const text = String(rawContent || "").trim();
  if (!text) {
    throw new Error("OpenAI returned an empty response.");
  }

  const direct = safeParseJsonObject(text);
  if (direct) return direct;

  const firstBrace = text.indexOf("{");
  const lastBrace = text.lastIndexOf("}");
  if (firstBrace >= 0 && lastBrace > firstBrace) {
    const sliced = text.slice(firstBrace, lastBrace + 1);
    const parsed = safeParseJsonObject(sliced);
    if (parsed) return parsed;
  }

  throw new Error("OpenAI returned non-JSON content.");
}

function safeParseJsonObject(text) {
  try {
    const parsed = JSON.parse(text);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return parsed;
    }
    return null;
  } catch {
    return null;
  }
}

function normalizeAiResearchResult(raw) {
  const sentiment = clampNumber(raw?.sentiment, -100, 100, 0);
  const confidence = clampNumber(raw?.confidence, 0, 100, 50);
  const action = parseAiAction(raw?.action);
  const buyCashPct = clampNumber(raw?.buyCashPct, 0.01, 0.5, action === "BUY" ? 0.08 : 0.05);
  const trimPositionPct = clampNumber(
    raw?.trimPositionPct,
    0.05,
    1,
    action === "SELL" ? 0.25 : 0.2
  );
  const thesis = cleanModelText(raw?.thesis, 520, "No thesis provided by model.");
  const catalyst = cleanModelText(raw?.catalyst, 380, "No catalyst provided by model.");
  const risk = cleanModelText(raw?.risk, 380, "No risk provided by model.");
  const brief = cleanModelText(
    raw?.brief,
    1600,
    `${thesis}\n\nCatalyst: ${catalyst}\nRisk: ${risk}`
  );

  return {
    sentiment,
    confidence,
    action,
    buyCashPct: round4(buyCashPct),
    trimPositionPct: round4(trimPositionPct),
    thesis,
    catalyst,
    risk,
    brief,
  };
}

function normalizeAutobotRecommendationPlanResult(raw, context, maxActions) {
  const cappedMaxActions = clampInteger(
    maxActions,
    1,
    MAX_AUTO_TRADE_ACTIONS_PER_CYCLE,
    DEFAULT_AUTO_TRADE_ACTIONS_PER_CYCLE
  );
  const safe = raw && typeof raw === "object" ? raw : {};
  const sharedThought = cleanModelText(safe.thought, 1000, "");
  const candidates = Array.isArray(safe.actions)
    ? safe.actions
    : Array.isArray(safe.recommendations)
      ? safe.recommendations
      : [safe];
  const firstCandidate =
    Array.isArray(candidates) && candidates.length > 0 && candidates[0] && typeof candidates[0] === "object"
      ? candidates[0]
      : null;

  const recommendations = [];
  const seen = new Set();
  for (const row of candidates) {
    if (!row || typeof row !== "object") continue;
    const normalized = normalizeAutobotRecommendationResult(
      {
        action: row.action ?? row.side,
        symbol: row.symbol,
        shares: row.shares ?? row.qty,
        reason: row.reason,
        thought: row.thought || sharedThought,
      },
      context
    );
    if (!normalized || normalized.side === "hold" || normalized.shares <= 0) {
      continue;
    }
    const key = `${normalized.side}:${normalized.symbol}`;
    if (!normalized.symbol || seen.has(key)) continue;
    seen.add(key);
    recommendations.push(normalized);
    if (recommendations.length >= cappedMaxActions) break;
  }

  if (recommendations.length === 0) {
    const fallback = normalizeAutobotRecommendationResult(
      {
        action: "HOLD",
        symbol: "",
        shares: 0,
        reason: cleanModelText(firstCandidate?.reason ?? safe.reason, 360, "No strong edge across watchlist."),
        thought: sharedThought,
      },
      context
    );
    return {
      thought: cleanModelText(sharedThought, 1000, fallback.reason),
      recommendations: [fallback],
    };
  }

  const planSummary = recommendations
    .map((item) => `${item.action} ${item.shares} ${item.symbol}`)
    .join(" | ");
  const thought = cleanModelText(sharedThought, 1000, planSummary);

  return {
    thought,
    recommendations,
  };
}

function normalizeAutobotRecommendationResult(raw, context) {
  const trackedSymbols = Array.isArray(context?.snapshot)
    ? context.snapshot.map((row) => parseSingleSymbol(row?.symbol)).filter(Boolean)
    : [];
  const trackedSet = new Set(trackedSymbols);
  const heldSharesBySymbol = new Map();
  for (const position of Array.isArray(context?.positions) ? context.positions : []) {
    const symbol = parseSingleSymbol(position?.symbol);
    const shares = asNumber(position?.shares);
    if (!symbol || !shares || shares <= 0) continue;
    heldSharesBySymbol.set(symbol, Math.floor(shares));
  }
  const priceBySymbol = new Map();
  for (const row of Array.isArray(context?.snapshot) ? context.snapshot : []) {
    const symbol = parseSingleSymbol(row?.symbol);
    const price = asNumber(row?.price);
    if (!symbol || !price || price <= 0) continue;
    priceBySymbol.set(symbol, price);
  }

  const action = parseAiAction(raw?.action);
  let symbol = parseSingleSymbol(raw?.symbol);
  if (!symbol || !trackedSet.has(symbol)) {
    symbol = pickFallbackAutobotSymbol(action, context, trackedSymbols, heldSharesBySymbol, priceBySymbol);
  }

  const requestedShares = asNumber(raw?.shares);
  const cleanReason = cleanModelText(raw?.reason, 360, "No reason provided by model.");
  const cleanThought = cleanModelText(raw?.thought, 1000, cleanReason);

  if (action === "HOLD" || !symbol) {
    return {
      action: "HOLD",
      side: "hold",
      symbol: symbol || "",
      shares: 0,
      reason: cleanReason,
      thought: cleanThought,
    };
  }

  if (action === "BUY") {
    const cash = asNumber(context?.cash);
    const price = priceBySymbol.get(symbol) || 0;
    const maxAffordable = cash && price > 0 ? Math.floor(cash / price) : 0;
    if (maxAffordable <= 0) {
      return {
        action: "HOLD",
        side: "hold",
        symbol,
        shares: 0,
        reason: "Insufficient cash for a new buy based on current quote context.",
        thought: cleanThought,
      };
    }

    let shares = Number.isFinite(requestedShares) && requestedShares > 0 ? Math.floor(requestedShares) : 0;
    if (shares <= 0) {
      shares = Math.max(1, Math.floor(maxAffordable * 0.18));
    }
    shares = Math.max(1, Math.min(shares, maxAffordable));
    return {
      action: "BUY",
      side: "buy",
      symbol,
      shares,
      reason: cleanReason,
      thought: cleanThought,
    };
  }

  const heldShares = heldSharesBySymbol.get(symbol) || 0;
  if (heldShares <= 0) {
    return {
      action: "HOLD",
      side: "hold",
      symbol,
      shares: 0,
      reason: `Cannot SELL ${symbol} because there are no held shares.`,
      thought: cleanThought,
    };
  }

  let sellShares = Number.isFinite(requestedShares) && requestedShares > 0 ? Math.floor(requestedShares) : 0;
  if (sellShares <= 0) {
    sellShares = Math.max(1, Math.floor(heldShares * 0.3));
  }
  sellShares = Math.max(1, Math.min(sellShares, heldShares));
  return {
    action: "SELL",
    side: "sell",
    symbol,
    shares: sellShares,
    reason: cleanReason,
    thought: cleanThought,
  };
}

function pickFallbackAutobotSymbol(action, context, trackedSymbols, heldSharesBySymbol, priceBySymbol) {
  if (!Array.isArray(trackedSymbols) || trackedSymbols.length === 0) return "";

  const snapshotBySymbol = new Map();
  for (const row of Array.isArray(context?.snapshot) ? context.snapshot : []) {
    const symbol = parseSingleSymbol(row?.symbol);
    if (!symbol) continue;
    snapshotBySymbol.set(symbol, row);
  }

  if (action === "SELL") {
    let chosen = "";
    let minScore = Number.POSITIVE_INFINITY;
    for (const symbol of trackedSymbols) {
      const heldShares = heldSharesBySymbol.get(symbol) || 0;
      if (heldShares <= 0) continue;
      const row = snapshotBySymbol.get(symbol);
      const sentiment = asNumber(row?.research?.sentiment);
      const dayPct = asNumber(row?.dayPct);
      const score =
        (Number.isFinite(sentiment) ? sentiment : 0) + (Number.isFinite(dayPct) ? dayPct * 2 : 0);
      if (!chosen || score < minScore) {
        chosen = symbol;
        minScore = score;
      }
    }
    return chosen || trackedSymbols[0] || "";
  }

  if (action === "BUY") {
    let chosen = "";
    let maxScore = Number.NEGATIVE_INFINITY;
    for (const symbol of trackedSymbols) {
      const price = priceBySymbol.get(symbol) || 0;
      if (!price || price <= 0) continue;
      const row = snapshotBySymbol.get(symbol);
      const sentiment = asNumber(row?.research?.sentiment);
      const confidence = asNumber(row?.research?.confidence);
      const dayPct = asNumber(row?.dayPct);
      const heldShares = heldSharesBySymbol.get(symbol) || 0;
      const hasResearch = Boolean(row?.research);
      const score =
        (Number.isFinite(sentiment) ? sentiment : 0) +
        (Number.isFinite(confidence) ? confidence * 0.35 : 0) +
        (Number.isFinite(dayPct) ? dayPct * 2 : 0) +
        (heldShares > 0 ? 4 : 0) +
        (hasResearch ? 8 : -3);
      if (!chosen || score > maxScore) {
        chosen = symbol;
        maxScore = score;
      }
    }
    return chosen || trackedSymbols[0] || "";
  }

  return trackedSymbols[0] || "";
}

function parseAiAction(value) {
  const action = String(value || "")
    .trim()
    .toUpperCase();
  if (action === "BUY" || action === "SELL" || action === "HOLD") return action;
  return "HOLD";
}

function clampNumber(raw, min, max, fallback) {
  const num = Number(raw);
  if (!Number.isFinite(num)) return fallback;
  return Math.min(max, Math.max(min, num));
}

function round4(value) {
  return Math.round((value + Number.EPSILON) * 10000) / 10000;
}

function cleanModelText(value, maxLength, fallback) {
  const text = String(value || "").trim();
  if (!text) return fallback;
  if (text.length <= maxLength) return text;
  return `${text.slice(0, maxLength - 1)}...`;
}

function buildFinnhubUrl(pathname) {
  const base = String(FINNHUB_BASE_URL || "https://finnhub.io/api/v1").trim();
  const normalizedBase = base.endsWith("/") ? base : `${base}/`;
  const normalizedPath = String(pathname || "").replace(/^\/+/, "");
  return new URL(normalizedPath, normalizedBase);
}

function buildOpenAiUrl(pathname) {
  const base = String(OPENAI_BASE_URL || "https://api.openai.com/v1").trim();
  const normalizedBase = base.endsWith("/") ? base : `${base}/`;
  const normalizedPath = String(pathname || "").replace(/^\/+/, "");
  return new URL(normalizedPath, normalizedBase);
}

async function getCachedAssetUniverse() {
  const now = Date.now();
  if (cachedAssetUniverse.assets.length > 0 && now - cachedAssetUniverse.fetchedAt < ASSET_CACHE_TTL_MS) {
    return cachedAssetUniverse.assets;
  }

  const assets = await fetchAlpacaAssetUniverse();
  cachedAssetUniverse = {
    fetchedAt: now,
    assets,
  };
  return assets;
}

async function fetchAlpacaAssetUniverse() {
  const url = new URL("/v2/assets", ALPACA_TRADING_URL);
  url.searchParams.set("status", "active");
  url.searchParams.set("asset_class", "us_equity");

  const response = await fetch(url, {
    method: "GET",
    headers: {
      "APCA-API-KEY-ID": ALPACA_KEY_ID,
      "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    const body = await safeReadText(response);
    throw new Error(`Alpaca assets failed (${response.status}): ${body.slice(0, 180)}`);
  }

  const payload = await response.json();
  if (!Array.isArray(payload)) {
    throw new Error("Alpaca assets returned an unexpected payload.");
  }

  return payload
    .map((asset) => {
      const symbol = parseSingleSymbol(asset?.symbol);
      const name = String(asset?.name || "").trim();
      const exchange = String(asset?.exchange || "").trim();
      const status = String(asset?.status || "").trim();
      const assetClass = String(asset?.class || asset?.asset_class || "").trim();
      const tradable = Boolean(asset?.tradable);
      const marginable = asBooleanOrNull(asset?.marginable);
      const shortable = asBooleanOrNull(asset?.shortable);
      const easyToBorrow = asBooleanOrNull(asset?.easy_to_borrow);
      const fractionable = asBooleanOrNull(asset?.fractionable);
      if (!symbol || !name || !exchange) return null;
      return {
        symbol,
        name,
        exchange,
        status,
        assetClass,
        tradable,
        marginable,
        shortable,
        easyToBorrow,
        fractionable,
      };
    })
    .filter(Boolean);
}

function asBooleanOrNull(value) {
  if (typeof value === "boolean") return value;
  return null;
}

function asNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function round2(value) {
  return Math.round((value + Number.EPSILON) * 100) / 100;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, Number(ms) || 0)));
}

async function safeReadText(response) {
  try {
    return await response.text();
  } catch {
    return "";
  }
}

async function readJsonBody(req) {
  const chunks = [];
  let total = 0;
  const maxBytes = 1024 * 1024;

  for await (const chunk of req) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    total += buf.length;
    if (total > maxBytes) {
      throw new Error("Request body too large.");
    }
    chunks.push(buf);
  }

  if (chunks.length === 0) return {};

  const raw = Buffer.concat(chunks).toString("utf8").trim();
  if (!raw) return {};

  try {
    return JSON.parse(raw);
  } catch {
    throw new Error("Request body must be valid JSON.");
  }
}

function json(res, statusCode, payload) {
  const body = Buffer.from(JSON.stringify(payload));
  res.statusCode = statusCode;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Content-Length", body.length);
  res.end(body);
}

function loadDotEnv(filePath) {
  if (!fs.existsSync(filePath)) return;

  try {
    const content = fs.readFileSync(filePath, "utf8");
    const lines = content.split(/\r?\n/);

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue;

      const idx = trimmed.indexOf("=");
      if (idx === -1) continue;

      const key = trimmed.slice(0, idx).trim();
      const value = trimmed.slice(idx + 1).trim().replace(/^['"]|['"]$/g, "");
      if (!key) continue;

      if (process.env[key] === undefined) {
        process.env[key] = value;
      }
    }
  } catch {
    // Ignore .env parse failures and continue with existing environment.
  }
}
