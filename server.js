const fs = require("fs");
const fsp = require("fs/promises");
const http = require("http");
const path = require("path");

loadDotEnv(path.join(__dirname, ".env"));

const PORT = Number(process.env.PORT) || 3000;
const ROOT_DIR = __dirname;

const ALPACA_MARKET_DATA_URL =
  process.env.ALPACA_MARKET_DATA_URL || "https://data.alpaca.markets";
const ALPACA_FEED = (process.env.ALPACA_FEED || "iex").toLowerCase();
const ALPACA_KEY_ID = process.env.ALPACA_KEY_ID || process.env.APCA_API_KEY_ID || "";
const ALPACA_SECRET_KEY =
  process.env.ALPACA_SECRET_KEY || process.env.APCA_API_SECRET_KEY || "";

const DEFAULT_SYMBOLS = ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AMD"];

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
  url.searchParams.set("symbols", symbol);
  url.searchParams.set("timeframe", timeframe);
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
  const allowed = new Set(["1Min", "5Min", "15Min", "1Hour", "1Day"]);
  return allowed.has(value) ? value : "1Min";
}

function parseLimit(raw) {
  const value = Number(raw);
  if (!Number.isFinite(value)) return 50;
  return Math.max(10, Math.min(200, Math.floor(value)));
}

function asNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function round2(value) {
  return Math.round((value + Number.EPSILON) * 100) / 100;
}

async function safeReadText(response) {
  try {
    return await response.text();
  } catch {
    return "";
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
