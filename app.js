const STORAGE_KEY = "matilda.wallet.v1";
const STARTING_CASH = 100000;
const PRICE_TICK_MS = 5000;
const CANDLE_REFRESH_MS = 30000;
const API_QUOTES_URL = "/api/quotes";
const API_CANDLES_URL = "/api/candles";
const DEFAULT_CHART_SCALE = "days";
const CHART_MODES = {
  hours: {
    label: "Hours",
    timeframe: "1Hour",
    fetchLimit: 96,
    displayLimit: 96,
    stepMs: 60 * 60 * 1000,
    aggregate: "none",
  },
  days: {
    label: "Days",
    timeframe: "1Day",
    fetchLimit: 90,
    displayLimit: 90,
    stepMs: 24 * 60 * 60 * 1000,
    aggregate: "none",
  },
  months: {
    label: "Months",
    timeframe: "1Day",
    fetchLimit: 365,
    displayLimit: 24,
    stepMs: 30 * 24 * 60 * 60 * 1000,
    aggregate: "month",
  },
};

const STOCKS = [
  { symbol: "AAPL", basePrice: 192.35 },
  { symbol: "MSFT", basePrice: 428.12 },
  { symbol: "NVDA", basePrice: 118.9 },
  { symbol: "AMZN", basePrice: 177.64 },
  { symbol: "META", basePrice: 485.32 },
  { symbol: "GOOGL", basePrice: 165.47 },
  { symbol: "TSLA", basePrice: 193.24 },
  { symbol: "AMD", basePrice: 173.65 },
];

const MOCK_AI_RESEARCH = {
  AAPL: {
    sentiment: 63,
    confidence: 78,
    action: "BUY",
    buyCashPct: 0.11,
    trimPositionPct: 0.2,
    thesis:
      "Revenue mix is broad and cash flow remains resilient. Hardware cycle looks stable with services cushioning downside.",
    catalyst: "Enterprise refresh and services margin expansion.",
    risk: "Consumer spending slowdown and supply chain concentration.",
  },
  MSFT: {
    sentiment: 58,
    confidence: 81,
    action: "BUY",
    buyCashPct: 0.09,
    trimPositionPct: 0.2,
    thesis:
      "Cloud demand remains durable and software pricing power is strong. Earnings quality is high relative to peers.",
    catalyst: "Commercial cloud workload growth and AI software attach.",
    risk: "Large-cap valuation sensitivity to rate moves.",
  },
  NVDA: {
    sentiment: 36,
    confidence: 70,
    action: "HOLD",
    buyCashPct: 0.05,
    trimPositionPct: 0.25,
    thesis:
      "Long-term growth story remains attractive, but short-term volatility is elevated after major runs.",
    catalyst: "Data center demand and next-gen chip launches.",
    risk: "Crowded positioning and cyclical capex pullbacks.",
  },
  AMZN: {
    sentiment: 49,
    confidence: 72,
    action: "BUY",
    buyCashPct: 0.08,
    trimPositionPct: 0.2,
    thesis:
      "Retail efficiency trend is improving and cloud profitability can support higher operating leverage.",
    catalyst: "Cost discipline and ad business growth.",
    risk: "Consumer demand swings and logistics cost pressure.",
  },
  META: {
    sentiment: 24,
    confidence: 67,
    action: "HOLD",
    buyCashPct: 0.05,
    trimPositionPct: 0.2,
    thesis:
      "Ad momentum is constructive, but upside may be balanced by heavy infrastructure and model spending.",
    catalyst: "Ad monetization and engagement growth.",
    risk: "Regulatory actions and AI compute cost inflation.",
  },
  GOOGL: {
    sentiment: 42,
    confidence: 74,
    action: "BUY",
    buyCashPct: 0.08,
    trimPositionPct: 0.2,
    thesis:
      "Core search cash generation remains strong while cloud and productivity stack adoption keeps improving.",
    catalyst: "Cloud margin expansion and AI feature monetization.",
    risk: "Antitrust headlines and ad cyclicality.",
  },
  TSLA: {
    sentiment: -18,
    confidence: 65,
    action: "SELL",
    buyCashPct: 0.04,
    trimPositionPct: 0.32,
    thesis:
      "Execution is still strong, but demand elasticity and margin pressure raise near-term uncertainty.",
    catalyst: "New model ramps and software revenue traction.",
    risk: "Price competition and macro-sensitive demand.",
  },
  AMD: {
    sentiment: -26,
    confidence: 62,
    action: "SELL",
    buyCashPct: 0.03,
    trimPositionPct: 0.28,
    thesis:
      "Product roadmap is compelling, but trading setup looks extended after fast gains versus fundamentals.",
    catalyst: "Data center share gains and AI server adoption.",
    risk: "Competitive pricing pressure and inventory cycles.",
  },
};

const els = {
  symbolSelect: document.getElementById("symbolSelect"),
  sideSelect: document.getElementById("sideSelect"),
  sharesInput: document.getElementById("sharesInput"),
  tradeForm: document.getElementById("tradeForm"),
  tradeMessage: document.getElementById("tradeMessage"),
  resetWalletBtn: document.getElementById("resetWalletBtn"),
  marketBody: document.getElementById("marketBody"),
  positionsBody: document.getElementById("positionsBody"),
  activityList: document.getElementById("activityList"),
  cashValue: document.getElementById("cashValue"),
  portfolioValue: document.getElementById("portfolioValue"),
  equityValue: document.getElementById("equityValue"),
  unrealizedValue: document.getElementById("unrealizedValue"),
  realizedValue: document.getElementById("realizedValue"),
  marketSourceStatus: document.getElementById("marketSourceStatus"),
  chartTitle: document.getElementById("chartTitle"),
  chartStatus: document.getElementById("chartStatus"),
  candleSvg: document.getElementById("candleSvg"),
  timeframeButtons: Array.from(document.querySelectorAll(".timeframe-btn")),
  aiTitle: document.getElementById("aiTitle"),
  aiStatus: document.getElementById("aiStatus"),
  aiSentimentBadge: document.getElementById("aiSentimentBadge"),
  aiSentimentFill: document.getElementById("aiSentimentFill"),
  aiConfidenceValue: document.getElementById("aiConfidenceValue"),
  aiConfidenceFill: document.getElementById("aiConfidenceFill"),
  aiRecommendationText: document.getElementById("aiRecommendationText"),
  applyAiRecommendationBtn: document.getElementById("applyAiRecommendationBtn"),
  aiResearchArea: document.getElementById("aiResearchArea"),
};

let state = hydrateState();
let isRefreshingPrices = false;
let isRefreshingCandles = false;
let chartCandles = [];
let lastCandleRefreshAt = 0;
let marketSourceLabel = "Source: Loading...";
let candleSourceLabel = "Select a symbol to load chart data";
let currentAiRecommendation = null;

initializeApp();

async function initializeApp() {
  populateSymbolSelect();
  bindEvents();
  if (!state.selectedSymbol || !state.prices[state.selectedSymbol]) {
    state.selectedSymbol = STOCKS[0].symbol;
  }
  if (!CHART_MODES[state.chartScale]) {
    state.chartScale = DEFAULT_CHART_SCALE;
  }
  els.symbolSelect.value = state.selectedSymbol;
  renderAll();
  await refreshPrices();
  await refreshCandlesForSelectedSymbol(true);
  window.setInterval(() => {
    void refreshPrices();
  }, PRICE_TICK_MS);
}

function populateSymbolSelect() {
  els.symbolSelect.innerHTML = STOCKS.map(
    (stock) => `<option value="${stock.symbol}">${stock.symbol}</option>`
  ).join("");
}

function bindEvents() {
  els.tradeForm.addEventListener("submit", onTradeSubmit);
  els.resetWalletBtn.addEventListener("click", resetWallet);
  els.symbolSelect.addEventListener("change", onSymbolChangeFromTradeForm);
  els.marketBody.addEventListener("click", onMarketSymbolClick);
  els.applyAiRecommendationBtn.addEventListener("click", applyAiRecommendationToTicket);
  for (const btn of els.timeframeButtons) {
    btn.addEventListener("click", onChartScaleButtonClick);
  }
}

function onSymbolChangeFromTradeForm() {
  const symbol = String(els.symbolSelect.value || "").toUpperCase();
  if (!symbol || !state.prices[symbol]) return;

  state.selectedSymbol = symbol;
  persistState();
  renderAll();
  void refreshCandlesForSelectedSymbol(true);
}

function onMarketSymbolClick(event) {
  const target = event.target;
  if (!(target instanceof Element)) return;

  const button = target.closest(".symbol-link");
  if (!button) return;

  const symbol = String(button.getAttribute("data-symbol") || "").toUpperCase();
  if (!symbol || !state.prices[symbol]) return;

  state.selectedSymbol = symbol;
  els.symbolSelect.value = symbol;
  persistState();
  renderAll();
  void refreshCandlesForSelectedSymbol(true);
}

function onChartScaleButtonClick(event) {
  const target = event.currentTarget;
  if (!(target instanceof HTMLElement)) return;

  const nextScale = String(target.dataset.chartScale || "").toLowerCase();
  if (!CHART_MODES[nextScale]) return;
  if (nextScale === state.chartScale) return;

  state.chartScale = nextScale;
  persistState();
  renderAll();
  void refreshCandlesForSelectedSymbol(true);
}

function onTradeSubmit(event) {
  event.preventDefault();

  const symbol = els.symbolSelect.value;
  const side = els.sideSelect.value;
  const shares = Number(els.sharesInput.value);

  if (!Number.isInteger(shares) || shares <= 0) {
    setTradeMessage("Shares must be a positive whole number.", "down");
    return;
  }

  const quote = state.prices[symbol];
  if (!quote) {
    setTradeMessage("Quote unavailable for that symbol.", "down");
    return;
  }

  if (side === "buy") {
    executeBuy(symbol, shares, quote.price);
  } else {
    executeSell(symbol, shares, quote.price);
  }
}

function executeBuy(symbol, shares, price) {
  const cost = round2(shares * price);

  if (cost > state.cash) {
    setTradeMessage(
      `Insufficient cash. Need ${fmtMoney(cost)} but have ${fmtMoney(state.cash)}.`,
      "down"
    );
    return;
  }

  const existing = state.positions[symbol];
  if (existing) {
    const totalShares = existing.shares + shares;
    existing.avgCost = round2((existing.avgCost * existing.shares + cost) / totalShares);
    existing.shares = totalShares;
  } else {
    state.positions[symbol] = {
      shares,
      avgCost: price,
    };
  }

  state.cash = round2(state.cash - cost);
  pushTransaction({
    side: "BUY",
    symbol,
    shares,
    price,
    total: cost,
  });

  setTradeMessage(`Bought ${shares} ${symbol} @ ${fmtMoney(price)}.`, "up");
  persistAndRender();
}

function executeSell(symbol, shares, price) {
  const position = state.positions[symbol];
  if (!position || position.shares < shares) {
    setTradeMessage(`Not enough ${symbol} shares to sell.`, "down");
    return;
  }

  const proceeds = round2(shares * price);
  const realized = round2((price - position.avgCost) * shares);

  position.shares -= shares;
  if (position.shares === 0) {
    delete state.positions[symbol];
  }

  state.cash = round2(state.cash + proceeds);
  state.realizedPnl = round2(state.realizedPnl + realized);

  pushTransaction({
    side: "SELL",
    symbol,
    shares,
    price,
    total: proceeds,
    realized,
  });

  const resultTone = realized >= 0 ? "up" : "down";
  setTradeMessage(
    `Sold ${shares} ${symbol} @ ${fmtMoney(price)} | Realized ${fmtMoney(realized)}.`,
    resultTone
  );
  persistAndRender();
}

function pushTransaction(tx) {
  state.transactions.unshift({
    ...tx,
    timestamp: new Date().toISOString(),
    id: `${Date.now()}_${Math.random().toString(16).slice(2, 7)}`,
  });
  state.transactions = state.transactions.slice(0, 100);
}

async function refreshPrices() {
  if (isRefreshingPrices) return;
  isRefreshingPrices = true;

  try {
    const incoming = await fetchLiveQuotes();
    applyLiveQuotes(incoming);
    marketSourceLabel = "Source: Alpaca live quotes";
  } catch (error) {
    updatePricesWithSimulation();
    marketSourceLabel = "Source: Simulation fallback (Alpaca unavailable)";
    console.warn("Live quote refresh failed; using simulation fallback.", error);
  } finally {
    persistAndRender();
    isRefreshingPrices = false;

    if (Date.now() - lastCandleRefreshAt >= CANDLE_REFRESH_MS) {
      void refreshCandlesForSelectedSymbol(false);
    }
  }
}

async function fetchLiveQuotes() {
  const symbols = STOCKS.map((stock) => stock.symbol).join(",");
  const endpoint = `${API_QUOTES_URL}?symbols=${encodeURIComponent(symbols)}`;

  const response = await fetch(endpoint, {
    headers: {
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    let details = "";
    try {
      const body = await response.json();
      details = body?.error ? `: ${body.error}` : "";
    } catch {
      details = "";
    }
    throw new Error(`Quote endpoint returned ${response.status}${details}`);
  }

  const payload = await response.json();
  if (!Array.isArray(payload.quotes) || payload.quotes.length === 0) {
    throw new Error("Quote endpoint returned no symbols.");
  }

  return payload.quotes;
}

function applyLiveQuotes(quotes) {
  for (const quote of quotes) {
    const symbol = String(quote.symbol || "").toUpperCase();
    if (!symbol || !state.prices[symbol]) continue;

    const price = Number(quote.price);
    const lastClose = Number(quote.lastClose);
    if (!Number.isFinite(price) || price <= 0) continue;

    const current = state.prices[symbol];
    state.prices[symbol] = {
      price: round2(price),
      lastClose:
        Number.isFinite(lastClose) && lastClose > 0 ? round2(lastClose) : current.lastClose,
    };
  }
}

function updatePricesWithSimulation() {
  for (const stock of STOCKS) {
    const quote = state.prices[stock.symbol];
    const drift = Math.random() * 0.036 - 0.018;
    const next = Math.max(1, quote.price * (1 + drift));
    quote.price = round2(next);
  }
}

async function refreshCandlesForSelectedSymbol(force = false) {
  const symbol = state.selectedSymbol;
  const mode = getChartMode();
  if (!symbol) return;
  if (isRefreshingCandles) return;
  if (!force && Date.now() - lastCandleRefreshAt < CANDLE_REFRESH_MS) return;

  isRefreshingCandles = true;
  candleSourceLabel = `Loading ${symbol} ${mode.label.toLowerCase()} candles...`;
  renderCandleChart();

  try {
    const rawCandles = await fetchLiveCandles(symbol, mode.timeframe, mode.fetchLimit);
    const candles = transformCandlesForMode(rawCandles, mode);
    if (symbol !== state.selectedSymbol) return;
    chartCandles = candles;
    candleSourceLabel =
      mode.aggregate === "month"
        ? "Source: Alpaca monthly candles (aggregated from daily bars)"
        : `Source: Alpaca ${mode.label.toLowerCase()} candles`;
  } catch (error) {
    if (symbol !== state.selectedSymbol) return;
    chartCandles = buildFallbackCandles(symbol, mode);
    candleSourceLabel = `Source: Simulated ${mode.label.toLowerCase()} candles`;
    console.warn("Live candles failed; using simulation fallback.", error);
  } finally {
    if (symbol === state.selectedSymbol) {
      lastCandleRefreshAt = Date.now();
      renderCandleChart();
    }
    isRefreshingCandles = false;
  }
}

async function fetchLiveCandles(symbol, timeframe, limit) {
  const endpoint =
    `${API_CANDLES_URL}?symbol=${encodeURIComponent(symbol)}` +
    `&timeframe=${encodeURIComponent(timeframe)}` +
    `&limit=${encodeURIComponent(limit)}`;

  const response = await fetch(endpoint, {
    headers: {
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    let details = "";
    try {
      const body = await response.json();
      details = body?.error ? `: ${body.error}` : "";
    } catch {
      details = "";
    }
    throw new Error(`Candles endpoint returned ${response.status}${details}`);
  }

  const payload = await response.json();
  if (!Array.isArray(payload.candles) || payload.candles.length === 0) {
    throw new Error("Candles endpoint returned no data.");
  }

  const normalized = payload.candles
    .map((row) => {
      const o = Number(row.o);
      const h = Number(row.h);
      const l = Number(row.l);
      const c = Number(row.c);
      const t = String(row.t || "");
      if (!Number.isFinite(o) || !Number.isFinite(h) || !Number.isFinite(l) || !Number.isFinite(c)) {
        return null;
      }
      if (!t) return null;
      return { o, h, l, c, t };
    })
    .filter(Boolean)
    .sort((a, b) => Date.parse(a.t) - Date.parse(b.t));

  if (normalized.length === 0) {
    throw new Error("Candles endpoint returned invalid candle rows.");
  }

  return normalized.slice(-limit);
}

function buildFallbackCandles(symbol, mode) {
  const now = Date.now();
  const currentPrice = state.prices[symbol]?.price || 100;
  const candles = [];
  let previousClose = currentPrice;
  const limit = mode?.displayLimit || CHART_MODES[DEFAULT_CHART_SCALE].displayLimit;
  const stepMs = mode?.stepMs || CHART_MODES[DEFAULT_CHART_SCALE].stepMs;

  for (let i = limit - 1; i >= 0; i -= 1) {
    const drift = (Math.random() * 0.014 - 0.007) * previousClose;
    const open = previousClose;
    const close = Math.max(1, open + drift);
    const high = Math.max(open, close) + Math.random() * previousClose * 0.003;
    const low = Math.min(open, close) - Math.random() * previousClose * 0.003;

    candles.push({
      t: new Date(now - i * stepMs).toISOString(),
      o: round2(open),
      h: round2(Math.max(high, open, close)),
      l: round2(Math.max(0.01, Math.min(low, open, close))),
      c: round2(close),
    });

    previousClose = close;
  }

  return candles;
}

function resetWallet() {
  const confirmed = window.confirm(
    "Reset Matilda wallet? This clears all virtual positions and history."
  );
  if (!confirmed) return;

  const priorSymbol = state.selectedSymbol;
  state = createInitialState();
  if (priorSymbol && state.prices[priorSymbol]) {
    state.selectedSymbol = priorSymbol;
  }
  els.symbolSelect.value = state.selectedSymbol;
  chartCandles = [];
  setTradeMessage("Wallet reset to starting balance.", "up");
  persistAndRender();
  void refreshCandlesForSelectedSymbol(true);
}

function persistAndRender() {
  persistState();
  renderAll();
}

function renderAll() {
  renderSummary();
  renderMarket();
  renderAiPanel();
  renderPositions();
  renderActivity();
  renderCandleChart();
  els.marketSourceStatus.textContent = marketSourceLabel;
}

function renderSummary() {
  const portfolio = calcPortfolioValue();
  const unrealized = calcUnrealizedPnl();
  const equity = round2(state.cash + portfolio);

  els.cashValue.textContent = fmtMoney(state.cash);
  els.portfolioValue.textContent = fmtMoney(portfolio);
  els.equityValue.textContent = fmtMoney(equity);
  els.unrealizedValue.textContent = fmtMoney(unrealized);
  els.realizedValue.textContent = fmtMoney(state.realizedPnl);

  applyPnLClass(els.unrealizedValue, unrealized);
  applyPnLClass(els.realizedValue, state.realizedPnl);
}

function renderMarket() {
  const sortedStocks = [...STOCKS].sort((a, b) => {
    const aConfidence = clamp(Math.round(getAiProfile(a.symbol).confidence), 0, 100);
    const bConfidence = clamp(Math.round(getAiProfile(b.symbol).confidence), 0, 100);
    if (bConfidence !== aConfidence) return bConfidence - aConfidence;
    return a.symbol.localeCompare(b.symbol);
  });

  els.marketBody.innerHTML = sortedStocks.map((stock) => {
    const quote = state.prices[stock.symbol];
    const profile = getAiProfile(stock.symbol);
    const confidence = clamp(Math.round(profile.confidence), 0, 100);
    const confidenceTone = confidence >= 75 ? "high" : confidence >= 55 ? "mid" : "low";
    const dayChange = round2(quote.price - quote.lastClose);
    const dayPct = quote.lastClose > 0 ? round2((dayChange / quote.lastClose) * 100) : 0;
    const cls = dayChange >= 0 ? "up" : "down";
    const active = stock.symbol === state.selectedSymbol ? "is-active" : "";
    return `
      <tr>
        <td>
          <button type="button" class="symbol-link ${active}" data-symbol="${stock.symbol}">
            ${stock.symbol}
          </button>
        </td>
        <td>${fmtMoney(quote.price)}</td>
        <td>
          <span class="confidence-chip ${confidenceTone}">${confidence}%</span>
        </td>
        <td class="${cls}">
          ${signedMoney(dayChange)} (${signedPct(dayPct)})
        </td>
      </tr>
    `;
  }).join("");
}

function renderAiPanel() {
  const symbol = state.selectedSymbol || STOCKS[0].symbol;
  const quote = state.prices[symbol];
  const profile = getAiProfile(symbol);
  const sentiment = clamp(Math.round(profile.sentiment), -100, 100);
  const confidence = clamp(Math.round(profile.confidence), 0, 100);
  const sentimentWidth = ((sentiment + 100) / 200) * 100;
  const recommendation = buildAiRecommendation(symbol, profile);

  currentAiRecommendation = recommendation;
  els.aiTitle.textContent = `AI Research: ${symbol}`;
  els.aiStatus.textContent = "Mock battle analyst model (placeholder until API integration)";
  els.aiSentimentBadge.textContent = `${sentiment > 0 ? "+" : ""}${sentiment} / 100`;
  els.aiSentimentBadge.classList.remove("up", "down");
  if (sentiment > 0) els.aiSentimentBadge.classList.add("up");
  if (sentiment < 0) els.aiSentimentBadge.classList.add("down");

  els.aiSentimentFill.style.width = `${sentimentWidth.toFixed(1)}%`;
  els.aiConfidenceValue.textContent = `${confidence}%`;
  els.aiConfidenceFill.style.width = `${confidence.toFixed(1)}%`;

  els.aiRecommendationText.textContent = recommendation.message;
  els.applyAiRecommendationBtn.disabled = !recommendation.canApply;
  els.applyAiRecommendationBtn.textContent = recommendation.canApply
    ? `Apply ${recommendation.side.toUpperCase()} ${recommendation.shares} Shares`
    : "No Action To Apply";

  els.aiResearchArea.value = buildAiResearchText(symbol, quote, profile);
}

function getAiProfile(symbol) {
  const fromMap = MOCK_AI_RESEARCH[symbol];
  if (fromMap) return fromMap;

  return {
    sentiment: 0,
    confidence: 50,
    action: "HOLD",
    buyCashPct: 0.05,
    trimPositionPct: 0.25,
    thesis: "No profile found for this symbol.",
    catalyst: "N/A",
    risk: "N/A",
  };
}

function buildAiRecommendation(symbol, profile) {
  const quote = state.prices[symbol];
  if (!quote) {
    return {
      canApply: false,
      side: "buy",
      shares: 0,
      message: "No live quote available for recommendation sizing.",
    };
  }

  const action = String(profile.action || "HOLD").toUpperCase();

  if (action === "BUY") {
    const allocation = clamp(Number(profile.buyCashPct || 0.08), 0.01, 0.35);
    const budget = round2(state.cash * allocation);
    const shares = Math.floor(budget / quote.price);
    if (shares <= 0) {
      return {
        canApply: false,
        side: "buy",
        shares: 0,
        message: `Buy signal, but cash is too low for 1 share at ${fmtMoney(quote.price)}.`,
      };
    }

    return {
      canApply: true,
      side: "buy",
      shares,
      message: `BUY ${shares} shares (~${fmtMoney(
        shares * quote.price
      )}, ${Math.round(allocation * 100)}% of cash).`,
    };
  }

  if (action === "SELL") {
    const heldShares = state.positions[symbol]?.shares || 0;
    const trimPct = clamp(Number(profile.trimPositionPct || 0.25), 0.05, 1);
    const shares = heldShares > 0 ? Math.max(1, Math.floor(heldShares * trimPct)) : 0;

    if (shares <= 0) {
      return {
        canApply: false,
        side: "sell",
        shares: 0,
        message: "SELL signal, but you currently hold 0 shares in this symbol.",
      };
    }

    return {
      canApply: true,
      side: "sell",
      shares,
      message: `SELL ${shares} shares (~${fmtMoney(
        shares * quote.price
      )}, trim ${Math.round(trimPct * 100)}% of position).`,
    };
  }

  return {
    canApply: false,
    side: "buy",
    shares: 0,
    message: "HOLD. No position size change recommended right now.",
  };
}

function buildAiResearchText(symbol, quote, profile) {
  const price = quote ? fmtMoney(quote.price) : "N/A";
  const lastClose = quote ? fmtMoney(quote.lastClose) : "N/A";
  return [
    `${symbol} mock AI snapshot`,
    `Current price: ${price} | Prior close: ${lastClose}`,
    "",
    `Thesis: ${profile.thesis}`,
    `Catalyst: ${profile.catalyst}`,
    `Risk: ${profile.risk}`,
    "",
    "Model note: This is synthetic placeholder output for interface testing.",
  ].join("\n");
}

function applyAiRecommendationToTicket() {
  const symbol = state.selectedSymbol;
  const recommendation = currentAiRecommendation;
  if (!symbol || !recommendation) return;
  if (!recommendation.canApply) {
    setTradeMessage("AI recommendation is not actionable right now.", "down");
    return;
  }

  els.symbolSelect.value = symbol;
  els.sideSelect.value = recommendation.side;
  els.sharesInput.value = String(recommendation.shares);
  setTradeMessage(
    `Applied AI recommendation: ${recommendation.side.toUpperCase()} ${recommendation.shares} ${symbol}.`,
    recommendation.side === "buy" ? "up" : "down"
  );
}

function renderCandleChart() {
  const mode = getChartMode();
  const symbol = state.selectedSymbol || "--";
  els.chartTitle.textContent = `Candles: ${symbol} (${mode.label})`;
  els.chartStatus.textContent = candleSourceLabel;
  syncChartScaleButtons();
  drawCandlesSvg(chartCandles);
}

function getChartMode() {
  return CHART_MODES[state.chartScale] || CHART_MODES[DEFAULT_CHART_SCALE];
}

function syncChartScaleButtons() {
  for (const btn of els.timeframeButtons) {
    const scale = String(btn.dataset.chartScale || "").toLowerCase();
    btn.classList.toggle("is-active", scale === state.chartScale);
  }
}

function transformCandlesForMode(candles, mode) {
  if (!Array.isArray(candles)) return [];

  if (mode.aggregate === "month") {
    return aggregateCandlesByMonth(candles).slice(-mode.displayLimit);
  }

  return candles.slice(-mode.displayLimit);
}

function aggregateCandlesByMonth(candles) {
  const out = [];
  let currentKey = "";
  let acc = null;

  for (const candle of candles) {
    const date = new Date(candle.t);
    const key = `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, "0")}`;

    if (key !== currentKey) {
      if (acc) out.push(acc);
      currentKey = key;
      acc = {
        t: new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1)).toISOString(),
        o: candle.o,
        h: candle.h,
        l: candle.l,
        c: candle.c,
      };
      continue;
    }

    acc.h = Math.max(acc.h, candle.h);
    acc.l = Math.min(acc.l, candle.l);
    acc.c = candle.c;
  }

  if (acc) out.push(acc);
  return out;
}

function drawCandlesSvg(candles) {
  const width = 960;
  const height = 320;
  const leftPad = 58;
  const rightPad = 18;
  const topPad = 14;
  const bottomPad = 32;
  const plotWidth = width - leftPad - rightPad;
  const plotHeight = height - topPad - bottomPad;

  if (!Array.isArray(candles) || candles.length === 0) {
    els.candleSvg.innerHTML = `
      <rect x="0" y="0" width="${width}" height="${height}" fill="transparent"></rect>
      <text x="${width / 2}" y="${height / 2}" text-anchor="middle" fill="#62717b" font-size="14">
        No candle data available for this symbol yet.
      </text>
    `;
    return;
  }

  let minPrice = Number.POSITIVE_INFINITY;
  let maxPrice = Number.NEGATIVE_INFINITY;
  for (const candle of candles) {
    minPrice = Math.min(minPrice, candle.l);
    maxPrice = Math.max(maxPrice, candle.h);
  }

  if (!Number.isFinite(minPrice) || !Number.isFinite(maxPrice)) {
    els.candleSvg.innerHTML = "";
    return;
  }

  if (maxPrice === minPrice) {
    maxPrice += 1;
    minPrice -= 1;
  }

  const padding = (maxPrice - minPrice) * 0.07;
  const yMin = minPrice - padding;
  const yMax = maxPrice + padding;
  const yRange = yMax - yMin;
  const toY = (price) => topPad + ((yMax - price) / yRange) * plotHeight;

  const step = plotWidth / candles.length;
  const candleBodyWidth = Math.max(4, Math.min(14, step * 0.62));
  const gridRows = 4;

  let markup = `<rect x="0" y="0" width="${width}" height="${height}" fill="transparent"></rect>`;

  for (let i = 0; i <= gridRows; i += 1) {
    const ratio = i / gridRows;
    const y = topPad + ratio * plotHeight;
    const gridPrice = yMax - ratio * yRange;
    markup += `
      <line x1="${leftPad}" y1="${y.toFixed(2)}" x2="${width - rightPad}" y2="${y.toFixed(
      2
    )}" stroke="rgba(18, 35, 47, 0.14)" stroke-width="1"></line>
      <text x="6" y="${(y + 4).toFixed(2)}" fill="#5f707b" font-size="12">${gridPrice.toFixed(
      2
    )}</text>
    `;
  }

  candles.forEach((candle, index) => {
    const x = leftPad + step * index + step * 0.5;
    const yOpen = toY(candle.o);
    const yClose = toY(candle.c);
    const yHigh = toY(candle.h);
    const yLow = toY(candle.l);
    const isUp = candle.c >= candle.o;
    const color = isUp ? "#0c8a4c" : "#bf4e2f";
    const bodyTop = Math.min(yOpen, yClose);
    const bodyHeight = Math.max(1, Math.abs(yClose - yOpen));
    const bodyX = x - candleBodyWidth / 2;

    markup += `
      <line x1="${x.toFixed(2)}" y1="${yHigh.toFixed(2)}" x2="${x.toFixed(2)}" y2="${yLow.toFixed(
      2
    )}" stroke="${color}" stroke-width="1.4"></line>
      <rect x="${bodyX.toFixed(2)}" y="${bodyTop.toFixed(2)}" width="${candleBodyWidth.toFixed(
      2
    )}" height="${bodyHeight.toFixed(2)}" fill="${color}" opacity="0.9"></rect>
    `;
  });

  const first = candles[0];
  const last = candles[candles.length - 1];
  const lastClose = last.c;

  markup += `
    <line x1="${leftPad}" y1="${(height - bottomPad).toFixed(2)}" x2="${(
    width - rightPad
  ).toFixed(2)}" y2="${(height - bottomPad).toFixed(2)}" stroke="rgba(18, 35, 47, 0.14)" stroke-width="1"></line>
    <text x="${leftPad}" y="${height - 8}" fill="#5f707b" font-size="12">${fmtChartTime(
    first.t
  )}</text>
    <text x="${width - rightPad}" y="${height - 8}" text-anchor="end" fill="#5f707b" font-size="12">${fmtChartTime(
    last.t
  )}</text>
    <text x="${width - rightPad}" y="16" text-anchor="end" fill="#1b2a33" font-size="12">Last: ${lastClose.toFixed(
    2
  )}</text>
  `;

  els.candleSvg.innerHTML = markup;
}

function renderPositions() {
  const symbols = Object.keys(state.positions);
  if (symbols.length === 0) {
    els.positionsBody.innerHTML = `
      <tr>
        <td colspan="5">No positions yet. Place a buy trade to get started.</td>
      </tr>
    `;
    return;
  }

  els.positionsBody.innerHTML = symbols.map((symbol) => {
    const pos = state.positions[symbol];
    const quote = state.prices[symbol];
    const marketValue = round2(pos.shares * quote.price);
    const unrealized = round2((quote.price - pos.avgCost) * pos.shares);
    const cls = unrealized >= 0 ? "up" : "down";

    return `
      <tr>
        <td class="ticker">${symbol}</td>
        <td>${pos.shares}</td>
        <td>${fmtMoney(pos.avgCost)}</td>
        <td>${fmtMoney(marketValue)}</td>
        <td class="${cls}">${signedMoney(unrealized)}</td>
      </tr>
    `;
  }).join("");
}

function renderActivity() {
  const recent = state.transactions.slice(0, 12);
  if (recent.length === 0) {
    els.activityList.innerHTML = "<li>No trades yet.</li>";
    return;
  }

  els.activityList.innerHTML = recent
    .map((tx) => {
      const tone = tx.side === "BUY" ? "up" : tx.realized >= 0 ? "up" : "down";
      const realizedPart =
        tx.side === "SELL" ? ` | Realized: ${signedMoney(tx.realized || 0)}` : "";
      return `
        <li class="${tone}">
          ${stampTime(tx.timestamp)} | ${tx.side} ${tx.shares} ${tx.symbol} @ ${fmtMoney(
            tx.price
          )} | Total: ${fmtMoney(tx.total)}${realizedPart}
        </li>
      `;
    })
    .join("");
}

function calcPortfolioValue() {
  return round2(
    Object.entries(state.positions).reduce((sum, [symbol, pos]) => {
      const quote = state.prices[symbol];
      return sum + pos.shares * quote.price;
    }, 0)
  );
}

function calcUnrealizedPnl() {
  return round2(
    Object.entries(state.positions).reduce((sum, [symbol, pos]) => {
      const quote = state.prices[symbol];
      return sum + (quote.price - pos.avgCost) * pos.shares;
    }, 0)
  );
}

function setTradeMessage(message, tone) {
  els.tradeMessage.textContent = message;
  els.tradeMessage.className = `trade-message ${tone}`;
}

function applyPnLClass(el, value) {
  el.classList.remove("up", "down");
  if (value > 0) el.classList.add("up");
  if (value < 0) el.classList.add("down");
}

function hydrateState() {
  const raw = window.localStorage.getItem(STORAGE_KEY);
  if (!raw) return createInitialState();

  try {
    const parsed = JSON.parse(raw);
    return normalizeState(parsed);
  } catch {
    return createInitialState();
  }
}

function normalizeState(input) {
  const fresh = createInitialState();
  const safe = input && typeof input === "object" ? input : {};

  const prices = {};
  for (const stock of STOCKS) {
    const incoming = safe.prices && safe.prices[stock.symbol];
    prices[stock.symbol] = {
      price:
        typeof incoming?.price === "number" && incoming.price > 0
          ? round2(incoming.price)
          : stock.basePrice,
      lastClose:
        typeof incoming?.lastClose === "number" && incoming.lastClose > 0
          ? round2(incoming.lastClose)
          : stock.basePrice,
    };
  }

  const positions = {};
  if (safe.positions && typeof safe.positions === "object") {
    for (const [symbol, pos] of Object.entries(safe.positions)) {
      if (!prices[symbol]) continue;
      const shares = Number(pos.shares);
      const avgCost = Number(pos.avgCost);
      if (Number.isFinite(shares) && shares > 0 && Number.isFinite(avgCost) && avgCost > 0) {
        positions[symbol] = {
          shares: Math.floor(shares),
          avgCost: round2(avgCost),
        };
      }
    }
  }

  return {
    cash:
      typeof safe.cash === "number" && Number.isFinite(safe.cash) && safe.cash >= 0
        ? round2(safe.cash)
        : fresh.cash,
    realizedPnl:
      typeof safe.realizedPnl === "number" && Number.isFinite(safe.realizedPnl)
        ? round2(safe.realizedPnl)
        : 0,
    selectedSymbol:
      typeof safe.selectedSymbol === "string" && prices[safe.selectedSymbol]
        ? safe.selectedSymbol
        : fresh.selectedSymbol,
    chartScale:
      typeof safe.chartScale === "string" && CHART_MODES[safe.chartScale]
        ? safe.chartScale
        : fresh.chartScale,
    prices,
    positions,
    transactions: Array.isArray(safe.transactions) ? safe.transactions.slice(0, 100) : [],
  };
}

function createInitialState() {
  const prices = {};
  for (const stock of STOCKS) {
    prices[stock.symbol] = {
      price: stock.basePrice,
      lastClose: stock.basePrice,
    };
  }

  return {
    cash: STARTING_CASH,
    realizedPnl: 0,
    selectedSymbol: STOCKS[0].symbol,
    chartScale: DEFAULT_CHART_SCALE,
    prices,
    positions: {},
    transactions: [],
  };
}

function persistState() {
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
}

function fmtMoney(value) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

function signedMoney(value) {
  return `${value >= 0 ? "+" : "-"}${fmtMoney(Math.abs(value))}`;
}

function signedPct(value) {
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function stampTime(iso) {
  return new Date(iso).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function fmtChartTime(iso) {
  return new Date(iso).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
  });
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function round2(value) {
  return Math.round((value + Number.EPSILON) * 100) / 100;
}
