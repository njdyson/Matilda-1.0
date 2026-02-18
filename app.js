const STORAGE_KEY = "matilda.wallet.v1";
const STARTING_CASH = 100000;
const PRICE_TICK_MS = 5000;
const CANDLE_REFRESH_MS = 30000;
const API_QUOTES_URL = "/api/quotes";
const API_CANDLES_URL = "/api/candles";
const API_SYMBOL_SEARCH_URL = "/api/symbol-search";
const API_COMPANY_URL = "/api/company";
const API_AI_RESEARCH_URL = "/api/ai/research";
const API_AI_AUTOBOT_URL = "/api/ai/autobot";
const API_AUTOBOT_RUNTIME_STATE_URL = "/api/autobot/runtime/state";
const API_AUTOBOT_RUNTIME_SYNC_URL = "/api/autobot/runtime/sync";
const API_AUTOBOT_RUNTIME_RUN_URL = "/api/autobot/runtime/run";
const API_ACCOUNT_SNAPSHOT_URL = "/api/account/snapshot";
const API_TRADES_URL = "/api/trades";
const DEFAULT_CHART_SCALE = "mins";
const MAX_TRACKED_SYMBOLS = 20;
const SEARCH_DEBOUNCE_MS = 250;
const BACKEND_SYNC_INTERVAL_MS = 20000;
const BACKEND_SYNC_MIN_GAP_MS = 4000;
const BROKER_SNAPSHOT_REFRESH_MS = 3 * 60 * 1000;
const MARKET_PAGE_SIZE = 10;
const COMPANY_INFO_RETRY_MS = 15000;
const COMPANY_INFO_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const AI_AUTO_REFRESH_CHECK_MS = 30000;
const AI_AUTO_REFRESH_RETRY_MS = 5 * 60 * 1000;
const AI_RESEARCH_TTL_MS_BY_HORIZON = {
  short: 2 * 60 * 60 * 1000,
  swing: 8 * 60 * 60 * 1000,
  long: 24 * 60 * 60 * 1000,
};
const MAX_AI_CACHE_ENTRIES = 300;
const MAX_COMPANY_CACHE_ENTRIES = 200;
const AI_PROVIDERS = ["openai"];
const AI_MODELS = ["gpt-4.1-mini", "gpt-4o-mini", "gpt-5-mini", "gpt-5-mini-2025-08-07"];
const AI_HORIZONS = ["short", "swing", "long"];
const DEFAULT_AI_SETTINGS = {
  provider: "openai",
  model: "gpt-4.1-mini",
  horizon: "swing",
  aiResearchAutoRefreshEnabled: true,
  aiResearchAutoRefreshMins: 60,
  autobotIntervalMins: 30,
  maxTradesPerCycle: 3,
  addConfidenceMin: 80,
  addSentimentMin: 45,
  sellConfidenceMin: 60,
  sellSentimentMax: -5,
};
const DEFAULT_AUTOBOT_STATE = {
  enabled: false,
  lastRunAt: 0,
  latestThought: "",
  latestRecommendation: null,
  latestAutoAction: null,
  lastStatus: "Disabled",
  lastError: "",
};
const DEFAULT_MARKET_SCOUT_STATE = {
  enabled: true,
  intervalMins: 2,
  lastRunAt: 0,
  cursor: 0,
  lastSymbol: "",
  latestMessage: "Research ticker idle.",
  recentMessages: [],
  checkedAtBySymbol: {},
  createdAt: 0,
};
const CHART_MODES = {
  mins: {
    label: "Mins",
    timeframe: "1Min",
    fetchLimit: 120,
    displayLimit: 120,
    stepMs: 60 * 1000,
  },
  days: {
    label: "Days",
    timeframe: "1Day",
    fetchLimit: 90,
    displayLimit: 90,
    stepMs: 24 * 60 * 60 * 1000,
  },
};

const DEFAULT_STOCKS = [
  { symbol: "AAPL", basePrice: 192.35 },
  { symbol: "MSFT", basePrice: 428.12 },
  { symbol: "NVDA", basePrice: 118.9 },
  { symbol: "AMZN", basePrice: 177.64 },
  { symbol: "META", basePrice: 485.32 },
  { symbol: "GOOGL", basePrice: 165.47 },
  { symbol: "TSLA", basePrice: 193.24 },
  { symbol: "AMD", basePrice: 173.65 },
];
const DEFAULT_SYMBOLS = DEFAULT_STOCKS.map((stock) => stock.symbol);
const DEFAULT_PRICE_BY_SYMBOL = Object.fromEntries(
  DEFAULT_STOCKS.map((stock) => [stock.symbol, stock.basePrice])
);

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
  symbolSearchInput: document.getElementById("symbolSearchInput"),
  addSymbolBtn: document.getElementById("addSymbolBtn"),
  symbolSearchResults: document.getElementById("symbolSearchResults"),
  symbolSearchMessage: document.getElementById("symbolSearchMessage"),
  sideSelect: document.getElementById("sideSelect"),
  sharesInput: document.getElementById("sharesInput"),
  tradeQuoteSymbol: document.getElementById("tradeQuoteSymbol"),
  tradeQuoteValue: document.getElementById("tradeQuoteValue"),
  tradeQuoteChange: document.getElementById("tradeQuoteChange"),
  tradeQuoteNotional: document.getElementById("tradeQuoteNotional"),
  tradeForm: document.getElementById("tradeForm"),
  tradeMessage: document.getElementById("tradeMessage"),
  openTradeTicketBtn: document.getElementById("openTradeTicketBtn"),
  tradeTicketModal: document.getElementById("tradeTicketModal"),
  closeTradeTicketBtn: document.getElementById("closeTradeTicketBtn"),
  openSettingsBtn: document.getElementById("openSettingsBtn"),
  settingsModal: document.getElementById("settingsModal"),
  closeSettingsBtn: document.getElementById("closeSettingsBtn"),
  aiProviderSelect: document.getElementById("aiProviderSelect"),
  aiModelSelect: document.getElementById("aiModelSelect"),
  aiHorizonSelect: document.getElementById("aiHorizonSelect"),
  aiResearchAutoRefreshEnabledInput: document.getElementById("aiResearchAutoRefreshEnabledInput"),
  aiResearchAutoRefreshMinsInput: document.getElementById("aiResearchAutoRefreshMinsInput"),
  autobotIntervalMinsInput: document.getElementById("autobotIntervalMinsInput"),
  autobotMaxTradesPerCycleInput: document.getElementById("autobotMaxTradesPerCycleInput"),
  discoveryIntervalMinsInput: document.getElementById("discoveryIntervalMinsInput"),
  addConfidenceMinInput: document.getElementById("addConfidenceMinInput"),
  addSentimentMinInput: document.getElementById("addSentimentMinInput"),
  sellConfidenceMinInput: document.getElementById("sellConfidenceMinInput"),
  sellSentimentMaxInput: document.getElementById("sellSentimentMaxInput"),
  addConfidenceMinValue: document.getElementById("addConfidenceMinValue"),
  addSentimentMinValue: document.getElementById("addSentimentMinValue"),
  sellConfidenceMinValue: document.getElementById("sellConfidenceMinValue"),
  sellSentimentMaxValue: document.getElementById("sellSentimentMaxValue"),
  resetWalletBtn: document.getElementById("resetWalletBtn"),
  marketBody: document.getElementById("marketBody"),
  marketPrevPageBtn: document.getElementById("marketPrevPageBtn"),
  marketNextPageBtn: document.getElementById("marketNextPageBtn"),
  marketPageLabel: document.getElementById("marketPageLabel"),
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
  watchlistResearchTickerLine: document.getElementById("watchlistResearchTickerLine"),
  discoveryEnabledToggle: document.getElementById("discoveryEnabledToggle"),
  aiCompanyInfo: document.getElementById("aiCompanyInfo"),
  aiSentimentBadge: document.getElementById("aiSentimentBadge"),
  aiSentimentFill: document.getElementById("aiSentimentFill"),
  aiConfidenceValue: document.getElementById("aiConfidenceValue"),
  aiConfidenceFill: document.getElementById("aiConfidenceFill"),
  aiRecommendationText: document.getElementById("aiRecommendationText"),
  applyAiRecommendationBtn: document.getElementById("applyAiRecommendationBtn"),
  updateAiResearchBtn: document.getElementById("updateAiResearchBtn"),
  aiResearchArea: document.getElementById("aiResearchArea"),
  aiResearchSources: document.getElementById("aiResearchSources"),
  autobotEnabledToggle: document.getElementById("autobotEnabledToggle"),
  autobotStatus: document.getElementById("autobotStatus"),
  backendRuntimeStatus: document.getElementById("backendRuntimeStatus"),
  autobotRunNowBtn: document.getElementById("autobotRunNowBtn"),
  autobotThoughtArea: document.getElementById("autobotThoughtArea"),
  autobotRecommendationLine: document.getElementById("autobotRecommendationLine"),
  autobotMessage: document.getElementById("autobotMessage"),
};

let state = hydrateState();
let isRefreshingPrices = false;
let isRefreshingCandles = false;
let chartCandles = [];
let lastCandleRefreshAt = 0;
let marketSourceLabel = "Source: Loading...";
let hasLiveQuoteData = false;
let candleSourceLabel = "Select a symbol to load chart data";
let currentAiRecommendation = null;
let symbolSearchDebounceId = 0;
const companyInfoBySymbol = Object.create(null);
const companyInfoLoadStatusBySymbol = Object.create(null);
const companyInfoErrorBySymbol = Object.create(null);
const companyInfoLastAttemptAtBySymbol = Object.create(null);
const aiResearchLoadStatusByKey = Object.create(null);
const aiResearchErrorByKey = Object.create(null);
let aiResearchAutoRefreshTimerId = 0;
let isAiResearchAutoRefreshRunning = false;
let autobotTimerId = 0;
let isAutobotRunning = false;
let backendSyncTimerId = 0;
let brokerSnapshotTimerId = 0;
let backendSyncDirty = true;
let backendSyncInFlight = false;
let pendingBackendSync = false;
let lastBackendSyncAttemptAt = 0;
let backendRuntimeServerSideActive = false;
let backendRuntimeHeartbeatAt = 0;
let backendRuntimeNextRunAt = 0;
let brokerTradingAvailable = false;
let isSubmittingBrokerTrade = false;

initializeApp();

async function initializeApp() {
  ensureAiRefreshMetaForWatchlist();
  await refreshServerCapabilities();
  await hydrateStateFromBackendRuntime();
  populateSymbolSelect();
  bindEvents();
  hydrateCompanyRuntimeFromState();
  syncAiSettingsControls();
  if (!state.selectedSymbol || !state.prices[state.selectedSymbol]) {
    state.selectedSymbol = getTrackedSymbols()[0] || DEFAULT_SYMBOLS[0];
  }
  if (!CHART_MODES[state.chartScale]) {
    state.chartScale = DEFAULT_CHART_SCALE;
  }
  els.symbolSelect.value = state.selectedSymbol;
  renderAll();
  restartAiResearchAutoRefreshScheduler();
  restartAutobotScheduler();
  restartBackendSyncScheduler();
  restartBrokerSnapshotScheduler();
  if (brokerTradingAvailable) {
    await refreshBrokerAccountSnapshot({ silent: true });
  }
  await syncStateToBackendRuntime({ force: true });
  await refreshPrices();
  await refreshCandlesForSelectedSymbol(true);
  window.setInterval(() => {
    void refreshPrices();
  }, PRICE_TICK_MS);
}

function populateSymbolSelect() {
  const symbols = getTrackedSymbols();
  els.symbolSelect.innerHTML = symbols.map(
    (symbol) => `<option value="${symbol}">${symbol}</option>`
  ).join("");
}

function bindEvents() {
  els.tradeForm.addEventListener("submit", onTradeSubmit);
  els.openTradeTicketBtn.addEventListener("click", openTradeTicketModal);
  els.closeTradeTicketBtn.addEventListener("click", closeTradeTicketModal);
  els.tradeTicketModal.addEventListener("click", onTradeTicketModalBackdropClick);
  els.openSettingsBtn.addEventListener("click", openSettingsModal);
  els.closeSettingsBtn.addEventListener("click", closeSettingsModal);
  els.settingsModal.addEventListener("click", onSettingsModalBackdropClick);
  document.addEventListener("keydown", onDocumentKeydown);
  els.resetWalletBtn.addEventListener("click", resetWallet);
  els.aiProviderSelect.addEventListener("change", onAiSettingsChange);
  els.aiModelSelect.addEventListener("change", onAiSettingsChange);
  els.aiHorizonSelect.addEventListener("change", onAiSettingsChange);
  els.aiResearchAutoRefreshEnabledInput.addEventListener("change", onAiSettingsChange);
  els.aiResearchAutoRefreshMinsInput.addEventListener("input", onAiSettingsChange);
  els.autobotIntervalMinsInput.addEventListener("input", onAiSettingsChange);
  els.autobotMaxTradesPerCycleInput.addEventListener("input", onAiSettingsChange);
  els.discoveryIntervalMinsInput.addEventListener("input", onAiSettingsChange);
  els.addConfidenceMinInput.addEventListener("input", onAiSettingsChange);
  els.addSentimentMinInput.addEventListener("input", onAiSettingsChange);
  els.sellConfidenceMinInput.addEventListener("input", onAiSettingsChange);
  els.sellSentimentMaxInput.addEventListener("input", onAiSettingsChange);
  els.autobotEnabledToggle.addEventListener("change", onAutobotEnabledToggleChange);
  els.discoveryEnabledToggle.addEventListener("change", onDiscoveryEnabledToggleChange);
  els.autobotRunNowBtn.addEventListener("click", onAutobotRunNowClick);
  els.symbolSelect.addEventListener("change", onSymbolChangeFromTradeForm);
  els.sideSelect.addEventListener("change", renderTradeQuoteBox);
  els.sharesInput.addEventListener("input", renderTradeQuoteBox);
  els.marketBody.addEventListener("click", onMarketTableClick);
  els.positionsBody.addEventListener("click", onPositionsTableClick);
  els.applyAiRecommendationBtn.addEventListener("click", applyAiRecommendationToTicket);
  els.updateAiResearchBtn.addEventListener("click", onUpdateAiResearchClick);
  els.addSymbolBtn.addEventListener("click", onAddSymbolClick);
  els.symbolSearchInput.addEventListener("input", onSymbolSearchInput);
  els.symbolSearchInput.addEventListener("keydown", onSymbolSearchInputKeydown);
  els.symbolSearchResults.addEventListener("click", onSymbolSearchResultClick);
  els.marketPrevPageBtn.addEventListener("click", onMarketPrevPageClick);
  els.marketNextPageBtn.addEventListener("click", onMarketNextPageClick);
  document.addEventListener("visibilitychange", onDocumentVisibilityChange);
  window.addEventListener("beforeunload", onWindowBeforeUnload);
  for (const btn of els.timeframeButtons) {
    btn.addEventListener("click", onChartScaleButtonClick);
  }
}

function openTradeTicketModal() {
  els.tradeTicketModal.classList.add("is-open");
  els.tradeTicketModal.setAttribute("aria-hidden", "false");
  updateBodyModalClass();
}

function closeTradeTicketModal() {
  els.tradeTicketModal.classList.remove("is-open");
  els.tradeTicketModal.setAttribute("aria-hidden", "true");
  updateBodyModalClass();
}

function openSettingsModal() {
  els.settingsModal.classList.add("is-open");
  els.settingsModal.setAttribute("aria-hidden", "false");
  updateBodyModalClass();
}

function closeSettingsModal() {
  els.settingsModal.classList.remove("is-open");
  els.settingsModal.setAttribute("aria-hidden", "true");
  updateBodyModalClass();
}

function updateBodyModalClass() {
  const hasOpenModal =
    els.tradeTicketModal.classList.contains("is-open") ||
    els.settingsModal.classList.contains("is-open");
  document.body.classList.toggle("modal-open", hasOpenModal);
}

function onTradeTicketModalBackdropClick(event) {
  if (event.target !== els.tradeTicketModal) return;
  closeTradeTicketModal();
}

function onSettingsModalBackdropClick(event) {
  if (event.target !== els.settingsModal) return;
  closeSettingsModal();
}

function onDocumentKeydown(event) {
  if (event.key !== "Escape") return;
  if (els.tradeTicketModal.classList.contains("is-open")) {
    closeTradeTicketModal();
    return;
  }
  if (els.settingsModal.classList.contains("is-open")) {
    closeSettingsModal();
  }
}

function onDocumentVisibilityChange() {
  if (document.visibilityState === "hidden") {
    void syncStateToBackendRuntime({ force: false });
    return;
  }
  if (brokerTradingAvailable) {
    void refreshBrokerAccountSnapshot({ silent: true });
  }
}

function onWindowBeforeUnload() {
  try {
    if (typeof navigator.sendBeacon !== "function") return;
    const payload = backendSyncDirty
      ? buildBackendRuntimeSyncPayload()
      : {
          clientHeartbeatAt: Date.now(),
          clientRuntimeUpdatedAt: Number(state.backendRuntimeUpdatedAt || 0),
        };
    navigator.sendBeacon(
      API_AUTOBOT_RUNTIME_SYNC_URL,
      new Blob([JSON.stringify(payload)], { type: "application/json" })
    );
  } catch {
    // Ignore unload sync failures.
  }
}

function syncAiSettingsControls() {
  if (
    !els.aiProviderSelect ||
    !els.aiModelSelect ||
    !els.aiHorizonSelect ||
    !els.aiResearchAutoRefreshEnabledInput ||
    !els.aiResearchAutoRefreshMinsInput ||
    !els.autobotIntervalMinsInput ||
    !els.autobotMaxTradesPerCycleInput ||
    !els.discoveryIntervalMinsInput ||
    !els.addConfidenceMinInput ||
    !els.addSentimentMinInput ||
    !els.sellConfidenceMinInput ||
    !els.sellSentimentMaxInput
  ) {
    return;
  }
  const settings = getAiSettings();
  els.aiProviderSelect.value = settings.provider;
  els.aiModelSelect.value = settings.model;
  els.aiHorizonSelect.value = settings.horizon;
  els.aiResearchAutoRefreshEnabledInput.checked = Boolean(settings.aiResearchAutoRefreshEnabled);
  els.aiResearchAutoRefreshMinsInput.value = String(settings.aiResearchAutoRefreshMins);
  els.autobotIntervalMinsInput.value = String(settings.autobotIntervalMins);
  els.autobotMaxTradesPerCycleInput.value = String(settings.maxTradesPerCycle);
  const scout = normalizeMarketScoutState(state.marketScout);
  state.marketScout = scout;
  els.discoveryIntervalMinsInput.value = String(scout.intervalMins);
  els.addConfidenceMinInput.value = String(settings.addConfidenceMin);
  els.addSentimentMinInput.value = String(settings.addSentimentMin);
  els.sellConfidenceMinInput.value = String(settings.sellConfidenceMin);
  els.sellSentimentMaxInput.value = String(settings.sellSentimentMax);
  renderAiThresholdValues(settings);
}

function onAiSettingsChange() {
  const provider = normalizeAiProvider(els.aiProviderSelect.value);
  const model = normalizeAiModel(els.aiModelSelect.value);
  const horizon = normalizeAiHorizon(els.aiHorizonSelect.value);
  const aiResearchAutoRefreshEnabled = Boolean(els.aiResearchAutoRefreshEnabledInput.checked);
  const aiResearchAutoRefreshMins = normalizeThreshold(
    els.aiResearchAutoRefreshMinsInput.value,
    15,
    720,
    DEFAULT_AI_SETTINGS.aiResearchAutoRefreshMins
  );
  const autobotIntervalMins = normalizeThreshold(
    els.autobotIntervalMinsInput.value,
    5,
    240,
    DEFAULT_AI_SETTINGS.autobotIntervalMins
  );
  const maxTradesPerCycle = normalizeThreshold(
    els.autobotMaxTradesPerCycleInput.value,
    1,
    5,
    DEFAULT_AI_SETTINGS.maxTradesPerCycle
  );
  const discoveryIntervalMins = normalizeThreshold(
    els.discoveryIntervalMinsInput.value,
    1,
    120,
    DEFAULT_MARKET_SCOUT_STATE.intervalMins
  );
  const addConfidenceMin = normalizeThreshold(
    els.addConfidenceMinInput.value,
    50,
    95,
    DEFAULT_AI_SETTINGS.addConfidenceMin
  );
  const addSentimentMin = normalizeThreshold(
    els.addSentimentMinInput.value,
    0,
    90,
    DEFAULT_AI_SETTINGS.addSentimentMin
  );
  const sellConfidenceMin = normalizeThreshold(
    els.sellConfidenceMinInput.value,
    50,
    95,
    DEFAULT_AI_SETTINGS.sellConfidenceMin
  );
  const sellSentimentMax = normalizeThreshold(
    els.sellSentimentMaxInput.value,
    -60,
    20,
    DEFAULT_AI_SETTINGS.sellSentimentMax
  );
  state.aiSettings = {
    provider,
    model,
    horizon,
    aiResearchAutoRefreshEnabled,
    aiResearchAutoRefreshMins,
    autobotIntervalMins,
    maxTradesPerCycle,
    addConfidenceMin,
    addSentimentMin,
    sellConfidenceMin,
    sellSentimentMax,
  };
  state.marketScout = normalizeMarketScoutState({
    ...state.marketScout,
    intervalMins: discoveryIntervalMins,
  });
  markBackendSyncDirty();
  renderAiThresholdValues(state.aiSettings);
  restartAiResearchAutoRefreshScheduler();
  restartAutobotScheduler();
  persistAndRender();
  void syncStateToBackendRuntime({ force: true });
}

function renderAiThresholdValues(settings) {
  if (
    !els.addConfidenceMinValue ||
    !els.addSentimentMinValue ||
    !els.sellConfidenceMinValue ||
    !els.sellSentimentMaxValue
  ) {
    return;
  }
  const safe = normalizeAiSettings(settings);
  els.addConfidenceMinValue.textContent = `${safe.addConfidenceMin}%`;
  els.addSentimentMinValue.textContent = String(safe.addSentimentMin);
  els.sellConfidenceMinValue.textContent = `${safe.sellConfidenceMin}%`;
  els.sellSentimentMaxValue.textContent = String(safe.sellSentimentMax);
}

function onAutobotEnabledToggleChange() {
  const enabled = Boolean(els.autobotEnabledToggle.checked);
  state.autobot = normalizeAutobotState({
    ...state.autobot,
    enabled,
    lastStatus: enabled ? "Enabled" : "Disabled",
    lastError: "",
  });
  markBackendSyncDirty();
  setAutobotMessage(enabled ? "Auto Trade enabled (backend runtime)." : "Auto Trade disabled.", "up");
  persistAndRender();
  restartAutobotScheduler();
  void syncStateToBackendRuntime({ force: true });
}

function onDiscoveryEnabledToggleChange() {
  const enabled = Boolean(els.discoveryEnabledToggle.checked);
  state.marketScout = normalizeMarketScoutState({
    ...state.marketScout,
    enabled,
    latestMessage: enabled
      ? state.marketScout?.latestMessage || "Research ticker idle."
      : "Discovery tracker disabled.",
  });
  markBackendSyncDirty();
  persistAndRender();
  void syncStateToBackendRuntime({ force: true });
}

function onAutobotRunNowClick() {
  void runBackendAutobotCycleNow();
}

async function runBackendAutobotCycleNow() {
  if (isAutobotRunning) return;
  isAutobotRunning = true;
  state.autobot = normalizeAutobotState({
    ...state.autobot,
    lastStatus: "Manual cycle running...",
    lastError: "",
  });
  renderAutobotPanel();

  try {
    const response = await fetch(API_AUTOBOT_RUNTIME_RUN_URL, {
      method: "POST",
      headers: {
        Accept: "application/json",
      },
    });
    if (!response.ok) {
      let details = "";
      try {
        const body = await response.json();
        details = typeof body?.error === "string" ? body.error : "";
      } catch {
        details = "";
      }
      throw new Error(
        details
          ? `Auto Trade runtime ${response.status}: ${details}`
          : `Auto Trade runtime ${response.status}.`
      );
    }

    const payload = await response.json();
    const merged = applyBackendRuntimePayload(payload?.runtime || null);
    if (merged.stateMerged) {
      persistState();
      renderAll();
    } else {
      renderAutobotPanel();
    }

    const statusText = String(payload?.outcome?.status || "").trim();
    if (statusText) {
      setAutobotMessage(statusText, "up");
    }
  } catch (error) {
    const message = getErrorMessage(error);
    state.autobot = normalizeAutobotState({
      ...state.autobot,
      lastError: message,
      lastStatus: "Error",
    });
    setAutobotMessage(message, "down");
    persistAndRender();
  } finally {
    isAutobotRunning = false;
    renderAutobotPanel();
  }
}

function restartAiResearchAutoRefreshScheduler() {
  if (aiResearchAutoRefreshTimerId) {
    window.clearInterval(aiResearchAutoRefreshTimerId);
    aiResearchAutoRefreshTimerId = 0;
  }

  const settings = getAiSettings();
  if (!settings.aiResearchAutoRefreshEnabled) return;

  aiResearchAutoRefreshTimerId = window.setInterval(() => {
    void runAiResearchAutoRefreshTick();
  }, AI_AUTO_REFRESH_CHECK_MS);
}

async function runAiResearchAutoRefreshTick() {
  if (isAiResearchAutoRefreshRunning) return;
  if (!hasLiveQuoteData) return;
  const settings = getAiSettings();
  if (!settings.aiResearchAutoRefreshEnabled) return;

  const symbol = getNextDueAiResearchSymbol(settings);
  if (!symbol) return;

  isAiResearchAutoRefreshRunning = true;
  try {
    await refreshAiResearchForSymbol(symbol, { source: "auto" });
  } finally {
    isAiResearchAutoRefreshRunning = false;
  }
}

function getNextDueAiResearchSymbol(settings) {
  const symbols = getTrackedSymbols();
  if (!symbols.length) return "";

  let selectedSymbol = "";
  let earliestDueAt = Number.POSITIVE_INFINITY;
  const now = Date.now();
  for (const symbol of symbols) {
    const dueAt = getAiResearchNextDueAt(symbol, settings);
    if (!Number.isFinite(dueAt) || dueAt > now) continue;

    const meta = ensureAiRefreshMetaForSymbol(symbol);
    if (meta.lastAttemptAt > 0 && now - meta.lastAttemptAt < AI_AUTO_REFRESH_RETRY_MS) continue;

    const aiCacheKey = buildAiCacheKey(symbol, settings);
    if (aiResearchLoadStatusByKey[aiCacheKey] === "loading") continue;

    if (dueAt < earliestDueAt) {
      earliestDueAt = dueAt;
      selectedSymbol = symbol;
    }
  }
  return selectedSymbol;
}

function restartAutobotScheduler() {
  if (autobotTimerId) {
    window.clearInterval(autobotTimerId);
    autobotTimerId = 0;
  }
  // Backend runtime is the single executor for scheduled Autobot trades.
}

function restartBackendSyncScheduler() {
  if (backendSyncTimerId) {
    window.clearInterval(backendSyncTimerId);
    backendSyncTimerId = 0;
  }
  backendSyncTimerId = window.setInterval(() => {
    void syncStateToBackendRuntime({ force: false });
  }, BACKEND_SYNC_INTERVAL_MS);
}

async function syncStateToBackendRuntime(options = {}) {
  const force = Boolean(options?.force);
  const now = Date.now();
  if (!force && now - lastBackendSyncAttemptAt < BACKEND_SYNC_MIN_GAP_MS) return false;
  if (backendSyncInFlight) {
    pendingBackendSync = true;
    return false;
  }

  lastBackendSyncAttemptAt = now;
  backendSyncInFlight = true;
  try {
    const includeFullState = force || backendSyncDirty;
    const payload = includeFullState
      ? buildBackendRuntimeSyncPayload()
      : {
          clientHeartbeatAt: Date.now(),
          clientRuntimeUpdatedAt: Number(state.backendRuntimeUpdatedAt || 0),
        };
    const response = await fetch(API_AUTOBOT_RUNTIME_SYNC_URL, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      return false;
    }
    const body = await response.json();
    const runtime = body?.runtime;
    const merged = applyBackendRuntimePayload(runtime);
    if (includeFullState) {
      backendSyncDirty = false;
    }
    if (merged.stateMerged) {
      persistState();
      renderAll();
    } else if (merged.metaChanged) {
      renderAutobotPanel();
    }
    return true;
  } catch {
    return false;
  } finally {
    backendSyncInFlight = false;
    if (pendingBackendSync) {
      pendingBackendSync = false;
      void syncStateToBackendRuntime({ force: true });
    }
  }
}

function buildBackendRuntimeSyncPayload() {
  const settings = getAiSettings();
  const autobot = normalizeAutobotState(state.autobot);
  const marketScout = normalizeMarketScoutState(state.marketScout);
  const watchlist = Array.isArray(state.watchlist)
    ? state.watchlist.map((symbol) => normalizeSymbolInput(symbol)).filter(Boolean)
    : [];
  const prices = {};
  for (const symbol of watchlist) {
    const quote = state.prices[symbol];
    if (!quote) continue;
    prices[symbol] = {
      price: round2(Number(quote.price) || 0),
      lastClose: round2(Number(quote.lastClose) || Number(quote.price) || 0),
    };
  }

  return {
    clientHeartbeatAt: Date.now(),
    clientRuntimeUpdatedAt: Number(state.backendRuntimeUpdatedAt || 0),
    aiSettings: {
      provider: settings.provider,
      model: settings.model,
      horizon: settings.horizon,
      autobotIntervalMins: settings.autobotIntervalMins,
      maxTradesPerCycle: settings.maxTradesPerCycle,
      aiResearchAutoRefreshEnabled: settings.aiResearchAutoRefreshEnabled,
      aiResearchAutoRefreshMins: settings.aiResearchAutoRefreshMins,
      addConfidenceMin: settings.addConfidenceMin,
      addSentimentMin: settings.addSentimentMin,
      sellConfidenceMin: settings.sellConfidenceMin,
      sellSentimentMax: settings.sellSentimentMax,
    },
    wallet: {
      cash: round2(state.cash),
      realizedPnl: round2(state.realizedPnl),
      selectedSymbol: state.selectedSymbol,
      watchlist,
      lockedSymbols: normalizeLockedWatchlistSymbols(state.lockedWatchlistSymbols, watchlist),
      prices,
      positions: state.positions,
      transactions: state.transactions.slice(0, 100),
    },
    aiResearchCache: normalizeAiResearchCache(state.aiResearchCache),
    aiRefreshMeta: normalizeAiRefreshMeta(state.aiRefreshMeta, watchlist),
    marketScout: {
      enabled: marketScout.enabled,
      intervalMins: marketScout.intervalMins,
    },
    autobot: {
      enabled: autobot.enabled,
    },
  };
}

async function hydrateStateFromBackendRuntime() {
  try {
    const response = await fetch(API_AUTOBOT_RUNTIME_STATE_URL, {
      headers: {
        Accept: "application/json",
      },
    });
    if (!response.ok) return false;
    const payload = await response.json();
    const runtime = payload?.runtime;
    const merged = applyBackendRuntimePayload(runtime);
    if (merged.stateMerged) {
      backendSyncDirty = false;
    }
    return merged.stateMerged;
  } catch {
    return false;
  }
}

async function refreshServerCapabilities() {
  try {
    const response = await fetch("/api/health", {
      headers: {
        Accept: "application/json",
      },
    });
    if (!response.ok) {
      brokerTradingAvailable = false;
      return false;
    }
    const payload = await response.json();
    brokerTradingAvailable = Boolean(payload?.alpacaConfigured);
    return true;
  } catch {
    brokerTradingAvailable = false;
    return false;
  }
}

function restartBrokerSnapshotScheduler() {
  if (brokerSnapshotTimerId) {
    window.clearInterval(brokerSnapshotTimerId);
    brokerSnapshotTimerId = 0;
  }
  if (!brokerTradingAvailable) return;
  brokerSnapshotTimerId = window.setInterval(() => {
    void refreshBrokerAccountSnapshot({ silent: true });
  }, BROKER_SNAPSHOT_REFRESH_MS);
}

async function refreshBrokerAccountSnapshot(options = {}) {
  if (!brokerTradingAvailable) return false;
  if (isSubmittingBrokerTrade) return false;
  try {
    const response = await fetch(API_ACCOUNT_SNAPSHOT_URL, {
      headers: {
        Accept: "application/json",
      },
    });
    if (!response.ok) {
      if (!options?.silent) {
        setTradeMessage("Broker account sync unavailable.", "down");
      }
      return false;
    }
    const payload = await response.json();
    const snapshot = payload?.snapshot;
    if (!snapshot || typeof snapshot !== "object") {
      if (!options?.silent) {
        setTradeMessage("Broker account sync returned invalid data.", "down");
      }
      return false;
    }
    const changed = applyBrokerSnapshotToState(snapshot);
    if (changed) {
      persistAndRender();
    } else if (!options?.silent) {
      renderAll();
    }
    return true;
  } catch {
    if (!options?.silent) {
      setTradeMessage("Broker account sync failed.", "down");
    }
    return false;
  }
}

function applyBrokerSnapshotToState(snapshot) {
  const beforeSignature = getBrokerSyncSignature();
  const safe = snapshot && typeof snapshot === "object" ? snapshot : {};
  const account = safe.account && typeof safe.account === "object" ? safe.account : {};
  const rows = Array.isArray(safe.positions) ? safe.positions : [];
  const pendingOrders = normalizePendingOrders(safe.openOrders);
  const currentWatchlist = Array.isArray(state.watchlist)
    ? state.watchlist.map((symbol) => normalizeSymbolInput(symbol)).filter(Boolean)
    : [];
  const positionSymbols = rows
    .map((row) => normalizeSymbolInput(row?.symbol))
    .filter(Boolean);
  const watchlist = Array.from(new Set([...currentWatchlist, ...positionSymbols])).slice(0, MAX_TRACKED_SYMBOLS);
  if (watchlist.length === 0) {
    watchlist.push(...DEFAULT_SYMBOLS.slice(0, MAX_TRACKED_SYMBOLS));
  }

  const prices = { ...state.prices };
  const positions = {};
  for (const symbol of watchlist) {
    const existing = prices[symbol];
    if (!existing || typeof existing !== "object") {
      const base = getBasePrice(symbol);
      prices[symbol] = {
        price: base,
        lastClose: base,
      };
    }
  }

  for (const row of rows) {
    const symbol = normalizeSymbolInput(row?.symbol);
    if (!symbol) continue;
    const qtyNum = Number(row?.qty);
    const shares = Number.isFinite(qtyNum) && qtyNum > 0 ? Math.floor(qtyNum) : 0;
    if (shares <= 0) continue;
    const avgEntryPriceNum = Number(row?.avgEntryPrice);
    const currentPriceNum = Number(row?.currentPrice);
    const avgCost =
      Number.isFinite(avgEntryPriceNum) && avgEntryPriceNum > 0
        ? round2(avgEntryPriceNum)
        : Number.isFinite(currentPriceNum) && currentPriceNum > 0
          ? round2(currentPriceNum)
          : prices[symbol]?.price || getBasePrice(symbol);
    positions[symbol] = {
      shares,
      avgCost,
    };
    const currentPrice =
      Number.isFinite(currentPriceNum) && currentPriceNum > 0
        ? round2(currentPriceNum)
        : prices[symbol]?.price || getBasePrice(symbol);
    prices[symbol] = {
      price: currentPrice,
      lastClose:
        Number.isFinite(prices[symbol]?.lastClose) && prices[symbol].lastClose > 0
          ? round2(prices[symbol].lastClose)
          : currentPrice,
    };
  }

  const cashNum = Number(account?.cash);
  if (Number.isFinite(cashNum) && cashNum >= 0) {
    state.cash = round2(cashNum);
  }
  state.watchlist = watchlist;
  state.lockedWatchlistSymbols = normalizeLockedWatchlistSymbols(
    state.lockedWatchlistSymbols,
    watchlist
  );
  state.prices = prices;
  state.positions = positions;
  state.pendingOrders = pendingOrders;
  ensureAiRefreshMetaForWatchlist();
  if (!state.selectedSymbol || !state.watchlist.includes(state.selectedSymbol)) {
    state.selectedSymbol = state.watchlist[0] || DEFAULT_SYMBOLS[0];
  }
  const maxPage = Math.max(0, Math.ceil(state.watchlist.length / MARKET_PAGE_SIZE) - 1);
  state.marketPage = Math.min(Math.max(0, Number(state.marketPage || 0)), maxPage);
  populateSymbolSelect();
  els.symbolSelect.value = state.selectedSymbol;
  renderTradeQuoteBox();
  return beforeSignature !== getBrokerSyncSignature();
}

function getBrokerSyncSignature() {
  const positions = Object.entries(state.positions || {})
    .map(([symbol, pos]) => {
      const cleanSymbol = normalizeSymbolInput(symbol);
      const shares = Math.max(0, Math.floor(Number(pos?.shares) || 0));
      const avgCost = round2(Number(pos?.avgCost) || 0);
      return cleanSymbol ? `${cleanSymbol}:${shares}:${avgCost}` : "";
    })
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
  const watchlist = Array.isArray(state.watchlist)
    ? state.watchlist.map((symbol) => normalizeSymbolInput(symbol)).filter(Boolean).sort()
    : [];
  const pendingOrders = normalizePendingOrders(state.pendingOrders)
    .map((row) => `${row.side}:${row.symbol}:${row.qty}:${row.filledQty}:${row.status}`)
    .sort((a, b) => a.localeCompare(b));
  const lockedSymbols = normalizeLockedWatchlistSymbols(state.lockedWatchlistSymbols, state.watchlist).sort(
    (a, b) => a.localeCompare(b)
  );
  return JSON.stringify({
    cash: round2(Number(state.cash) || 0),
    selectedSymbol: normalizeSymbolInput(state.selectedSymbol),
    watchlist,
    lockedSymbols,
    positions,
    pendingOrders,
  });
}

function applyBackendRuntimePayload(runtime) {
  const safe = runtime && typeof runtime === "object" ? runtime : null;
  if (!safe) {
    return { stateMerged: false, metaChanged: false };
  }

  let metaChanged = false;
  const heartbeatAt = Number(safe.lastClientHeartbeatAt);
  const nextRunAt = Number(safe.nextRunAt);
  const active = Boolean(safe.serverSideAutobotActive);
  const normalizedHeartbeatAt =
    Number.isFinite(heartbeatAt) && heartbeatAt > 0 ? Math.floor(heartbeatAt) : 0;
  const normalizedNextRunAt =
    Number.isFinite(nextRunAt) && nextRunAt > 0 ? Math.floor(nextRunAt) : 0;
  if (backendRuntimeHeartbeatAt !== normalizedHeartbeatAt) {
    backendRuntimeHeartbeatAt = normalizedHeartbeatAt;
    metaChanged = true;
  }
  if (backendRuntimeNextRunAt !== normalizedNextRunAt) {
    backendRuntimeNextRunAt = normalizedNextRunAt;
    metaChanged = true;
  }
  if (backendRuntimeServerSideActive !== active) {
    backendRuntimeServerSideActive = active;
    metaChanged = true;
  }

  const runtimeUpdatedAt = Number(safe.updatedAt);
  if (!Number.isFinite(runtimeUpdatedAt) || runtimeUpdatedAt <= Number(state.backendRuntimeUpdatedAt || 0)) {
    return { stateMerged: false, metaChanged };
  }

  const runtimeWatchlist = Array.isArray(safe.wallet?.watchlist)
    ? safe.wallet.watchlist.map((symbol) => normalizeSymbolInput(symbol)).filter(Boolean)
    : [];
  const clientSelectedSymbol = normalizeSymbolInput(state.selectedSymbol);
  const runtimeSelectedSymbol = normalizeSymbolInput(safe.wallet?.selectedSymbol);
  const preferredSelectedSymbol =
    clientSelectedSymbol && runtimeWatchlist.includes(clientSelectedSymbol)
      ? clientSelectedSymbol
      : runtimeSelectedSymbol || clientSelectedSymbol;

  const merged = normalizeState({
    ...state,
    cash: safe.wallet?.cash,
    realizedPnl: safe.wallet?.realizedPnl,
    selectedSymbol: preferredSelectedSymbol || state.selectedSymbol,
    watchlist: safe.wallet?.watchlist,
    lockedWatchlistSymbols: safe.wallet?.lockedSymbols,
    prices: safe.wallet?.prices,
    positions: safe.wallet?.positions,
    transactions: safe.wallet?.transactions,
    aiSettings: safe.aiSettings || state.aiSettings,
    aiResearchCache: safe.aiResearchCache || state.aiResearchCache,
    aiRefreshMeta: safe.aiRefreshMeta || state.aiRefreshMeta,
    marketScout: safe.marketScout || state.marketScout,
    autobot: safe.autobot || state.autobot,
    backendRuntimeUpdatedAt: Math.floor(runtimeUpdatedAt),
  });
  state = merged;
  return { stateMerged: true, metaChanged: true };
}

async function runAutobotCycle({ autoExecute, source }) {
  if (isAutobotRunning) return;
  if (source !== "manual" && !normalizeAutobotState(state.autobot).enabled) return;

  isAutobotRunning = true;
  state.autobot = normalizeAutobotState({
    ...state.autobot,
    lastStatus: source === "manual" ? "Thinking..." : "Auto cycle running...",
    lastError: "",
  });
  renderAutobotPanel();

  try {
    const settings = getAiSettings();
    const context = buildAutobotContextPayload(settings);
    const recommendation = await fetchAutobotRecommendation(settings, context);
    const now = Date.now();
    let nextStatus = source === "manual" ? "Manual update complete" : "Auto update complete";
    let latestAutoAction = state.autobot?.latestAutoAction || null;
    state.autobot = normalizeAutobotState({
      ...state.autobot,
      lastRunAt: now,
      latestThought: recommendation.thought || recommendation.reason || "",
      latestRecommendation: recommendation,
      latestAutoAction,
      lastStatus: nextStatus,
      lastError: "",
    });

    if (autoExecute) {
      if (recommendation.side === "hold" || recommendation.shares <= 0) {
        latestAutoAction = {
          type: "HOLD",
          symbol: recommendation.symbol,
          shares: 0,
          reason: recommendation.reason || "No strong edge. Holding.",
          executed: false,
          at: now,
        };
        nextStatus = "Auto cycle: hold (no trade)";
      } else {
        await refreshPrices();
        const executed = executeRecommendationTrade(recommendation, "Autobot auto");
        latestAutoAction = {
          type: recommendation.side.toUpperCase(),
          symbol: recommendation.symbol,
          shares: recommendation.shares,
          reason: recommendation.reason || "Model-selected action.",
          executed,
          at: Date.now(),
        };
        if (executed) {
          nextStatus = `Auto executed ${recommendation.side.toUpperCase()} ${recommendation.shares} ${recommendation.symbol}`;
          setAutobotMessage(
            `Auto-executed ${recommendation.side.toUpperCase()} ${recommendation.shares} ${recommendation.symbol}.`,
            recommendation.side === "buy" ? "up" : "down"
          );
        } else {
          nextStatus = `Auto suggested ${recommendation.side.toUpperCase()} but execution failed`;
          setAutobotMessage(
            `Auto recommendation not executed: ${recommendation.side.toUpperCase()} ${recommendation.shares} ${recommendation.symbol}.`,
            "down"
          );
        }
      }
    }

    state.autobot = normalizeAutobotState({
      ...state.autobot,
      latestAutoAction,
      lastStatus: nextStatus,
      lastError: "",
    });
    persistState();
    renderAll();
  } catch (error) {
    const message = getErrorMessage(error);
    state.autobot = normalizeAutobotState({
      ...state.autobot,
      lastError: message,
      lastStatus: "Error",
    });
    setAutobotMessage(message, "down");
    persistAndRender();
  } finally {
    isAutobotRunning = false;
    renderAutobotPanel();
  }
}

function buildAutobotContextPayload(settings) {
  const symbols = Array.from(
    new Set(
      (Array.isArray(state.watchlist) ? state.watchlist : [])
        .map((symbol) => normalizeSymbolInput(symbol))
        .filter(Boolean)
    )
  );
  const fallbackSymbol = normalizeSymbolInput(state.selectedSymbol);
  if (symbols.length === 0 && fallbackSymbol) {
    symbols.push(fallbackSymbol);
  }
  const positions = Object.entries(state.positions)
    .map(([symbol, pos]) => {
      const quote = state.prices[symbol];
      const mark = quote?.price && quote.price > 0 ? quote.price : pos.avgCost;
      const invested = round2(pos.shares * pos.avgCost);
      const marketValue = round2(pos.shares * mark);
      const unrealized = round2((mark - pos.avgCost) * pos.shares);
      const unrealizedPct = invested > 0 ? round2((unrealized / invested) * 100) : 0;
      return {
        symbol,
        shares: pos.shares,
        avgCost: round2(pos.avgCost),
        invested,
        marketPrice: round2(mark),
        marketValue,
        unrealizedPnl: unrealized,
        unrealizedPct,
      };
    })
    .sort((a, b) => a.symbol.localeCompare(b.symbol));

  const snapshot = symbols.map((symbol) => {
    const quote = state.prices[symbol];
    const entry = getAiResearchEntry(symbol, settings);
    const profile = entry?.profile || null;
    const dayChange =
      quote && Number.isFinite(quote.price) && Number.isFinite(quote.lastClose)
        ? round2(quote.price - quote.lastClose)
        : null;
    const dayPct =
      dayChange !== null && quote && quote.lastClose > 0 ? round2((dayChange / quote.lastClose) * 100) : null;

    return {
      symbol,
      price: quote?.price || null,
      lastClose: quote?.lastClose || null,
      dayChange,
      dayPct,
      hasResearch: Boolean(profile),
      research: profile
        ? {
            sentiment: profile.sentiment,
            confidence: profile.confidence,
            action: profile.action,
            thesis: truncateText(profile.thesis, 240),
            catalyst: truncateText(profile.catalyst, 200),
            risk: truncateText(profile.risk, 200),
            brief: truncateText(profile.brief, 360),
          }
        : null,
    };
  });

  return {
    cash: round2(state.cash),
    realizedPnl: round2(state.realizedPnl),
    portfolioValue: calcPortfolioValue(),
    unrealizedPnl: calcUnrealizedPnl(),
    totalEquity: round2(state.cash + calcPortfolioValue()),
    horizon: settings.horizon,
    positions,
    snapshot,
  };
}

async function fetchAutobotRecommendation(settings, context) {
  const response = await fetch(API_AI_AUTOBOT_URL, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      provider: settings.provider,
      model: settings.model,
      horizon: settings.horizon,
      context,
    }),
  });

  if (!response.ok) {
    let details = "";
    try {
      const body = await response.json();
      details = typeof body?.error === "string" ? body.error : "";
    } catch {
      details = "";
    }
    throw new Error(details ? `Autobot API ${response.status}: ${details}` : `Autobot API ${response.status}.`);
  }

  const payload = await response.json();
  if (!payload || typeof payload !== "object" || !payload.recommendation) {
    throw new Error("Autobot API returned an unexpected payload.");
  }
  return normalizeAutobotRecommendation(payload.recommendation);
}

function getTrackedSymbols() {
  return Array.isArray(state.watchlist)
    ? state.watchlist.filter((symbol) => typeof symbol === "string" && state.prices[symbol])
    : [];
}

function getSortedTrackedSymbols() {
  const symbols = getTrackedSymbols();
  const settings = getAiSettings();
  return [...symbols].sort((a, b) => {
    const aConfidence = getResearchConfidenceForSymbol(a, settings);
    const bConfidence = getResearchConfidenceForSymbol(b, settings);
    const aHasConfidence = Number.isFinite(aConfidence);
    const bHasConfidence = Number.isFinite(bConfidence);
    if (aHasConfidence && bHasConfidence && bConfidence !== aConfidence) {
      return bConfidence - aConfidence;
    }
    if (aHasConfidence !== bHasConfidence) {
      return aHasConfidence ? -1 : 1;
    }
    return a.localeCompare(b);
  });
}

function getResearchConfidenceForSymbol(symbol, settings) {
  const entry = getAiResearchEntry(symbol, settings || getAiSettings());
  if (!entry?.profile) return null;
  return clamp(Math.round(entry.profile.confidence), 0, 100);
}

function normalizeSymbolInput(raw) {
  const symbol = String(raw || "")
    .trim()
    .toUpperCase();
  if (!symbol) return "";
  if (!/^[A-Z0-9.-]{1,12}$/.test(symbol)) return "";
  return symbol;
}

function getBasePrice(symbol) {
  return DEFAULT_PRICE_BY_SYMBOL[symbol] || 100;
}

function removeSymbolFromWatchlist(symbol) {
  if (!state.watchlist.includes(symbol)) return;
  if (normalizeLockedWatchlistSymbols(state.lockedWatchlistSymbols, state.watchlist).includes(symbol)) {
    setSymbolSearchMessage(`Unlock ${symbol} before removing it from watchlist.`, "down");
    return;
  }

  if (state.watchlist.length <= 1) {
    setSymbolSearchMessage("At least one symbol must remain in the snapshot.", "down");
    return;
  }

  if (state.positions[symbol]?.shares > 0) {
    setSymbolSearchMessage(`Sell ${symbol} position before removing it from snapshot.`, "down");
    return;
  }

  state.watchlist = state.watchlist.filter((item) => item !== symbol);
  state.lockedWatchlistSymbols = normalizeLockedWatchlistSymbols(
    state.lockedWatchlistSymbols.filter((item) => item !== symbol),
    state.watchlist
  );
  delete state.prices[symbol];
  if (state.aiRefreshMeta && typeof state.aiRefreshMeta === "object") {
    delete state.aiRefreshMeta[symbol];
  }

  if (state.selectedSymbol === symbol) {
    state.selectedSymbol = getTrackedSymbols()[0] || DEFAULT_SYMBOLS[0];
    chartCandles = [];
    candleSourceLabel = "Select a symbol to load chart data";
  }

  const maxPage = Math.max(0, Math.ceil(state.watchlist.length / MARKET_PAGE_SIZE) - 1);
  state.marketPage = Math.min(Number(state.marketPage || 0), maxPage);

  populateSymbolSelect();
  els.symbolSelect.value = state.selectedSymbol;
  setSymbolSearchMessage(`Removed ${symbol} from snapshot.`, "up");
  markBackendSyncDirty();
  persistAndRender();
  void refreshCandlesForSelectedSymbol(true);
}

function toggleWatchlistLock(symbol) {
  const clean = normalizeSymbolInput(symbol);
  if (!clean || !state.watchlist.includes(clean)) return;
  const locked = new Set(normalizeLockedWatchlistSymbols(state.lockedWatchlistSymbols, state.watchlist));
  const nextLocked = new Set(locked);
  if (locked.has(clean)) {
    nextLocked.delete(clean);
    setSymbolSearchMessage(`Unlocked ${clean}.`, "up");
  } else {
    nextLocked.add(clean);
    setSymbolSearchMessage(`Locked ${clean} from auto-removal.`, "up");
  }
  state.lockedWatchlistSymbols = normalizeLockedWatchlistSymbols(Array.from(nextLocked), state.watchlist);
  markBackendSyncDirty();
  persistAndRender();
}

function setMarketPageForSymbol(symbol) {
  const sorted = getSortedTrackedSymbols();
  const index = sorted.indexOf(symbol);
  if (index < 0) return;
  state.marketPage = Math.floor(index / MARKET_PAGE_SIZE);
}

function onSymbolChangeFromTradeForm() {
  const symbol = String(els.symbolSelect.value || "").toUpperCase();
  if (!symbol || !state.prices[symbol]) return;

  state.selectedSymbol = symbol;
  setMarketPageForSymbol(symbol);
  persistState();
  renderAll();
  void refreshCandlesForSelectedSymbol(true);
}

function onMarketTableClick(event) {
  const target = event.target;
  if (!(target instanceof Element)) return;

  const lockButton = target.closest(".market-lock-btn");
  if (lockButton) {
    const symbol = normalizeSymbolInput(lockButton.getAttribute("data-symbol"));
    if (!symbol) return;
    toggleWatchlistLock(symbol);
    return;
  }

  const removeButton = target.closest(".market-remove-btn");
  if (removeButton) {
    const symbol = normalizeSymbolInput(removeButton.getAttribute("data-symbol"));
    if (!symbol) return;
    removeSymbolFromWatchlist(symbol);
    return;
  }

  const button = target.closest(".symbol-link");
  if (!button) return;

  const symbol = String(button.getAttribute("data-symbol") || "").toUpperCase();
  if (!symbol || !state.prices[symbol]) return;

  state.selectedSymbol = symbol;
  setMarketPageForSymbol(symbol);
  els.symbolSelect.value = symbol;
  persistState();
  renderAll();
  void refreshCandlesForSelectedSymbol(true);
}

function onPositionsTableClick(event) {
  const target = event.target;
  if (!(target instanceof Element)) return;
  const sellButton = target.closest(".position-sell-btn");
  if (!sellButton) return;

  const symbol = normalizeSymbolInput(sellButton.getAttribute("data-symbol"));
  const shares = Number(sellButton.getAttribute("data-shares"));
  if (!symbol || !state.prices[symbol] || !Number.isFinite(shares) || shares <= 0) return;

  if (!state.watchlist.includes(symbol) && state.watchlist.length < MAX_TRACKED_SYMBOLS) {
    state.watchlist.push(symbol);
    ensureAiRefreshMetaForSymbol(symbol, Date.now());
  }
  populateSymbolSelect();
  state.selectedSymbol = symbol;
  setMarketPageForSymbol(symbol);
  els.symbolSelect.value = symbol;
  els.sideSelect.value = "sell";
  els.sharesInput.value = String(Math.floor(shares));
  persistState();
  renderAll();
  void refreshCandlesForSelectedSymbol(true);
  setTradeMessage(`Prepared sell ticket for ${Math.floor(shares)} ${symbol} shares.`, "down");
}

function onMarketPrevPageClick() {
  const nextPage = Math.max(0, Number(state.marketPage || 0) - 1);
  if (nextPage === state.marketPage) return;
  state.marketPage = nextPage;
  persistAndRender();
}

function onMarketNextPageClick() {
  const symbols = getSortedTrackedSymbols();
  const maxPage = Math.max(0, Math.ceil(symbols.length / MARKET_PAGE_SIZE) - 1);
  const nextPage = Math.min(maxPage, Number(state.marketPage || 0) + 1);
  if (nextPage === state.marketPage) return;
  state.marketPage = nextPage;
  persistAndRender();
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

function onSymbolSearchInput() {
  const query = String(els.symbolSearchInput.value || "").trim();
  if (symbolSearchDebounceId) {
    window.clearTimeout(symbolSearchDebounceId);
    symbolSearchDebounceId = 0;
  }

  if (!query) {
    renderSymbolSearchResults([]);
    return;
  }

  symbolSearchDebounceId = window.setTimeout(() => {
    void runSymbolSearch(query);
  }, SEARCH_DEBOUNCE_MS);
}

function onSymbolSearchInputKeydown(event) {
  if (event.key !== "Enter") return;
  event.preventDefault();
  void onAddSymbolClick();
}

async function onAddSymbolClick() {
  const raw = String(els.symbolSearchInput.value || "").trim();
  const symbol = normalizeSymbolInput(raw);
  if (!symbol) {
    setSymbolSearchMessage("Enter a valid symbol (letters, numbers, dot, dash).", "down");
    return;
  }

  if (state.watchlist.includes(symbol)) {
    state.selectedSymbol = symbol;
    setMarketPageForSymbol(symbol);
    populateSymbolSelect();
    els.symbolSelect.value = symbol;
    setSymbolSearchMessage(`${symbol} is already on your watchlist.`, "up");
    persistAndRender();
    void refreshCandlesForSelectedSymbol(true);
    return;
  }

  if (state.watchlist.length >= MAX_TRACKED_SYMBOLS) {
    setSymbolSearchMessage(
      `Watchlist limit reached (${MAX_TRACKED_SYMBOLS}). Remove one before adding another.`,
      "down"
    );
    return;
  }

  setSymbolSearchMessage(`Adding ${symbol}...`, "up");

  try {
    const quote = await fetchQuoteForSymbol(symbol);
    state.watchlist.push(symbol);
    state.prices[symbol] = {
      price: round2(quote.price),
      lastClose: round2(quote.lastClose),
    };
    ensureAiRefreshMetaForSymbol(symbol, Date.now());
    state.selectedSymbol = symbol;
    setMarketPageForSymbol(symbol);
    els.symbolSearchInput.value = "";
    renderSymbolSearchResults([]);
    populateSymbolSelect();
    els.symbolSelect.value = symbol;
    setSymbolSearchMessage(`Added ${symbol} to your watchlist.`, "up");
    markBackendSyncDirty();
    persistAndRender();
    void refreshCandlesForSelectedSymbol(true);
  } catch (error) {
    const details = error instanceof Error ? error.message : "Symbol could not be loaded.";
    setSymbolSearchMessage(`Could not add ${symbol}. ${details}`, "down");
  }
}

function onSymbolSearchResultClick(event) {
  const target = event.target;
  if (!(target instanceof Element)) return;
  const button = target.closest(".symbol-search-result");
  if (!button) return;
  const symbol = normalizeSymbolInput(button.getAttribute("data-symbol"));
  if (!symbol) return;
  els.symbolSearchInput.value = symbol;
  void onAddSymbolClick();
}

async function runSymbolSearch(query) {
  const trimmed = String(query || "").trim();
  if (!trimmed) {
    renderSymbolSearchResults([]);
    return;
  }

  try {
    const endpoint =
      `${API_SYMBOL_SEARCH_URL}?query=${encodeURIComponent(trimmed)}` +
      `&limit=${encodeURIComponent(8)}`;
    const response = await fetch(endpoint, {
      headers: {
        Accept: "application/json",
      },
    });

    if (!response.ok) {
      throw new Error("Symbol search unavailable.");
    }

    const payload = await response.json();
    const results = Array.isArray(payload.results) ? payload.results : [];
    if (trimmed !== String(els.symbolSearchInput.value || "").trim()) return;
    renderSymbolSearchResults(results);
  } catch {
    if (trimmed !== String(els.symbolSearchInput.value || "").trim()) return;
    renderSymbolSearchResults([]);
  }
}

function renderSymbolSearchResults(results) {
  if (!Array.isArray(results) || results.length === 0) {
    els.symbolSearchResults.innerHTML = "";
    return;
  }

  els.symbolSearchResults.innerHTML = results
    .map((row) => {
      const symbol = normalizeSymbolInput(row?.symbol);
      if (!symbol) return "";
      const name = String(row?.name || "").trim();
      const exchange = String(row?.exchange || "").trim();
      const alreadyTracked = state.watchlist.includes(symbol);
      const status = alreadyTracked ? "Added" : "Add";
      const safeName = escapeHtml(name || "Unknown name");
      const safeExchange = escapeHtml(exchange);
      return `
        <button type="button" class="symbol-search-result" data-symbol="${symbol}" ${
          alreadyTracked ? "disabled" : ""
        }>
          <span class="symbol-search-result-symbol">${symbol}</span>
          <span class="symbol-search-result-name">${safeName}</span>
          <span class="symbol-search-result-meta">${safeExchange}</span>
          <span class="symbol-search-result-action">${status}</span>
        </button>
      `;
    })
    .join("");
}

function setSymbolSearchMessage(message, tone) {
  els.symbolSearchMessage.textContent = message;
  els.symbolSearchMessage.className = `trade-message ${tone}`;
}

async function onTradeSubmit(event) {
  event.preventDefault();

  const symbol = els.symbolSelect.value;
  const side = String(els.sideSelect.value || "").toLowerCase();
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

  if (!(side === "buy" || side === "sell")) {
    setTradeMessage("Trade side must be buy or sell.", "down");
    return;
  }

  if (!brokerTradingAvailable) {
    setTradeMessage("Broker trading is unavailable. Alpaca connection is required.", "down");
    return;
  }

  if (!hasLiveQuoteData) {
    setTradeMessage("Live market data unavailable. Trading is paused until Alpaca quotes recover.", "down");
    return;
  }

  await executeBrokerTrade(symbol, side, shares);
}

async function executeBrokerTrade(symbol, side, shares) {
  if (isSubmittingBrokerTrade) return;
  if (!(side === "buy" || side === "sell")) {
    setTradeMessage("Trade side must be buy or sell.", "down");
    return;
  }
  isSubmittingBrokerTrade = true;
  try {
    setTradeMessage(`Submitting ${side.toUpperCase()} ${shares} ${symbol} to Alpaca...`, "up");
    const response = await fetch(API_TRADES_URL, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        symbol,
        side,
        qty: shares,
      }),
    });
    if (!response.ok) {
      let details = "";
      try {
        const body = await response.json();
        details = typeof body?.error === "string" ? body.error : "";
      } catch {
        details = "";
      }
      throw new Error(
        details ? `Trade API ${response.status}: ${details}` : `Trade API ${response.status}.`
      );
    }
    const payload = await response.json();
    const order = payload?.order && typeof payload.order === "object" ? payload.order : null;
    const snapshot = payload?.snapshot && typeof payload.snapshot === "object" ? payload.snapshot : null;
    const orderStatus = String(order?.status || "").toLowerCase();
    const filledQty = Number(order?.filledQty);
    const filledPrice = Number(order?.filledAvgPrice);
    const tx =
      Number.isFinite(filledQty) && filledQty > 0 && Number.isFinite(filledPrice) && filledPrice > 0
        ? {
            side: side.toUpperCase(),
            symbol,
            shares: Math.floor(filledQty),
            price: round2(filledPrice),
            total: round2(Math.floor(filledQty) * filledPrice),
          }
        : null;
    if (snapshot) {
      const changed = applyBrokerSnapshotToState(snapshot);
      if (tx) {
        pushTransaction(tx);
      }
      if (changed || tx) {
        persistAndRender();
      }
    } else {
      await refreshBrokerAccountSnapshot({ silent: false });
      if (tx) {
        pushTransaction(tx);
        persistAndRender();
      }
    }
    if (orderStatus === "filled") {
      setTradeMessage(
        `Alpaca filled ${side.toUpperCase()} ${shares} ${symbol}.`,
        side === "buy" ? "up" : "down"
      );
    } else {
      setTradeMessage(
        `Alpaca accepted ${side.toUpperCase()} ${shares} ${symbol} (${orderStatus || "submitted"}).`,
        "up"
      );
    }
  } catch (error) {
    setTradeMessage(error instanceof Error ? error.message : "Broker trade failed.", "down");
  } finally {
    isSubmittingBrokerTrade = false;
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
  markBackendSyncDirty();
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
  markBackendSyncDirty();
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
    const incoming = await fetchLiveQuotes(getTrackedSymbols());
    applyLiveQuotes(incoming);
    hasLiveQuoteData = true;
    marketSourceLabel = "Source: Alpaca live quotes";
  } catch (error) {
    updatePricesWithSimulation();
    hasLiveQuoteData = false;
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

async function fetchLiveQuotes(symbols) {
  if (!Array.isArray(symbols) || symbols.length === 0) {
    throw new Error("No symbols provided for quote refresh.");
  }
  const joined = symbols.join(",");
  const endpoint = `${API_QUOTES_URL}?symbols=${encodeURIComponent(joined)}`;

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

async function fetchQuoteForSymbol(symbol) {
  const normalized = normalizeSymbolInput(symbol);
  if (!normalized) {
    throw new Error("Invalid symbol format.");
  }

  const quotes = await fetchLiveQuotes([normalized]);
  const match = quotes.find((row) => normalizeSymbolInput(row?.symbol) === normalized);
  if (!match) {
    throw new Error("No quote returned by Alpaca for this symbol.");
  }

  const price = Number(match.price);
  if (!Number.isFinite(price) || price <= 0) {
    throw new Error("Quote price is invalid for this symbol.");
  }

  const lastClose = Number(match.lastClose);
  return {
    price,
    lastClose: Number.isFinite(lastClose) && lastClose > 0 ? lastClose : price,
  };
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
  for (const symbol of getTrackedSymbols()) {
    const quote = state.prices[symbol];
    if (!quote) continue;
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
    candleSourceLabel = `Source: Alpaca ${mode.label.toLowerCase()} candles`;
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
  closeSettingsModal();

  const priorSymbol = state.selectedSymbol;
  const aiSettings = normalizeAiSettings(state.aiSettings);
  const aiResearchCache = normalizeAiResearchCache(state.aiResearchCache);
  const companyCache = normalizeCompanyCache(state.companyCache);
  const aiRefreshMeta = normalizeAiRefreshMeta(state.aiRefreshMeta, state.watchlist);
  const marketScout = normalizeMarketScoutState(state.marketScout);
  const autobot = normalizeAutobotState(state.autobot);
  const backendRuntimeUpdatedAt = Number(state.backendRuntimeUpdatedAt || 0);
  state = createInitialState();
  state.aiSettings = aiSettings;
  state.aiResearchCache = aiResearchCache;
  state.companyCache = companyCache;
  state.aiRefreshMeta = normalizeAiRefreshMeta(aiRefreshMeta, state.watchlist);
  state.marketScout = marketScout;
  state.backendRuntimeUpdatedAt =
    Number.isFinite(backendRuntimeUpdatedAt) && backendRuntimeUpdatedAt > 0
      ? Math.floor(backendRuntimeUpdatedAt)
      : 0;
  state.autobot = {
    ...DEFAULT_AUTOBOT_STATE,
    enabled: autobot.enabled,
    lastStatus: autobot.enabled ? "Enabled" : "Disabled",
  };
  hydrateCompanyRuntimeFromState();
  if (priorSymbol && state.prices[priorSymbol]) {
    state.selectedSymbol = priorSymbol;
    setMarketPageForSymbol(priorSymbol);
  }
  syncAiSettingsControls();
  populateSymbolSelect();
  els.symbolSelect.value = state.selectedSymbol;
  els.symbolSearchInput.value = "";
  renderSymbolSearchResults([]);
  setSymbolSearchMessage("", "up");
  chartCandles = [];
  setTradeMessage("Wallet reset to starting balance.", "up");
  markBackendSyncDirty();
  persistAndRender();
  restartAiResearchAutoRefreshScheduler();
  restartAutobotScheduler();
  void refreshCandlesForSelectedSymbol(true);
}

function persistAndRender() {
  persistState();
  renderAll();
}

function renderAll() {
  syncAiSettingsControls();
  renderAutobotPanel();
  renderWatchlistResearchTicker();
  renderSummary();
  renderTradeQuoteBox();
  renderMarket();
  renderAiPanel();
  renderPositions();
  renderActivity();
  renderCandleChart();
  els.marketSourceStatus.textContent = marketSourceLabel;
}

function renderWatchlistResearchTicker() {
  if (!els.watchlistResearchTickerLine) return;
  const scout = normalizeMarketScoutState(state.marketScout);
  state.marketScout = scout;
  if (els.discoveryEnabledToggle) {
    els.discoveryEnabledToggle.checked = Boolean(scout.enabled);
  }
  els.watchlistResearchTickerLine.textContent = scout.enabled
    ? scout.latestMessage || "Research ticker idle."
    : "Discovery tracker disabled.";
}

function renderAutobotPanel() {
  if (
    !els.autobotEnabledToggle ||
    !els.autobotStatus ||
    !els.backendRuntimeStatus ||
    !els.autobotThoughtArea ||
    !els.autobotRecommendationLine ||
    !els.autobotRunNowBtn
  ) {
    return;
  }

  const autobot = normalizeAutobotState(state.autobot);
  state.autobot = autobot;
  const settings = getAiSettings();
  els.autobotEnabledToggle.checked = autobot.enabled;

  if (isAutobotRunning) {
    els.autobotStatus.textContent = "Thinking with portfolio + snapshot context...";
  } else if (autobot.enabled) {
    const nextRunMins =
      backendRuntimeNextRunAt > 0
        ? Math.max(0, Math.ceil((backendRuntimeNextRunAt - Date.now()) / (60 * 1000)))
        : getAutobotNextRunMins(autobot.lastRunAt, settings.autobotIntervalMins);
    const nextText = Number.isFinite(nextRunMins) ? ` | next ~${nextRunMins}m` : "";
    els.autobotStatus.textContent = `Auto ON (${settings.autobotIntervalMins}m cadence)${nextText}`;
  } else {
    els.autobotStatus.textContent = "Manual mode (auto disabled)";
  }

  const heartbeatText =
    backendRuntimeHeartbeatAt > 0 ? stampDateTime(backendRuntimeHeartbeatAt) : "--";
  const lastCycleText = autobot.lastRunAt > 0 ? stampDateTime(autobot.lastRunAt) : "--";
  let nextRunText = "--";
  if (autobot.enabled) {
    if (backendRuntimeNextRunAt > 0) {
      const deltaMs = backendRuntimeNextRunAt - Date.now();
      nextRunText = deltaMs <= 0 ? "due now" : `~${Math.max(1, Math.ceil(deltaMs / (60 * 1000)))}m`;
    } else {
      nextRunText = `~${getAutobotNextRunMins(autobot.lastRunAt, settings.autobotIntervalMins)}m`;
    }
  }
  els.backendRuntimeStatus.textContent =
    `Backend Runtime: ${backendRuntimeServerSideActive ? "enabled" : "disabled"} | ` +
    `sync ${heartbeatText} | last cycle ${lastCycleText} | next ${nextRunText}`;

  els.autobotThoughtArea.value = autobot.latestThought || "No thoughts yet. Click Think Now.";
  const latestAutoAction = normalizeAutobotAction(autobot.latestAutoAction);
  const recommendation = normalizeAutobotRecommendation(autobot.latestRecommendation);
  if (latestAutoAction) {
    const stamp = latestAutoAction.at > 0 ? ` (${stampDateTime(latestAutoAction.at)})` : "";
    const execTag = latestAutoAction.executed ? "executed" : "no trade";
    const sizePart =
      latestAutoAction.type === "HOLD" ? "HOLD" : `${latestAutoAction.type} ${latestAutoAction.shares} ${latestAutoAction.symbol}`;
    els.autobotRecommendationLine.textContent = `Latest Auto Action${stamp}: ${sizePart} [${execTag}] | ${latestAutoAction.reason}`;
  } else if (recommendation) {
    const sizePart =
      recommendation.side === "hold"
        ? "HOLD"
        : `${recommendation.side.toUpperCase()} ${recommendation.shares} ${recommendation.symbol}`;
    els.autobotRecommendationLine.textContent = `Latest Decision: ${sizePart} | ${recommendation.reason}`;
  } else {
    els.autobotRecommendationLine.textContent = "Latest Auto Action: --";
  }
  els.autobotRunNowBtn.disabled = isAutobotRunning;
}

function getAutobotNextRunMins(lastRunAt, intervalMins) {
  const intervalMs = Math.max(1, Number(intervalMins || 30)) * 60 * 1000;
  if (!Number.isFinite(lastRunAt) || lastRunAt <= 0) return 0;
  const nextAt = lastRunAt + intervalMs;
  const delta = nextAt - Date.now();
  if (delta <= 0) return 0;
  return Math.max(1, Math.ceil(delta / (60 * 1000)));
}

function setAutobotMessage(message, tone) {
  if (!els.autobotMessage) return;
  els.autobotMessage.textContent = message;
  els.autobotMessage.className = `trade-message ${tone}`;
}

function renderTradeQuoteBox() {
  const symbol = state.selectedSymbol || "--";
  const quote = state.prices[symbol];
  const side = String(els.sideSelect.value || "buy").toLowerCase() === "sell" ? "sell" : "buy";
  const sharesInput = Number(els.sharesInput.value);
  const shares = Number.isFinite(sharesInput) && sharesInput > 0 ? Math.floor(sharesInput) : 0;

  els.tradeQuoteSymbol.textContent = symbol;
  els.tradeQuoteChange.classList.remove("up", "down");

  if (!quote) {
    els.tradeQuoteValue.textContent = "N/A";
    els.tradeQuoteChange.textContent = "Day: unavailable";
    els.tradeQuoteNotional.textContent = "Order value: unavailable";
    return;
  }

  const price = Number(quote.price);
  const lastClose = Number(quote.lastClose);
  if (!Number.isFinite(price) || price <= 0) {
    els.tradeQuoteValue.textContent = "N/A";
    els.tradeQuoteChange.textContent = "Day: unavailable";
    els.tradeQuoteNotional.textContent = "Order value: unavailable";
    return;
  }

  const dayChange = round2(
    price - (Number.isFinite(lastClose) && lastClose > 0 ? lastClose : price)
  );
  const dayPct =
    Number.isFinite(lastClose) && lastClose > 0 ? round2((dayChange / lastClose) * 100) : 0;

  els.tradeQuoteValue.textContent = fmtMoney(price);
  els.tradeQuoteChange.textContent = `Day: ${signedMoney(dayChange)} (${signedPct(dayPct)})`;
  if (dayChange > 0) els.tradeQuoteChange.classList.add("up");
  if (dayChange < 0) els.tradeQuoteChange.classList.add("down");

  if (shares <= 0) {
    els.tradeQuoteNotional.textContent = "Order value: enter shares";
  } else {
    const orderValue = round2(price * shares);
    const actionLabel = side === "sell" ? "Sell" : "Buy";
    els.tradeQuoteNotional.textContent = `${actionLabel} ${shares} ~= ${fmtMoney(orderValue)}`;
  }
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
  const settings = getAiSettings();
  const sortedSymbols = getSortedTrackedSymbols();
  const totalPages = Math.max(1, Math.ceil(sortedSymbols.length / MARKET_PAGE_SIZE));
  const clampedPage = clamp(Number(state.marketPage || 0), 0, totalPages - 1);
  state.marketPage = clampedPage;
  const start = clampedPage * MARKET_PAGE_SIZE;
  const pageSymbols = sortedSymbols.slice(start, start + MARKET_PAGE_SIZE);

  if (pageSymbols.length === 0) {
    els.marketBody.innerHTML = `
      <tr>
        <td colspan="6">No symbols in watchlist.</td>
      </tr>
    `;
  } else {
    const lockedSet = new Set(
      normalizeLockedWatchlistSymbols(state.lockedWatchlistSymbols, state.watchlist)
    );
    els.marketBody.innerHTML = pageSymbols.map((symbol) => {
      const quote = state.prices[symbol];
      if (!quote) return "";
      const researchEntry = getAiResearchEntry(symbol, settings);
      const profile = researchEntry?.profile || null;
      const confidence = getResearchConfidenceForSymbol(symbol, settings);
      const hasConfidence = Number.isFinite(confidence);
      const confidenceTone = hasConfidence
        ? confidence >= 75
          ? "high"
          : confidence >= 55
            ? "mid"
            : "low"
        : "empty";
      const confidenceText = hasConfidence ? `${confidence}%` : "&nbsp;";
      const signal = profile ? derivePositionSignal(symbol, profile) : null;
      const positionTone = signal?.tone || "empty";
      const positionText = signal?.label || "&nbsp;";
      const dayChange = round2(quote.price - quote.lastClose);
      const dayPct = quote.lastClose > 0 ? round2((dayChange / quote.lastClose) * 100) : 0;
      const cls = dayChange >= 0 ? "up" : "down";
      const active = symbol === state.selectedSymbol ? "is-active" : "";
      const hasOpenPosition = Number(state.positions[symbol]?.shares || 0) > 0;
      const isLocked = lockedSet.has(symbol);
      const lockIcon = isLocked ? "&#128274;" : "&#128275;";
      const lockTitle = isLocked ? "Unlock (allow auto-removal)" : "Lock (prevent auto-removal)";
      const removeDisabled = hasOpenPosition || isLocked;
      const removeTitle = hasOpenPosition
        ? "Sell position first"
        : isLocked
          ? "Unlock first"
          : "Remove from watchlist";
      return `
        <tr>
          <td>
            <button type="button" class="symbol-link ${active}" data-symbol="${symbol}">
              ${symbol}
            </button>
          </td>
          <td>${fmtMoney(quote.price)}</td>
          <td>
            <span class="position-chip ${positionTone}">${positionText}</span>
          </td>
          <td>
            <span class="confidence-chip ${confidenceTone}">${confidenceText}</span>
          </td>
          <td class="${cls}">
            ${signedMoney(dayChange)} (${signedPct(dayPct)})
          </td>
          <td>
            <div class="market-actions">
            <button
              type="button"
              class="table-action-btn market-icon-btn market-lock-btn ${isLocked ? "is-locked" : ""}"
              data-symbol="${symbol}"
              title="${lockTitle}"
              aria-label="${lockTitle}"
            >
              ${lockIcon}
            </button>
            <button
              type="button"
              class="table-action-btn market-icon-btn market-remove-btn"
              data-symbol="${symbol}"
              ${removeDisabled ? "disabled" : ""}
              title="${removeTitle}"
              aria-label="${removeTitle}"
            >
              &times;
            </button>
            </div>
          </td>
        </tr>
      `;
    }).join("");
  }

  els.marketPageLabel.textContent = `Page ${clampedPage + 1} of ${totalPages}`;
  els.marketPrevPageBtn.disabled = clampedPage <= 0;
  els.marketNextPageBtn.disabled = clampedPage >= totalPages - 1;
}

function renderAiPanel() {
  const symbol = state.selectedSymbol || getTrackedSymbols()[0] || DEFAULT_SYMBOLS[0];
  const settings = getAiSettings();
  const quote = state.prices[symbol];
  const company = companyInfoBySymbol[symbol] || getCachedCompanyInfo(symbol)?.company || null;
  const companyError = companyInfoErrorBySymbol[symbol] || "";
  const companyLoadStatus = companyInfoLoadStatusBySymbol[symbol] || "";
  const researchEntry = getAiResearchEntry(symbol, settings);
  const profile = researchEntry?.profile || null;
  const sentiment = profile ? clamp(Math.round(profile.sentiment), -100, 100) : null;
  const confidence = profile ? clamp(Math.round(profile.confidence), 0, 100) : null;
  const sentimentWidth = profile ? ((sentiment + 100) / 200) * 100 : 0;
  const recommendation = profile ? buildAiRecommendation(symbol, profile) : null;
  const aiCacheKey = buildAiCacheKey(symbol, settings);
  const aiLoadStatus = aiResearchLoadStatusByKey[aiCacheKey] || "";
  const aiError = aiResearchErrorByKey[aiCacheKey] || "";
  const isStale = researchEntry ? isAiResearchEntryStale(researchEntry) : false;
  const titleText = company?.name
    ? `Watchlist Research: ${symbol} | ${company.name}`
    : `Watchlist Research: ${symbol}`;
  els.aiTitle.textContent = titleText;
  els.aiCompanyInfo.textContent = buildAiCompanyInfoLine(symbol, company);
  els.aiCompanyInfo.className = `ai-company-info${
    companyLoadStatus === "unavailable" && companyError ? " down" : ""
  }`;

  if (!hasLiveQuoteData) {
    currentAiRecommendation = null;
    els.aiStatus.textContent = "No live market data. AI research paused.";
    els.aiSentimentBadge.classList.remove("up", "down");
    els.aiSentimentBadge.textContent = "--";
    els.aiSentimentFill.style.width = "0%";
    els.aiConfidenceValue.textContent = "--";
    els.aiConfidenceFill.style.width = "0%";
    els.aiRecommendationText.textContent = "--";
    els.applyAiRecommendationBtn.disabled = true;
    els.applyAiRecommendationBtn.textContent = "No Action To Apply";
    els.updateAiResearchBtn.disabled = true;
    els.updateAiResearchBtn.textContent = "Update";
    const offlineResearchText = buildNoLiveDataResearchText(symbol);
    const offlineSources = replaceResearchUrlsWithSourceTokens(offlineResearchText);
    els.aiResearchArea.value = offlineSources.text;
    renderAiResearchSources(offlineSources.urls);
    return;
  }

  ensureCompanyInfoLoaded(symbol);

  currentAiRecommendation = recommendation;
  if (aiLoadStatus === "loading") {
    els.aiStatus.textContent = "Updating OpenAI research...";
  } else if (aiError) {
    els.aiStatus.textContent = `AI research error: ${aiError}`;
  } else if (researchEntry) {
    els.aiStatus.textContent = `OpenAI ${settings.model} | ${settings.horizon} horizon | Updated ${stampDateTime(
      researchEntry.fetchedAt
    )}${isStale ? " (stale)" : ""}`;
  } else if (companyError) {
    els.aiStatus.textContent = `Company profile error: ${companyError}`;
  } else {
    els.aiStatus.textContent = "No AI research cached yet. Click Update.";
  }
  els.aiSentimentBadge.classList.remove("up", "down");
  if (profile) {
    els.aiSentimentBadge.textContent = `${sentiment > 0 ? "+" : ""}${sentiment} / 100`;
    if (sentiment > 0) els.aiSentimentBadge.classList.add("up");
    if (sentiment < 0) els.aiSentimentBadge.classList.add("down");
    els.aiSentimentFill.style.width = `${sentimentWidth.toFixed(1)}%`;
    els.aiConfidenceValue.textContent = `${confidence}%`;
    els.aiConfidenceFill.style.width = `${confidence.toFixed(1)}%`;
    els.aiRecommendationText.textContent = recommendation.message;
    els.applyAiRecommendationBtn.disabled = !recommendation.canApply;
    els.applyAiRecommendationBtn.textContent = recommendation.canApply
      ? `Apply ${(recommendation.verb || recommendation.side.toUpperCase())} ${recommendation.shares} Shares`
      : "No Action To Apply";
  } else {
    els.aiSentimentBadge.textContent = "--";
    els.aiSentimentFill.style.width = "0%";
    els.aiConfidenceValue.textContent = "--";
    els.aiConfidenceFill.style.width = "0%";
    els.aiRecommendationText.textContent = "--";
    els.applyAiRecommendationBtn.disabled = true;
    els.applyAiRecommendationBtn.textContent = "No Action To Apply";
  }
  els.updateAiResearchBtn.disabled = aiLoadStatus === "loading";
  els.updateAiResearchBtn.textContent = aiLoadStatus === "loading" ? "Updating..." : "Update";

  const researchText = buildAiResearchText(symbol, quote, researchEntry);
  const researchSources = replaceResearchUrlsWithSourceTokens(researchText);
  els.aiResearchArea.value = researchSources.text;
  renderAiResearchSources(researchSources.urls);
}

function getAiProfile(symbol) {
  const settings = getAiSettings();
  const entry = getAiResearchEntry(symbol, settings);
  if (entry?.profile) {
    return entry.profile;
  }
  return getMockAiProfile(symbol);
}

function getMockAiProfile(symbol) {
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

function ensureCompanyInfoLoaded(symbol) {
  if (!symbol) return;
  const cached = getCachedCompanyInfo(symbol);
  if (cached) {
    companyInfoBySymbol[symbol] = cached.company;
    companyInfoLoadStatusBySymbol[symbol] = "loaded";
    delete companyInfoErrorBySymbol[symbol];
    return;
  }

  const now = Date.now();
  const currentStatus = companyInfoLoadStatusBySymbol[symbol];
  if (currentStatus === "loading") {
    return;
  }
  const lastAttemptAt = Number(companyInfoLastAttemptAtBySymbol[symbol] || 0);
  if (now - lastAttemptAt < COMPANY_INFO_RETRY_MS) {
    return;
  }

  companyInfoLastAttemptAtBySymbol[symbol] = now;
  companyInfoLoadStatusBySymbol[symbol] = "loading";
  void fetchCompanyInfo(symbol)
    .then((company) => {
      if (company) {
        companyInfoBySymbol[symbol] = company;
        setCachedCompanyInfo(symbol, company);
        delete companyInfoErrorBySymbol[symbol];
        companyInfoLoadStatusBySymbol[symbol] = "loaded";
        persistState();
      } else {
        companyInfoErrorBySymbol[symbol] = "No profile returned by API.";
        companyInfoLoadStatusBySymbol[symbol] = "unavailable";
      }
    })
    .catch((error) => {
      companyInfoErrorBySymbol[symbol] = getErrorMessage(error);
      companyInfoLoadStatusBySymbol[symbol] = "unavailable";
    })
    .finally(() => {
      if (state.selectedSymbol === symbol) {
        renderAiPanel();
      }
    });
}

async function onUpdateAiResearchClick() {
  const symbol = state.selectedSymbol;
  if (!symbol) return;
  await refreshAiResearchForSymbol(symbol, { source: "manual" });
}

async function refreshAiResearchForSymbol(symbol, options = {}) {
  const normalizedSymbol = normalizeSymbolInput(symbol);
  if (!normalizedSymbol) return false;
  const settings = getAiSettings();
  const aiCacheKey = buildAiCacheKey(normalizedSymbol, settings);
  if (aiResearchLoadStatusByKey[aiCacheKey] === "loading") return false;

  if (!hasLiveQuoteData) {
    aiResearchLoadStatusByKey[aiCacheKey] = "unavailable";
    aiResearchErrorByKey[aiCacheKey] = "No live market data available. AI research paused.";
    if (state.selectedSymbol === normalizedSymbol) {
      renderAiPanel();
    } else {
      renderMarket();
    }
    return false;
  }

  const source = options && typeof options === "object" ? String(options.source || "manual") : "manual";
  const attemptAt = Date.now();
  ensureAiRefreshMetaForSymbol(normalizedSymbol, attemptAt);
  markAiResearchRefreshAttempt(normalizedSymbol, attemptAt);

  aiResearchLoadStatusByKey[aiCacheKey] = "loading";
  delete aiResearchErrorByKey[aiCacheKey];
  if (state.selectedSymbol === normalizedSymbol) {
    renderAiPanel();
  }

  try {
    const company = await getCompanyInfoForAi(normalizedSymbol);
    const quote = state.prices[normalizedSymbol] || null;
    const profile = await fetchAiResearchForSymbol(normalizedSymbol, settings, quote, company);
    upsertAiResearchEntry(normalizedSymbol, settings, profile);
    aiResearchLoadStatusByKey[aiCacheKey] = "loaded";
    markBackendSyncDirty();
    persistState();
    renderAll();
    return true;
  } catch (error) {
    aiResearchLoadStatusByKey[aiCacheKey] = "unavailable";
    aiResearchErrorByKey[aiCacheKey] = getErrorMessage(error);
    if (source === "manual" && state.selectedSymbol === normalizedSymbol) {
      renderAiPanel();
    } else {
      renderMarket();
    }
    return false;
  }
}

async function getCompanyInfoForAi(symbol) {
  const cached = getCachedCompanyInfo(symbol);
  if (cached?.company) {
    companyInfoBySymbol[symbol] = cached.company;
    companyInfoLoadStatusBySymbol[symbol] = "loaded";
    return cached.company;
  }

  try {
    const company = await fetchCompanyInfo(symbol);
    if (company) {
      companyInfoBySymbol[symbol] = company;
      companyInfoLoadStatusBySymbol[symbol] = "loaded";
      delete companyInfoErrorBySymbol[symbol];
      setCachedCompanyInfo(symbol, company);
      persistState();
      return company;
    }
  } catch (error) {
    companyInfoErrorBySymbol[symbol] = getErrorMessage(error);
    companyInfoLoadStatusBySymbol[symbol] = "unavailable";
  }

  return null;
}

async function fetchAiResearchForSymbol(symbol, settings, quote, company) {
  const marketContext = await buildAiMarketContextForResearch(symbol, settings, quote);
  const position = buildPositionContextForAi(symbol, quote);
  const endpoint = API_AI_RESEARCH_URL;
  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      symbol,
      provider: settings.provider,
      model: settings.model,
      horizon: settings.horizon,
      quote: quote
        ? {
            price: Number(quote.price),
            lastClose: Number(quote.lastClose),
          }
        : null,
      company: company
        ? {
            name: company.name,
            exchange: company.exchange,
            industry: company.industry,
            country: company.country,
            currency: company.currency,
            ipo: company.ipo,
            marketCapitalization: company.marketCapitalization,
            shareOutstanding: company.shareOutstanding,
          }
        : null,
      marketContext,
      position,
    }),
  });

  if (!response.ok) {
    let details = "";
    try {
      const body = await response.json();
      if (body && typeof body === "object" && typeof body.error === "string") {
        details = body.error;
      } else if (body && typeof body === "object" && typeof body.message === "string") {
        details = body.message;
      }
    } catch {
      details = "";
    }
    if (response.status === 405) {
      throw new Error(
        "POST /api/ai/research is not available. Restart the Node server with the latest code."
      );
    }
    throw new Error(
      details
        ? `AI API returned ${response.status}: ${details}`
        : `AI API returned ${response.status}.`
    );
  }

  const payload = await response.json();
  if (!payload || typeof payload !== "object" || !payload.research || typeof payload.research !== "object") {
    throw new Error("AI API returned an unexpected payload.");
  }

  return normalizeIncomingAiProfile(payload.research);
}

async function buildAiMarketContextForResearch(symbol, settings, quote) {
  const normalizedSymbol = normalizeSymbolInput(symbol);
  const safeSettings = normalizeAiSettings(settings);
  const watchlist = getTrackedSymbols().slice(0, 20).map((itemSymbol) => {
    const itemQuote = state.prices[itemSymbol] || null;
    const itemPosition = state.positions[itemSymbol] || null;
    const shares = Math.max(0, Math.floor(Number(itemPosition?.shares) || 0));
    const avgCost = Number(itemPosition?.avgCost || 0);
    const marketPrice = Number(itemQuote?.price || 0);
    const marketValue = shares > 0 && marketPrice > 0 ? round2(shares * marketPrice) : 0;
    const dayChange =
      itemQuote && Number.isFinite(itemQuote.price) && Number.isFinite(itemQuote.lastClose)
        ? round2(itemQuote.price - itemQuote.lastClose)
        : null;
    const dayPct =
      dayChange !== null && itemQuote && itemQuote.lastClose > 0
        ? round2((dayChange / itemQuote.lastClose) * 100)
        : null;
    const entry = getAiResearchEntry(itemSymbol, safeSettings);
    const profile = entry?.profile || null;
    return {
      symbol: itemSymbol,
      price: itemQuote?.price || null,
      lastClose: itemQuote?.lastClose || null,
      dayChange,
      dayPct,
      shares,
      avgCost: shares > 0 && avgCost > 0 ? round2(avgCost) : null,
      marketValue: marketValue > 0 ? marketValue : 0,
      sentiment: profile ? profile.sentiment : null,
      confidence: profile ? profile.confidence : null,
      action: profile ? profile.action : null,
    };
  });

  const candleWindow = getAiCandleWindowByHorizon(safeSettings.horizon);
  let candleSummary = null;
  try {
    const candles = await fetchLiveCandles(
      normalizedSymbol,
      candleWindow.timeframe,
      candleWindow.limit
    );
    candleSummary = summarizeCandlesForAi(candles, candleWindow.timeframe);
  } catch {
    candleSummary = null;
  }

  return {
    symbol: normalizedSymbol,
    horizon: safeSettings.horizon,
    quote: quote
      ? {
          price: Number(quote.price),
          lastClose: Number(quote.lastClose),
        }
      : null,
    portfolio: {
      cash: round2(state.cash),
      portfolioValue: calcPortfolioValue(),
      unrealizedPnl: calcUnrealizedPnl(),
      realizedPnl: round2(state.realizedPnl),
      totalEquity: round2(state.cash + calcPortfolioValue()),
    },
    watchlist,
    candleSummary,
  };
}

function getAiCandleWindowByHorizon(horizon) {
  const value = normalizeAiHorizon(horizon);
  if (value === "short") {
    return {
      timeframe: "1Min",
      limit: 120,
    };
  }
  if (value === "long") {
    return {
      timeframe: "1Day",
      limit: 180,
    };
  }
  return {
    timeframe: "1Day",
    limit: 90,
  };
}

function summarizeCandlesForAi(candles, timeframe) {
  if (!Array.isArray(candles) || candles.length < 2) return null;
  const sorted = [...candles].sort((a, b) => Date.parse(a.t) - Date.parse(b.t));
  const first = sorted[0];
  const last = sorted[sorted.length - 1];
  const firstClose = Number(first.c);
  const lastClose = Number(last.c);
  if (!Number.isFinite(firstClose) || !Number.isFinite(lastClose) || firstClose <= 0) return null;

  let high = Number.NEGATIVE_INFINITY;
  let low = Number.POSITIVE_INFINITY;
  let rangePctSum = 0;
  for (const row of sorted) {
    const h = Number(row.h);
    const l = Number(row.l);
    const c = Number(row.c);
    if (!Number.isFinite(h) || !Number.isFinite(l) || !Number.isFinite(c) || c <= 0) continue;
    high = Math.max(high, h);
    low = Math.min(low, l);
    rangePctSum += ((h - l) / c) * 100;
  }

  const changePct = round2(((lastClose - firstClose) / firstClose) * 100);
  const avgRangePct = sorted.length > 0 ? round2(rangePctSum / sorted.length) : 0;
  return {
    timeframe,
    points: sorted.length,
    start: String(first.t || ""),
    end: String(last.t || ""),
    open: round2(Number(first.o) || firstClose),
    close: round2(lastClose),
    high: Number.isFinite(high) ? round2(high) : round2(lastClose),
    low: Number.isFinite(low) ? round2(low) : round2(lastClose),
    changePct,
    avgRangePct,
    closeSeries: sorted.slice(-20).map((row) => round2(Number(row.c) || lastClose)),
  };
}

function buildPositionContextForAi(symbol, quote) {
  const shares = Number(state.positions[symbol]?.shares || 0);
  const avgCost = Number(state.positions[symbol]?.avgCost || 0);
  const mark = Number(quote?.price || 0);
  if (!Number.isFinite(shares) || shares <= 0 || !Number.isFinite(avgCost) || avgCost <= 0) {
    return {
      shares: 0,
      avgCost: null,
      marketPrice: Number.isFinite(mark) && mark > 0 ? round2(mark) : null,
      marketValue: 0,
      unrealizedPnl: 0,
      unrealizedPct: 0,
    };
  }

  const marketPrice = Number.isFinite(mark) && mark > 0 ? mark : avgCost;
  const marketValue = round2(shares * marketPrice);
  const costBasis = shares * avgCost;
  const unrealizedPnl = round2((marketPrice - avgCost) * shares);
  const unrealizedPct = costBasis > 0 ? round2((unrealizedPnl / costBasis) * 100) : 0;

  return {
    shares: Math.floor(shares),
    avgCost: round2(avgCost),
    marketPrice: round2(marketPrice),
    marketValue,
    unrealizedPnl,
    unrealizedPct,
  };
}

async function fetchCompanyInfo(symbol) {
  const normalized = normalizeSymbolInput(symbol);
  if (!normalized) return null;

  const endpoint = `${API_COMPANY_URL}?symbol=${encodeURIComponent(normalized)}`;
  const response = await fetch(endpoint, {
    headers: {
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    let details = "";
    try {
      const body = await response.json();
      if (body && typeof body === "object" && typeof body.error === "string") {
        details = body.error;
      } else if (body && typeof body === "object" && typeof body.message === "string") {
        details = body.message;
      }
    } catch {
      details = "";
    }
    throw new Error(
      details
        ? `Company API returned ${response.status}: ${details}`
        : `Company API returned ${response.status}.`
    );
  }

  const payload = await response.json();
  if (!payload || typeof payload !== "object" || !payload.company || typeof payload.company !== "object") {
    throw new Error("Company API returned an unexpected payload.");
  }

  const company = payload.company;
  return {
    symbol: normalizeSymbolInput(company.symbol) || normalized,
    name: String(company.name || "").trim(),
    exchange: String(company.exchange || "").trim(),
    industry: String(company.industry || "").trim(),
    country: String(company.country || "").trim(),
    currency: String(company.currency || "").trim(),
    ipo: String(company.ipo || "").trim(),
    phone: String(company.phone || "").trim(),
    weburl: String(company.weburl || "").trim(),
    logo: String(company.logo || "").trim(),
    marketCapitalization:
      typeof company.marketCapitalization === "number" && Number.isFinite(company.marketCapitalization)
        ? company.marketCapitalization
        : null,
    shareOutstanding:
      typeof company.shareOutstanding === "number" && Number.isFinite(company.shareOutstanding)
        ? company.shareOutstanding
        : null,
  };
}

function buildAiCompanyInfoLine(symbol, company) {
  const status = companyInfoLoadStatusBySymbol[symbol];
  const error = companyInfoErrorBySymbol[symbol];
  if (company) {
    const exchange = company.exchange || "N/A";
    const industry = company.industry || "N/A";
    const country = company.country || "N/A";
    const currency = company.currency || "N/A";
    const ipo = company.ipo || "N/A";
    const marketCap = formatOptionalNumber(company.marketCapitalization, 2);
    const shares = formatOptionalNumber(company.shareOutstanding, 2);
    return [
      `${company.name || symbol} (${symbol})`,
      `Exchange: ${exchange} | Industry: ${industry}`,
      `Country: ${country} | Currency: ${currency} | IPO: ${ipo}`,
      `Market Cap (M): ${marketCap} | Shares Outstanding (M): ${shares}`,
    ].join("\n");
  }
  if (status === "loading") {
    return "Loading company profile from Finnhub...";
  }
  if (status === "unavailable") {
    return error ? `Company profile unavailable: ${error}` : "Company profile unavailable.";
  }
  return "Company profile not loaded yet.";
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

  const signal = derivePositionSignal(symbol, profile);

  if (signal.tradeSide === "buy") {
    const allocation = clamp(Number(profile.buyCashPct || 0.08), 0.01, 0.35);
    const adjustedAllocation = signal.label === "ADD" ? clamp(allocation * 0.6, 0.01, 0.25) : allocation;
    const label = signal.label === "ADD" ? "ADD" : "BUY";
    const adjustedBudget = round2(state.cash * adjustedAllocation);
    const shares = Math.floor(adjustedBudget / quote.price);
    if (shares <= 0) {
      return {
        canApply: false,
        side: "buy",
        shares: 0,
        message: `${label} signal, but cash is too low for 1 share at ${fmtMoney(quote.price)}.`,
      };
    }

    return {
      canApply: true,
      side: "buy",
      verb: label,
      shares,
      message: `${label} ${shares} shares (~${fmtMoney(
        shares * quote.price
      )}, ${Math.round(adjustedAllocation * 100)}% of cash).`,
    };
  }

  if (signal.tradeSide === "sell") {
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
      verb: "SELL",
      shares,
      message: `SELL ${shares} shares (~${fmtMoney(
        shares * quote.price
      )}, trim ${Math.round(trimPct * 100)}% of position).`,
    };
  }

  if (signal.label === "WATCH") {
    return {
      canApply: false,
      side: "buy",
      shares: 0,
      message: "WATCH. No current position. Wait for a BUY setup before entering.",
    };
  }

  return {
    canApply: false,
    side: "buy",
    shares: 0,
    message: "HOLD. No position size change recommended right now.",
  };
}

function derivePositionSignal(symbol, profile) {
  const settings = getAiSettings();
  const action = normalizeAiAction(profile?.action);
  const heldShares = Number(state.positions[symbol]?.shares || 0);
  const hasPosition = heldShares > 0;
  const confidence = clamp(Math.round(Number(profile?.confidence) || 0), 0, 100);
  const sentiment = clamp(Math.round(Number(profile?.sentiment) || 0), -100, 100);
  const isStrongAddSignal =
    action === "BUY" &&
    hasPosition &&
    confidence >= settings.addConfidenceMin &&
    sentiment >= settings.addSentimentMin;
  const isStrongSellSignal =
    action === "SELL" &&
    hasPosition &&
    confidence >= settings.sellConfidenceMin &&
    sentiment <= settings.sellSentimentMax;

  if (!hasPosition) {
    if (action === "BUY") {
      return {
        label: "BUY",
        tone: "buy",
        tradeSide: "buy",
      };
    }
    return {
      label: "WATCH",
      tone: "watch",
      tradeSide: null,
    };
  }

  if (isStrongSellSignal) {
    return {
      label: "SELL",
      tone: "sell",
      tradeSide: "sell",
    };
  }

  if (isStrongAddSignal) {
    return {
      label: "ADD",
      tone: "buy",
      tradeSide: "buy",
    };
  }

  return {
    label: "HOLD",
    tone: "hold",
    tradeSide: null,
  };
}

function buildAiResearchText(symbol, quote, researchEntry) {
  const price = quote ? fmtMoney(quote.price) : "N/A";
  const lastClose = quote ? fmtMoney(quote.lastClose) : "N/A";
  if (researchEntry?.profile?.brief) {
    return [
      researchEntry.profile.brief,
      "",
      `Snapshot price: ${price} | Prior close: ${lastClose}`,
      `Model: ${researchEntry.model} | Horizon: ${researchEntry.horizon} | Updated: ${stampDateTime(
        researchEntry.fetchedAt
      )}`,
    ].join("\n");
  }

  return [
    `${symbol} placeholder AI snapshot`,
    `Current price: ${price} | Prior close: ${lastClose}`,
    "",
    "No cached OpenAI research for the current settings yet.",
    "Click Update to fetch sentiment and an AI brief.",
    "",
    `Selected horizon: ${getAiSettings().horizon}`,
  ].join("\n");
}

function buildNoLiveDataResearchText(symbol) {
  return [
    `${symbol} AI research paused`,
    "",
    "No live Alpaca quote feed is available.",
    "AI research requests are blocked while prices are simulated.",
    "",
    "Reconnect live data and then click Update.",
  ].join("\n");
}

function replaceResearchUrlsWithSourceTokens(text) {
  const raw = String(text || "");
  const urls = [];
  const urlIndexByValue = new Map();
  const replacedText = raw.replace(/https?:\/\/[^\s<>"']+/gi, (rawUrl) => {
    const clean = normalizeResearchSourceUrl(rawUrl);
    if (!clean) return rawUrl;
    let index = urlIndexByValue.get(clean);
    if (!index) {
      if (urls.length >= 8) return "(source)";
      urls.push(clean);
      index = urls.length;
      urlIndexByValue.set(clean, index);
    }
    return `(source ${index})`;
  });
  return {
    text: replacedText,
    urls,
  };
}

function normalizeResearchSourceUrl(rawUrl) {
  const trimmed = String(rawUrl || "")
    .trim()
    .replace(/[),.;:!?]+$/g, "");
  if (!trimmed) return "";
  try {
    const parsed = new URL(trimmed);
    const protocol = String(parsed.protocol || "").toLowerCase();
    if (protocol !== "http:" && protocol !== "https:") return "";
    return parsed.toString();
  } catch {
    return "";
  }
}

function renderAiResearchSources(urls) {
  if (!els.aiResearchSources) return;
  const safeUrls = Array.isArray(urls) ? urls.filter(Boolean) : [];
  if (safeUrls.length === 0) {
    els.aiResearchSources.textContent = "Sources: --";
    return;
  }
  const links = safeUrls
    .map(
      (url, index) =>
        `<a href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">(source ${index + 1})</a>`
    )
    .join(" ");
  els.aiResearchSources.innerHTML = `Sources: ${links}`;
}

function applyAiRecommendationToTicket() {
  const symbol = state.selectedSymbol;
  const recommendation = currentAiRecommendation;
  if (!symbol || !recommendation) {
    setTradeMessage("Run AI Update first to generate a recommendation.", "down");
    return;
  }
  if (!recommendation.canApply) {
    setTradeMessage("AI recommendation is not actionable right now.", "down");
    return;
  }

  const applied = applyRecommendationToTicket(
    {
      symbol,
      side: recommendation.side,
      shares: recommendation.shares,
      verb: recommendation.verb || recommendation.side.toUpperCase(),
    },
    "AI recommendation"
  );
  if (!applied) return;
}

function applyRecommendationToTicket(recommendation, sourceLabel) {
  const symbol = normalizeSymbolInput(recommendation?.symbol || state.selectedSymbol);
  const side = String(recommendation?.side || "").toLowerCase();
  const shares = Number(recommendation?.shares);
  if (!symbol || !state.prices[symbol]) {
    setTradeMessage(`${sourceLabel} symbol is not available in your watchlist quotes.`, "down");
    return false;
  }
  if (!(side === "buy" || side === "sell")) {
    setTradeMessage(`${sourceLabel} is not a trade action.`, "down");
    return false;
  }
  if (!Number.isInteger(shares) || shares <= 0) {
    setTradeMessage(`${sourceLabel} has an invalid share size.`, "down");
    return false;
  }

  state.selectedSymbol = symbol;
  setMarketPageForSymbol(symbol);
  els.symbolSelect.value = symbol;
  els.sideSelect.value = side;
  els.sharesInput.value = String(shares);
  persistState();
  renderAll();
  const verb = recommendation.verb || side.toUpperCase();
  setTradeMessage(`Applied ${sourceLabel}: ${verb} ${shares} ${symbol}.`, side === "buy" ? "up" : "down");
  return true;
}

function executeRecommendationTrade(recommendation, sourceLabel) {
  const symbol = normalizeSymbolInput(recommendation?.symbol || state.selectedSymbol);
  const side = String(recommendation?.side || "").toLowerCase();
  const shares = Number(recommendation?.shares);
  if (!symbol || !state.prices[symbol]) {
    setTradeMessage(`${sourceLabel} could not execute: quote unavailable for ${symbol || "symbol"}.`, "down");
    return false;
  }
  if (!Number.isInteger(shares) || shares <= 0 || !(side === "buy" || side === "sell")) {
    setTradeMessage(`${sourceLabel} could not execute: invalid action payload.`, "down");
    return false;
  }

  state.selectedSymbol = symbol;
  setMarketPageForSymbol(symbol);
  els.symbolSelect.value = symbol;
  els.sideSelect.value = side;
  els.sharesInput.value = String(shares);
  const quote = state.prices[symbol];
  if (side === "buy") {
    executeBuy(symbol, shares, quote.price);
  } else {
    executeSell(symbol, shares, quote.price);
  }
  return true;
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
  return candles.slice(-mode.displayLimit);
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
  const symbols = Object.keys(state.positions).sort((a, b) => a.localeCompare(b));
  const pendingOrders = normalizePendingOrders(state.pendingOrders);
  if (symbols.length === 0 && pendingOrders.length === 0) {
    els.positionsBody.innerHTML = `
      <tr>
        <td colspan="6">No positions yet. Place a buy trade to get started.</td>
      </tr>
    `;
    return;
  }

  const positionRows = symbols.map((symbol) => {
    const pos = state.positions[symbol];
    const quote = state.prices[symbol];
    const markPrice =
      quote && Number.isFinite(quote.price) && quote.price > 0 ? quote.price : pos.avgCost;
    const marketValue = round2(pos.shares * markPrice);
    const unrealized = round2((markPrice - pos.avgCost) * pos.shares);
    const cls = unrealized >= 0 ? "up" : "down";

    return `
      <tr>
        <td class="ticker">${symbol}</td>
        <td>${pos.shares}</td>
        <td>${fmtMoney(pos.avgCost)}</td>
        <td>${fmtMoney(marketValue)}</td>
        <td class="${cls}">${signedMoney(unrealized)}</td>
        <td>
          <button
            type="button"
            class="table-action-btn position-sell-btn"
            data-symbol="${symbol}"
            data-shares="${pos.shares}"
          >
            Sell
          </button>
        </td>
      </tr>
    `;
  });

  const pendingRows = pendingOrders.map((order) => {
    const qtyLabel = order.filledQty > 0 ? `${order.filledQty}/${order.qty}` : String(order.qty);
    const statusText = String(order.status || "pending")
      .split(/[_-]+/g)
      .filter(Boolean)
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
      .join(" ");
    const sideText = order.side.toUpperCase();
    return `
      <tr class="pending-order-row">
        <td class="ticker">${order.symbol}</td>
        <td>${qtyLabel}</td>
        <td>--</td>
        <td>--</td>
        <td>--</td>
        <td><span class="pending-order-status">${statusText} ${sideText}</span></td>
      </tr>
    `;
  });

  els.positionsBody.innerHTML = [...positionRows, ...pendingRows].join("");
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
      const price =
        quote && Number.isFinite(quote.price) && quote.price > 0 ? quote.price : pos.avgCost;
      return sum + pos.shares * price;
    }, 0)
  );
}

function calcUnrealizedPnl() {
  return round2(
    Object.entries(state.positions).reduce((sum, [symbol, pos]) => {
      const quote = state.prices[symbol];
      const price =
        quote && Number.isFinite(quote.price) && quote.price > 0 ? quote.price : pos.avgCost;
      return sum + (price - pos.avgCost) * pos.shares;
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

function getAiSettings() {
  const normalized = normalizeAiSettings(state.aiSettings);
  state.aiSettings = normalized;
  return normalized;
}

function normalizeAiProvider(raw) {
  const value = String(raw || "").trim().toLowerCase();
  return AI_PROVIDERS.includes(value) ? value : DEFAULT_AI_SETTINGS.provider;
}

function normalizeAiModel(raw) {
  const value = String(raw || "").trim();
  return AI_MODELS.includes(value) ? value : DEFAULT_AI_SETTINGS.model;
}

function normalizeAiHorizon(raw) {
  const value = String(raw || "").trim().toLowerCase();
  return AI_HORIZONS.includes(value) ? value : DEFAULT_AI_SETTINGS.horizon;
}

function normalizeAiSettings(input) {
  const safe = input && typeof input === "object" ? input : {};
  return {
    provider: normalizeAiProvider(safe.provider),
    model: normalizeAiModel(safe.model),
    horizon: normalizeAiHorizon(safe.horizon),
    aiResearchAutoRefreshEnabled:
      typeof safe.aiResearchAutoRefreshEnabled === "boolean"
        ? safe.aiResearchAutoRefreshEnabled
        : DEFAULT_AI_SETTINGS.aiResearchAutoRefreshEnabled,
    aiResearchAutoRefreshMins: normalizeThreshold(
      safe.aiResearchAutoRefreshMins,
      15,
      720,
      DEFAULT_AI_SETTINGS.aiResearchAutoRefreshMins
    ),
    autobotIntervalMins: normalizeThreshold(
      safe.autobotIntervalMins,
      5,
      240,
      DEFAULT_AI_SETTINGS.autobotIntervalMins
    ),
    maxTradesPerCycle: normalizeThreshold(
      safe.maxTradesPerCycle,
      1,
      5,
      DEFAULT_AI_SETTINGS.maxTradesPerCycle
    ),
    addConfidenceMin: normalizeThreshold(
      safe.addConfidenceMin,
      50,
      95,
      DEFAULT_AI_SETTINGS.addConfidenceMin
    ),
    addSentimentMin: normalizeThreshold(
      safe.addSentimentMin,
      0,
      90,
      DEFAULT_AI_SETTINGS.addSentimentMin
    ),
    sellConfidenceMin: normalizeThreshold(
      safe.sellConfidenceMin,
      50,
      95,
      DEFAULT_AI_SETTINGS.sellConfidenceMin
    ),
    sellSentimentMax: normalizeThreshold(
      safe.sellSentimentMax,
      -60,
      20,
      DEFAULT_AI_SETTINGS.sellSentimentMax
    ),
  };
}

function normalizeThreshold(raw, min, max, fallback) {
  const value = Number(raw);
  if (!Number.isFinite(value)) return fallback;
  return clamp(Math.round(value), min, max);
}

function normalizeAutobotState(input) {
  const safe = input && typeof input === "object" ? input : {};
  return {
    enabled: Boolean(safe.enabled),
    lastRunAt:
      typeof safe.lastRunAt === "number" && Number.isFinite(safe.lastRunAt) && safe.lastRunAt > 0
        ? safe.lastRunAt
        : 0,
    latestThought: truncateText(safe.latestThought || "", 900),
    latestRecommendation: normalizeAutobotRecommendation(safe.latestRecommendation),
    latestAutoAction: normalizeAutobotAction(safe.latestAutoAction),
    lastStatus: truncateText(safe.lastStatus || DEFAULT_AUTOBOT_STATE.lastStatus, 180),
    lastError: truncateText(safe.lastError || "", 220),
  };
}

function normalizeMarketScoutState(input) {
  const safe = input && typeof input === "object" ? input : {};
  const checkedAtBySymbol = {};
  const incomingChecked =
    safe.checkedAtBySymbol && typeof safe.checkedAtBySymbol === "object"
      ? safe.checkedAtBySymbol
      : {};
  for (const [symbol, ts] of Object.entries(incomingChecked)) {
    const clean = normalizeSymbolInput(symbol);
    const stamp = Number(ts);
    if (!clean || !Number.isFinite(stamp) || stamp <= 0) continue;
    checkedAtBySymbol[clean] = Math.floor(stamp);
  }
  const recentMessages = Array.isArray(safe.recentMessages)
    ? safe.recentMessages.map((msg) => truncateText(msg, 260)).filter(Boolean).slice(0, 40)
    : [];
  const createdAtRaw = Number(safe.createdAt);
  const createdAt = Number.isFinite(createdAtRaw) && createdAtRaw > 0 ? Math.floor(createdAtRaw) : 0;
  return {
    enabled: typeof safe.enabled === "boolean" ? safe.enabled : DEFAULT_MARKET_SCOUT_STATE.enabled,
    intervalMins: normalizeThreshold(
      safe.intervalMins,
      1,
      120,
      DEFAULT_MARKET_SCOUT_STATE.intervalMins
    ),
    lastRunAt: normalizeThreshold(safe.lastRunAt, 0, Number.MAX_SAFE_INTEGER, 0),
    cursor: normalizeThreshold(safe.cursor, 0, Number.MAX_SAFE_INTEGER, 0),
    lastSymbol: normalizeSymbolInput(safe.lastSymbol),
    latestMessage: truncateText(safe.latestMessage || DEFAULT_MARKET_SCOUT_STATE.latestMessage, 260),
    recentMessages,
    checkedAtBySymbol,
    createdAt,
  };
}

function normalizeAutobotAction(input) {
  if (!input || typeof input !== "object") return null;
  const type = normalizeAiAction(input.type);
  const symbol = normalizeSymbolInput(input.symbol);
  const sharesRaw = Number(input.shares);
  const shares = Number.isFinite(sharesRaw) ? Math.max(0, Math.floor(sharesRaw)) : 0;
  const reason = truncateText(input.reason || "", 320);
  const executed = Boolean(input.executed);
  const atRaw = Number(input.at);
  const at = Number.isFinite(atRaw) && atRaw > 0 ? Math.floor(atRaw) : 0;
  return {
    type,
    symbol,
    shares: type === "HOLD" ? 0 : shares,
    reason: reason || "No reason provided.",
    executed,
    at,
  };
}

function normalizeAutobotRecommendation(input) {
  if (!input || typeof input !== "object") return null;
  const symbol = normalizeSymbolInput(input.symbol);
  const sideRaw = String(input.side || input.action || "").trim().toLowerCase();
  const side =
    sideRaw === "buy" || sideRaw === "sell" || sideRaw === "hold"
      ? sideRaw
      : normalizeAiAction(input.action) === "BUY"
        ? "buy"
        : normalizeAiAction(input.action) === "SELL"
          ? "sell"
          : "hold";
  const sharesRaw = Number(input.shares);
  const shares = Number.isFinite(sharesRaw) ? Math.max(0, Math.floor(sharesRaw)) : 0;
  const reason = truncateText(input.reason || "", 320);
  const thought = truncateText(input.thought || "", 900);

  if (!symbol) {
    return {
      symbol: "",
      side: "hold",
      shares: 0,
      reason: reason || "No symbol provided.",
      thought,
    };
  }

  return {
    symbol,
    side,
    shares: side === "hold" ? 0 : shares,
    reason: reason || "No reason provided.",
    thought,
  };
}

function buildAiCacheKey(symbol, settings) {
  const normalizedSymbol = normalizeSymbolInput(symbol);
  const safeSettings = normalizeAiSettings(settings);
  return `${normalizedSymbol}|${safeSettings.provider}|${safeSettings.model}|${safeSettings.horizon}`;
}

function getAiResearchNextDueAt(symbol, settings) {
  const normalizedSymbol = normalizeSymbolInput(symbol);
  if (!normalizedSymbol) return Number.POSITIVE_INFINITY;
  const safeSettings = normalizeAiSettings(settings);
  const intervalMs = safeSettings.aiResearchAutoRefreshMins * 60 * 1000;
  const meta = ensureAiRefreshMetaForSymbol(normalizedSymbol);
  const entry = getAiResearchEntry(normalizedSymbol, safeSettings);
  const lastUpdatedAt = Math.max(Number(meta.lastUpdatedAt || 0), Number(entry?.fetchedAt || 0));
  const anchor = lastUpdatedAt > 0 ? lastUpdatedAt : Number(meta.addedAt || Date.now());
  return anchor + intervalMs;
}

function getAiResearchEntry(symbol, settings) {
  const normalizedSymbol = normalizeSymbolInput(symbol);
  if (!normalizedSymbol) return null;
  const key = buildAiCacheKey(normalizedSymbol, settings);
  const entry = state.aiResearchCache && typeof state.aiResearchCache === "object"
    ? state.aiResearchCache[key]
    : null;
  if (!entry || typeof entry !== "object") return null;
  const fetchedAt = Number(entry.fetchedAt);
  if (!Number.isFinite(fetchedAt) || fetchedAt <= 0) return null;

  return {
    symbol: normalizeSymbolInput(entry.symbol) || normalizedSymbol,
    provider: normalizeAiProvider(entry.provider),
    model: normalizeAiModel(entry.model),
    horizon: normalizeAiHorizon(entry.horizon),
    fetchedAt,
    profile: normalizeIncomingAiProfile(entry.profile),
  };
}

function getAiResearchTtlMs(horizon) {
  return AI_RESEARCH_TTL_MS_BY_HORIZON[normalizeAiHorizon(horizon)] || AI_RESEARCH_TTL_MS_BY_HORIZON.swing;
}

function isAiResearchEntryStale(entry) {
  if (!entry) return true;
  return Date.now() - entry.fetchedAt > getAiResearchTtlMs(entry.horizon);
}

function upsertAiResearchEntry(symbol, settings, profile) {
  const normalizedSymbol = normalizeSymbolInput(symbol);
  if (!normalizedSymbol) return;
  const safeSettings = normalizeAiSettings(settings);
  if (!state.aiResearchCache || typeof state.aiResearchCache !== "object") {
    state.aiResearchCache = {};
  }

  const key = buildAiCacheKey(normalizedSymbol, safeSettings);
  const fetchedAt = Date.now();
  state.aiResearchCache[key] = {
    symbol: normalizedSymbol,
    provider: safeSettings.provider,
    model: safeSettings.model,
    horizon: safeSettings.horizon,
    fetchedAt,
    profile: normalizeIncomingAiProfile(profile),
  };
  markAiResearchRefreshSuccess(normalizedSymbol, fetchedAt);
  state.aiResearchCache = normalizeAiResearchCache(state.aiResearchCache);
}

function normalizeAiResearchCache(input) {
  if (!input || typeof input !== "object") return {};
  const rows = [];
  for (const value of Object.values(input)) {
    if (!value || typeof value !== "object") continue;
    const symbol = normalizeSymbolInput(value.symbol);
    if (!symbol) continue;
    const provider = normalizeAiProvider(value.provider);
    const model = normalizeAiModel(value.model);
    const horizon = normalizeAiHorizon(value.horizon);
    const fetchedAt = Number(value.fetchedAt);
    if (!Number.isFinite(fetchedAt) || fetchedAt <= 0) continue;
    const profile = normalizeIncomingAiProfile(value.profile);
    rows.push({
      key: buildAiCacheKey(symbol, { provider, model, horizon }),
      value: {
        symbol,
        provider,
        model,
        horizon,
        fetchedAt,
        profile,
      },
    });
  }

  rows.sort((a, b) => b.value.fetchedAt - a.value.fetchedAt);
  const out = {};
  for (const row of rows.slice(0, MAX_AI_CACHE_ENTRIES)) {
    out[row.key] = row.value;
  }
  return out;
}

function normalizeAiRefreshMeta(input, watchlist) {
  const safe = input && typeof input === "object" ? input : {};
  const now = Date.now();
  const defaultIntervalMs = DEFAULT_AI_SETTINGS.aiResearchAutoRefreshMins * 60 * 1000;
  const out = {};
  for (const rawSymbol of watchlist || []) {
    const symbol = normalizeSymbolInput(rawSymbol);
    if (!symbol) continue;
    const entry = normalizeAiRefreshMetaEntry(safe[symbol]);
    const defaultAddedAt = Math.max(1, now - getSymbolRefreshOffsetMs(symbol, defaultIntervalMs));
    out[symbol] = {
      addedAt: entry?.addedAt || defaultAddedAt,
      lastUpdatedAt: entry?.lastUpdatedAt || 0,
      lastAttemptAt: entry?.lastAttemptAt || 0,
    };
  }
  return out;
}

function normalizeAiRefreshMetaEntry(input) {
  if (!input || typeof input !== "object") return null;
  const addedAtRaw = Number(input.addedAt);
  const lastUpdatedAtRaw = Number(input.lastUpdatedAt);
  const lastAttemptAtRaw = Number(input.lastAttemptAt);
  const addedAt = Number.isFinite(addedAtRaw) && addedAtRaw > 0 ? Math.floor(addedAtRaw) : 0;
  const lastUpdatedAt =
    Number.isFinite(lastUpdatedAtRaw) && lastUpdatedAtRaw > 0 ? Math.floor(lastUpdatedAtRaw) : 0;
  const lastAttemptAt =
    Number.isFinite(lastAttemptAtRaw) && lastAttemptAtRaw > 0 ? Math.floor(lastAttemptAtRaw) : 0;
  if (addedAt <= 0 && lastUpdatedAt <= 0 && lastAttemptAt <= 0) return null;
  return {
    addedAt,
    lastUpdatedAt,
    lastAttemptAt,
  };
}

function getSymbolRefreshOffsetMs(symbol, intervalMs) {
  if (!symbol || !Number.isFinite(intervalMs) || intervalMs <= 1) return 0;
  let hash = 0;
  for (let i = 0; i < symbol.length; i += 1) {
    hash = (hash * 31 + symbol.charCodeAt(i)) >>> 0;
  }
  return hash % Math.floor(intervalMs);
}

function ensureAiRefreshMetaForWatchlist() {
  const watchlist = Array.isArray(state.watchlist)
    ? state.watchlist.map((symbol) => normalizeSymbolInput(symbol)).filter(Boolean)
    : [];
  state.aiRefreshMeta = normalizeAiRefreshMeta(state.aiRefreshMeta, watchlist);
}

function ensureAiRefreshMetaForSymbol(symbol, preferredAddedAt = 0) {
  const normalizedSymbol = normalizeSymbolInput(symbol);
  if (!normalizedSymbol) {
    return {
      addedAt: 0,
      lastUpdatedAt: 0,
      lastAttemptAt: 0,
    };
  }
  if (!state.aiRefreshMeta || typeof state.aiRefreshMeta !== "object") {
    state.aiRefreshMeta = {};
  }
  const watchlist = Array.isArray(state.watchlist) ? state.watchlist : [];
  if (!watchlist.includes(normalizedSymbol)) {
    return {
      addedAt: 0,
      lastUpdatedAt: 0,
      lastAttemptAt: 0,
    };
  }
  const existing = normalizeAiRefreshMetaEntry(state.aiRefreshMeta[normalizedSymbol]);
  const preferredAtNum = Number(preferredAddedAt);
  const preferredAt =
    Number.isFinite(preferredAtNum) && preferredAtNum > 0 ? Math.floor(preferredAtNum) : 0;
  const defaultIntervalMs = DEFAULT_AI_SETTINGS.aiResearchAutoRefreshMins * 60 * 1000;
  const fallbackAddedAt = preferredAt || Math.max(1, Date.now() - getSymbolRefreshOffsetMs(normalizedSymbol, defaultIntervalMs));
  const next = {
    addedAt: existing?.addedAt || fallbackAddedAt,
    lastUpdatedAt: existing?.lastUpdatedAt || 0,
    lastAttemptAt: existing?.lastAttemptAt || 0,
  };
  if (preferredAt > 0 && (next.addedAt <= 0 || preferredAt < next.addedAt)) {
    next.addedAt = preferredAt;
  }
  state.aiRefreshMeta[normalizedSymbol] = next;
  return next;
}

function markAiResearchRefreshAttempt(symbol, at = Date.now()) {
  const normalizedSymbol = normalizeSymbolInput(symbol);
  if (!normalizedSymbol) return;
  const meta = ensureAiRefreshMetaForSymbol(normalizedSymbol);
  const time = Number.isFinite(Number(at)) && Number(at) > 0 ? Math.floor(Number(at)) : Date.now();
  state.aiRefreshMeta[normalizedSymbol] = {
    ...meta,
    lastAttemptAt: time,
  };
}

function markAiResearchRefreshSuccess(symbol, at = Date.now()) {
  const normalizedSymbol = normalizeSymbolInput(symbol);
  if (!normalizedSymbol) return;
  const meta = ensureAiRefreshMetaForSymbol(normalizedSymbol);
  const time = Number.isFinite(Number(at)) && Number(at) > 0 ? Math.floor(Number(at)) : Date.now();
  state.aiRefreshMeta[normalizedSymbol] = {
    ...meta,
    lastUpdatedAt: time,
    lastAttemptAt: time,
  };
}

function normalizeIncomingAiProfile(raw) {
  const safe = raw && typeof raw === "object" ? raw : {};
  const sentimentValue = Number(safe.sentiment);
  const confidenceValue = Number(safe.confidence);
  const buyCashPctValue = Number(safe.buyCashPct);
  const trimPositionPctValue = Number(safe.trimPositionPct);
  return {
    sentiment: clamp(Math.round(Number.isFinite(sentimentValue) ? sentimentValue : 0), -100, 100),
    confidence: clamp(
      Math.round(Number.isFinite(confidenceValue) ? confidenceValue : 50),
      0,
      100
    ),
    action: normalizeAiAction(safe.action),
    buyCashPct: clamp(Number.isFinite(buyCashPctValue) ? buyCashPctValue : 0.08, 0.01, 0.5),
    trimPositionPct: clamp(
      Number.isFinite(trimPositionPctValue) ? trimPositionPctValue : 0.25,
      0.05,
      1
    ),
    thesis: cleanAiText(safe.thesis, 600, "No thesis provided."),
    catalyst: cleanAiText(safe.catalyst, 420, "No catalyst provided."),
    risk: cleanAiText(safe.risk, 420, "No risk provided."),
    brief: cleanAiText(safe.brief, 1800, ""),
  };
}

function normalizeAiAction(raw) {
  const action = String(raw || "").trim().toUpperCase();
  if (action === "BUY" || action === "SELL" || action === "HOLD") return action;
  return "HOLD";
}

function cleanAiText(value, maxLength, fallback) {
  const text = String(value || "").trim();
  if (!text) return fallback;
  if (text.length <= maxLength) return text;
  return `${text.slice(0, maxLength - 1)}...`;
}

function getCachedCompanyInfo(symbol) {
  const normalizedSymbol = normalizeSymbolInput(symbol);
  if (!normalizedSymbol) return null;
  const cache = state.companyCache && typeof state.companyCache === "object" ? state.companyCache : {};
  const entry = cache[normalizedSymbol];
  if (!entry || typeof entry !== "object") return null;
  const fetchedAt = Number(entry.fetchedAt);
  if (!Number.isFinite(fetchedAt) || fetchedAt <= 0) return null;
  if (Date.now() - fetchedAt > COMPANY_INFO_TTL_MS) return null;
  const company = normalizeCompanyInfo(entry.company);
  if (!company) return null;
  return { fetchedAt, company };
}

function setCachedCompanyInfo(symbol, company) {
  const normalizedSymbol = normalizeSymbolInput(symbol);
  if (!normalizedSymbol) return;
  const normalizedCompany = normalizeCompanyInfo(company);
  if (!normalizedCompany) return;

  if (!state.companyCache || typeof state.companyCache !== "object") {
    state.companyCache = {};
  }
  state.companyCache[normalizedSymbol] = {
    fetchedAt: Date.now(),
    company: normalizedCompany,
  };
  state.companyCache = normalizeCompanyCache(state.companyCache);
}

function normalizeCompanyCache(input) {
  if (!input || typeof input !== "object") return {};
  const rows = [];
  for (const [rawSymbol, value] of Object.entries(input)) {
    const symbol = normalizeSymbolInput(rawSymbol);
    if (!symbol || !value || typeof value !== "object") continue;
    const fetchedAt = Number(value.fetchedAt);
    if (!Number.isFinite(fetchedAt) || fetchedAt <= 0) continue;
    if (Date.now() - fetchedAt > COMPANY_INFO_TTL_MS) continue;
    const company = normalizeCompanyInfo(value.company);
    if (!company) continue;
    rows.push({
      symbol,
      fetchedAt,
      company,
    });
  }

  rows.sort((a, b) => b.fetchedAt - a.fetchedAt);
  const out = {};
  for (const row of rows.slice(0, MAX_COMPANY_CACHE_ENTRIES)) {
    out[row.symbol] = {
      fetchedAt: row.fetchedAt,
      company: row.company,
    };
  }
  return out;
}

function hydrateCompanyRuntimeFromState() {
  state.companyCache = normalizeCompanyCache(state.companyCache);
  for (const [symbol, entry] of Object.entries(state.companyCache)) {
    const normalizedSymbol = normalizeSymbolInput(symbol);
    if (!normalizedSymbol || !entry || typeof entry !== "object") continue;
    const company = normalizeCompanyInfo(entry.company);
    if (!company) continue;
    companyInfoBySymbol[normalizedSymbol] = company;
    companyInfoLoadStatusBySymbol[normalizedSymbol] = "loaded";
    delete companyInfoErrorBySymbol[normalizedSymbol];
  }
}

function normalizeCompanyInfo(raw) {
  if (!raw || typeof raw !== "object") return null;
  return {
    symbol: normalizeSymbolInput(raw.symbol),
    name: String(raw.name || "").trim(),
    exchange: String(raw.exchange || "").trim(),
    industry: String(raw.industry || "").trim(),
    country: String(raw.country || "").trim(),
    currency: String(raw.currency || "").trim(),
    ipo: String(raw.ipo || "").trim(),
    phone: String(raw.phone || "").trim(),
    weburl: String(raw.weburl || "").trim(),
    logo: String(raw.logo || "").trim(),
    marketCapitalization:
      typeof raw.marketCapitalization === "number" && Number.isFinite(raw.marketCapitalization)
        ? raw.marketCapitalization
        : null,
    shareOutstanding:
      typeof raw.shareOutstanding === "number" && Number.isFinite(raw.shareOutstanding)
        ? raw.shareOutstanding
        : null,
  };
}

function normalizePendingOrders(input) {
  if (!Array.isArray(input)) return [];
  return input
    .map((row) => {
      if (!row || typeof row !== "object") return null;
      const symbol = normalizeSymbolInput(row.symbol);
      const sideRaw = String(row.side || "").trim().toLowerCase();
      const side = sideRaw === "buy" || sideRaw === "sell" ? sideRaw : "";
      const qtyNum = Number(row.qty);
      const filledQtyNum = Number(row.filledQty);
      const qty = Number.isFinite(qtyNum) && qtyNum > 0 ? Math.floor(qtyNum) : 0;
      const filledQty = Number.isFinite(filledQtyNum) && filledQtyNum > 0 ? Math.floor(filledQtyNum) : 0;
      const statusRaw = String(row.status || "").trim().toLowerCase();
      const status = statusRaw || "pending";
      if (!symbol || !side || qty <= 0) return null;
      return {
        id: String(row.id || "").trim(),
        symbol,
        side,
        qty,
        filledQty,
        status,
        submittedAt: String(row.submittedAt || "").trim(),
      };
    })
    .filter(Boolean)
    .slice(0, 100);
}

function normalizeLockedWatchlistSymbols(input, watchlist) {
  const watchSet = new Set(
    (Array.isArray(watchlist) ? watchlist : [])
      .map((symbol) => normalizeSymbolInput(symbol))
      .filter(Boolean)
  );
  if (watchSet.size === 0) return [];
  return Array.from(
    new Set(
      (Array.isArray(input) ? input : [])
        .map((symbol) => normalizeSymbolInput(symbol))
        .filter((symbol) => symbol && watchSet.has(symbol))
    )
  );
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
  const persistedPositionSymbols =
    safe.positions && typeof safe.positions === "object"
      ? Object.keys(safe.positions).map((symbol) => normalizeSymbolInput(symbol)).filter(Boolean)
      : [];
  const persistedWatchlist = Array.isArray(safe.watchlist)
    ? safe.watchlist.map((symbol) => normalizeSymbolInput(symbol)).filter(Boolean)
    : [];
  const watchlist = Array.from(new Set([...persistedPositionSymbols, ...persistedWatchlist])).slice(
    0,
    MAX_TRACKED_SYMBOLS
  );
  if (watchlist.length === 0) {
    watchlist.push(...DEFAULT_SYMBOLS.slice(0, MAX_TRACKED_SYMBOLS));
  }

  const prices = {};
  for (const symbol of watchlist) {
    const incoming = safe.prices && safe.prices[symbol];
    const basePrice = getBasePrice(symbol);
    prices[symbol] = {
      price:
        typeof incoming?.price === "number" && incoming.price > 0
          ? round2(incoming.price)
          : basePrice,
      lastClose:
        typeof incoming?.lastClose === "number" && incoming.lastClose > 0
          ? round2(incoming.lastClose)
          : basePrice,
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

  const aiSettings = normalizeAiSettings(safe.aiSettings);
  const aiResearchCache = normalizeAiResearchCache(safe.aiResearchCache);
  const companyCache = normalizeCompanyCache(safe.companyCache);
  const aiRefreshMeta = normalizeAiRefreshMeta(safe.aiRefreshMeta, watchlist);
  const lockedWatchlistSymbols = normalizeLockedWatchlistSymbols(
    safe.lockedWatchlistSymbols || safe.wallet?.lockedSymbols,
    watchlist
  );
  const marketScout = normalizeMarketScoutState(safe.marketScout);
  const autobot = normalizeAutobotState(safe.autobot);
  const pendingOrders = normalizePendingOrders(safe.pendingOrders || safe.openOrders);

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
    chartScale: normalizeChartScale(safe.chartScale, fresh.chartScale),
    marketPage:
      typeof safe.marketPage === "number" && Number.isFinite(safe.marketPage) && safe.marketPage >= 0
        ? Math.floor(safe.marketPage)
        : 0,
    watchlist,
    lockedWatchlistSymbols,
    prices,
    positions,
    pendingOrders,
    transactions: Array.isArray(safe.transactions) ? safe.transactions.slice(0, 100) : [],
    backendRuntimeUpdatedAt:
      typeof safe.backendRuntimeUpdatedAt === "number" &&
      Number.isFinite(safe.backendRuntimeUpdatedAt) &&
      safe.backendRuntimeUpdatedAt > 0
        ? Math.floor(safe.backendRuntimeUpdatedAt)
        : 0,
    aiSettings,
    aiResearchCache,
    companyCache,
    aiRefreshMeta,
    marketScout,
    autobot,
  };
}

function normalizeChartScale(rawValue, fallback) {
  const value = typeof rawValue === "string" ? rawValue.toLowerCase() : "";
  if (value === "hours") return "mins";
  if (value === "months") return "days";
  if (value && CHART_MODES[value]) return value;
  return fallback;
}

function createInitialState() {
  const watchlist = [...DEFAULT_SYMBOLS];
  const prices = {};
  for (const symbol of watchlist) {
    const basePrice = getBasePrice(symbol);
    prices[symbol] = {
      price: basePrice,
      lastClose: basePrice,
    };
  }

  return {
    cash: STARTING_CASH,
    realizedPnl: 0,
    selectedSymbol: watchlist[0],
    chartScale: DEFAULT_CHART_SCALE,
    marketPage: 0,
    watchlist,
    lockedWatchlistSymbols: [],
    prices,
    positions: {},
    pendingOrders: [],
    transactions: [],
    backendRuntimeUpdatedAt: 0,
    aiSettings: { ...DEFAULT_AI_SETTINGS },
    aiResearchCache: {},
    companyCache: {},
    aiRefreshMeta: normalizeAiRefreshMeta({}, watchlist),
    marketScout: { ...DEFAULT_MARKET_SCOUT_STATE },
    autobot: { ...DEFAULT_AUTOBOT_STATE },
  };
}

function persistState() {
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
}

function markBackendSyncDirty() {
  backendSyncDirty = true;
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

function stampDateTime(value) {
  return new Date(value).toLocaleString([], {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
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

function formatOptionalNumber(value, digits = 2) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "N/A";
  return num.toFixed(digits);
}

function getErrorMessage(error) {
  if (error instanceof Error && error.message) return truncateText(error.message, 220);
  return "Unknown error loading company profile.";
}

function truncateText(value, maxLength) {
  const text = String(value || "").trim();
  if (text.length <= maxLength) return text;
  return `${text.slice(0, maxLength - 1)}...`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
