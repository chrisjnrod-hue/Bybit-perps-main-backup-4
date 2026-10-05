const { performance } = require('perf_hooks');
const dbModule = require('../db');
const bybit = require('./bybitRest');
const bybitWs = require('./bybitWs');
const config = require('../config');
const logger = require('pino')();
const Bottleneck = require('bottleneck');
const macdUtil = require('./macd');
const signalManager = require('./signalManager');
const notificationQueue = require('./notificationQueue');

const limiter = new Bottleneck({
  minTime: 50
});

const SEED_CONCURRENCY = Number(
  config.SEED_CONCURRENCY || 6
);

const FIVE_MINUTES_MS = 5 * 60 * 1000;

let isRunning = false;
let startupComplete = false;
let boundaryScanInProgress = false;

// Global de-dupe registry used across 5m mid-candle, MTF alignment, and root-TF reopen flows.
// Key format: symbol|root_tf|candle_open_time|signal_family
const processedEventKeys = new Map();

// Tracks startup-batch signals so the poller does not re-queue the same signal again
// through mid-candle / root-candle loops after the initial summary batch was sent.
const startupBatchSignalKeys = new Set();

// Cooldown cache for REST fallbacks during WS gaps (Key: symbol|tf, Value: timestamp)
const restFallbackCooldowns = new Map();

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms || 0);
  });
}

function normalizeRootTf(tf) {
  if (tf === null || tf === undefined) {
    return null;
  }

  const value = String(tf)
    .trim()
    .toUpperCase();

  if (value === '1D' || value === 'D') {
    return 'D';
  }

  if (value === '1H' || value === 'H') {
    return '60';
  }

  if (value === '4H') {
    return '240';
  }

  return value;
}

function buildRootTfs() {
  const raw = Array.isArray(config.ROOT_TFS)
    ? config.ROOT_TFS
    : ['60', '240', 'D'];

  const seen = new Set();
  const output = [];

  for (const timeframe of raw) {
    const normalized = normalizeRootTf(timeframe);

    if (!normalized || seen.has(normalized)) {
      continue;
    }

    seen.add(normalized);
    output.push(normalized);
  }

  return output.length > 0
    ? output
    : ['60', '240', 'D'];
}

function isUsdtSymbol(symbol) {
  const value = String(symbol || '')
    .toUpperCase();

  if (!value) {
    return false;
  }

  if (/USDT[QHUZ0-9]/.test(value.slice(-6))) {
    return false;
  }

  return /USDT(\.P)?$/.test(value);
}

function getLatestOpenTime(db, symbol, timeframe) {
  const row = db
    .prepare(
      `
        SELECT open_time
        FROM klines
        WHERE symbol = ?
          AND timeframe = ?
        ORDER BY open_time DESC
        LIMIT 1
      `
    )
    .get(symbol, timeframe);

  if (!row) {
    return null;
  }

  const openTime = Number(row.open_time);

  return Number.isFinite(openTime)
    ? openTime
    : null;
}

function getRootTfMs(tf) {
  const normalized = normalizeRootTf(tf);

  if (normalized === 'D') {
    return 24 * 60 * 60 * 1000;
  }

  const minutes = Number(normalized);

  if (Number.isFinite(minutes) && minutes > 0) {
    return minutes * 60 * 1000;
  }

  return FIVE_MINUTES_MS;
}

function getNextTimeframeBoundaryMs(tf, nowMs = Date.now()) {
  const tfMs = getRootTfMs(tf);

  const epochStart = Date.UTC(1970, 0, 1);
  const alignedFloor = Math.floor(
    (nowMs - epochStart) / tfMs
  ) * tfMs;

  return alignedFloor + tfMs + epochStart;
}

function calculateHistogramFlip(histogramRows = []) {
  if (!Array.isArray(histogramRows) || histogramRows.length < 2) {
    return false;
  }

  const current = histogramRows[histogramRows.length - 1];
  const previous = histogramRows[histogramRows.length - 2];

  const currentHistogram = Number(current?.histogram);
  const previousHistogram = Number(previous?.histogram);

  if (
    !Number.isFinite(currentHistogram) ||
    !Number.isFinite(previousHistogram)
  ) {
    return false;
  }

  return (previousHistogram <= 0 && currentHistogram > 0);
}

async function detectMidCandleFlip(symbol, timeframe) {
  try {
    if (
      macdUtil &&
      typeof macdUtil.isMacdFlip === 'function'
    ) {
      return await macdUtil.isMacdFlip(
        symbol,
        timeframe
      );
    }
  } catch (err) {
    logger.debug(
      { err, symbol, timeframe },
      'poller: isMacdFlip mid-candle detection failed'
    );
  }

  try {
    if (
      macdUtil &&
      typeof macdUtil.computeMacdHistogram === 'function'
    ) {
      const histogram = await macdUtil.computeMacdHistogram(
        symbol,
        timeframe
      );

      return calculateHistogramFlip(histogram);
    }
  } catch (err) {
    logger.debug(
      { err, symbol, timeframe },
      'poller: computeMacdHistogram mid-candle detection failed'
    );
  }

  return false;
}

function buildEventId(type, symbol, timeframe, candleOpenTime) {
  return [
    type,
    String(symbol || ''),
    String(timeframe || ''),
    Number(candleOpenTime) || 0
  ].join(':');
}

function startupBatchKeyFromSignal(signal) {
  if (!signal || !signal.symbol || !signal.root_tf) {
    return null;
  }

  const symbol = String(signal.symbol).toUpperCase();
  const rootTf = normalizeRootTf(signal.root_tf);
  const candleOpenTime = Number(
    signal.candle_open_time ??
    signal.candleOpenTime ??
    signal.detected_at ??
    0
  );

  const effectiveCandle = Number.isFinite(candleOpenTime)
    ? candleOpenTime
    : 0;

  return `${symbol}|${rootTf}|${effectiveCandle}`;
}

function dedupKeyFromSignal(signal, family = 'generic') {
  if (!signal || !signal.symbol || !signal.root_tf) {
    return null;
  }

  const symbol = String(signal.symbol).toUpperCase();
  const rootTf = normalizeRootTf(signal.root_tf);
  const candleOpenTime = Number(
    signal.candle_open_time ??
    signal.candleOpenTime ??
    signal.detected_at ??
    0
  );

  const effectiveCandle = Number.isFinite(candleOpenTime)
    ? candleOpenTime
    : 0;

  return `${symbol}|${rootTf}|${effectiveCandle}|${String(family).toLowerCase()}`;
}

function alreadyProcessedSignal(signal, family = 'generic') {
  if (isStartupBatchSignal(signal, family)) {
    return true;
  }

  const key = dedupKeyFromSignal(signal, family);
  if (!key) {
    return true;
  }

  if (processedEventKeys.has(key)) {
    return true;
  }

  processedEventKeys.set(key, Date.now());
  return false;
}

function clearExpiredProcessedSignalKeys(ttlMs = 60 * 60 * 1000) {
  const now = Date.now();

  for (const [key, ts] of processedEventKeys.entries()) {
    if (now - ts > ttlMs) {
      processedEventKeys.delete(key);
    }
  }
}

function registerStartupBatchSignal(signal, family = 'generic') {
  const baseKey = startupBatchKeyFromSignal(signal);
  if (baseKey) {
    startupBatchSignalKeys.add(baseKey);
  }

  const key = dedupKeyFromSignal(signal, family);
  if (key) {
    startupBatchSignalKeys.add(key);
    processedEventKeys.set(key, Date.now());
  }

  const symbol = String(signal?.symbol || '').toUpperCase();
  const rootTf = normalizeRootTf(signal?.root_tf);
  const candleOpenTime = Number(
    signal?.candle_open_time ??
    signal?.candleOpenTime ??
    signal?.detected_at ??
    0
  );
  const effectiveCandle = Number.isFinite(candleOpenTime) ? candleOpenTime : 0;

  if (symbol && rootTf) {
    const families = [
      'generic',
      'midcandle',
      'rootcandle',
      'alignment',
      'ws_midcandle',
      'ws_rootcandle',
      'rootcandle_update',
      'midcandle_update',
      'mtf_alignment'
    ];
    for (const fam of families) {
      processedEventKeys.set(`${symbol}|${rootTf}|${effectiveCandle}|${fam}`, Date.now());
    }
  }
}

function isStartupBatchSignal(signal, family = 'generic') {
  const baseKey = startupBatchKeyFromSignal(signal);
  if (baseKey && startupBatchSignalKeys.has(baseKey)) {
    return true;
  }

  const key = dedupKeyFromSignal(signal, family);
  if (key && startupBatchSignalKeys.has(key)) {
    return true;
  }

  return false;
}

async function validateMtfAlignmentConsensus(symbol) {
  const alignment = await signalManager.evaluateMtfAlignment(symbol);

  const alignmentValues = Object.values(alignment || {});

  if (alignmentValues.length === 0) {
    return { isAligned: false, mtfScore: 0, reason: 'no_data', positiveCount: 0, totalCount: 0 };
  }

  const positiveCount = alignmentValues.filter((value) => {
    return value && value.positive === true;
  }).length;

  const totalCount = alignmentValues.length;
  const mtfScore = positiveCount / totalCount;

  const threshold = Number(
    config.MTF_ALIGNMENT_RATING || 0.6
  );

  const isAligned = mtfScore >= threshold;

  return {
    isAligned,
    mtfScore,
    positiveCount,
    totalCount,
    alignment,
    reason: isAligned ? 'threshold_met' : `only_${positiveCount}_of_${totalCount}`
  };
}

function registerBybitWsListeners() {
  if (
    !bybitWs ||
    typeof bybitWs.on !== 'function'
  ) {
    return;
  }

  bybitWs.removeAllListeners('kline');
  bybitWs.on('kline', async (kline) => {
    try {
      if (!kline || !kline.symbol || !kline.timeframe) {
        return;
      }

      const symbol = String(kline.symbol).toUpperCase();
      const tf = normalizeRootTf(kline.timeframe);

      if (!isUsdtSymbol(symbol)) {
        return;
      }

      if (!tf) {
        return;
      }

      const rootTfs = buildRootTfs();

      if (!rootTfs.includes(tf)) {
        return;
      }

      const openTime = Number(kline.open_time);

      if (!Number.isFinite(openTime)) {
        return;
      }

      const wsSignal = {
        symbol,
        root_tf: tf,
        candle_open_time: openTime,
        detected_at: Date.now()
      };

      const midFlip = await detectMidCandleFlip(symbol, tf);

      if (midFlip && !alreadyProcessedSignal(wsSignal, 'ws_midcandle')) {
        const mtfValidation = await validateMtfAlignmentConsensus(symbol);

        if (mtfValidation.isAligned) {
          const eventId = buildEventId('midcandle', symbol, tf, openTime);

          const signal = await signalManager.handleRootSignal({
            symbol,
            root_tf: tf,
            detected_at: Date.now(),
            candle_open_time: openTime,
            eventId,
            signalType: 'midcandle_update',
            notifyImmediately: true
          });

          if (signal) {
            const finalSignal = {
              ...signal,
              eventId,
              notificationType: 'midcandle_update',
              signalType: 'midcandle_update'
            };

            const queued = notificationQueue.enqueueSignal(finalSignal, 'midcandle_update');

            logger.info(
              {
                symbol,
                tf,
                openTime,
                queued,
                eventId
              },
              'poller.ws: ws-triggered midcandle update enqueued'
            );
          }
        }
      }

      // Decoupled WebSocket state key: prevents advancing boundary loop's processedOpen watermark
      const processedStateKey = `poller.ws.processedCandle.${symbol}.${tf}`;
      const processedOpen = Number(dbModule.getState(processedStateKey) || 0);

      if (openTime > processedOpen) {
        const flip = await macdUtil.isMacdFlip(symbol, tf);

        if (flip && !alreadyProcessedSignal(wsSignal, 'ws_rootcandle')) {
          const mtfValidation = await validateMtfAlignmentConsensus(symbol);

          if (mtfValidation.isAligned) {
            const rootEventId = buildEventId('rootcandle', symbol, tf, openTime);

            const signal = await signalManager.handleRootSignal({
              symbol,
              root_tf: tf,
              detected_at: Date.now(),
              candle_open_time: openTime,
              eventId: rootEventId,
              signalType: 'rootcandle_update',
              notifyImmediately: false
            });

            if (signal) {
              const rootSignal = {
                ...signal,
                eventId: rootEventId,
                notificationType: 'new_root_candle',
                signalType: 'rootcandle_update'
              };

              notificationQueue.enqueueSignal(rootSignal, 'new_root_candle');
              logger.info(
                {
                  symbol,
                  tf,
                  openTime,
                  rootEventId
                },
                'poller.ws: ws-triggered root candle signal enqueued'
              );
            }
          }
        }

        dbModule.setState(processedStateKey, openTime);
      }
    } catch (err) {
      logger.debug(
        { err },
        'poller.ws: ws kline event failed'
      );
    }
  });
}

module.exports = {
  start() {
    if (isRunning) {
      logger.debug(
        'poller.start: poller is already running'
      );
      return;
    }

    isRunning = true;
    startupComplete = true;

    try {
      signalManager.setOpenTradesAllowed(true);
      logger.info(
        'poller.start: open trades enabled'
      );
    } catch (err) {
      logger.debug(
        { err },
        'poller.start: failed to enable open trades'
      );
    }

    registerBybitWsListeners();

    if (bybitWs && typeof bybitWs.start === 'function' && bybitWs.isEnabled()) {
      try {
        bybitWs.start();
        const db = dbModule.get();
        if (db) {
          const rows = db.prepare('SELECT symbol FROM symbols').all();
          if (rows && rows.length > 0) {
            const symbols = rows.map((r) => r.symbol).filter(isUsdtSymbol);
            const count = bybitWs.subscribeSymbols(symbols);
            logger.info({ count }, 'poller.start: subscribed symbols to WS stream');
          }
        }
      } catch (err) {
        logger.warn({ err }, 'poller.start: failed to initialize WS stream');
      }
    }

    this.startBoundaryScanLoop();
    logger.info(
      'poller.start: boundary scan loop started'
    );

    this.startRootTfCandleOpenLoop();
    logger.info(
      'poller.start: root TF candle open loop started'
    );
  },

  async initialScan(options = {}) {
    const { seed = true } = options;

    logger.info(
      { seed },
      'poller.initialScan: starting'
    );

    let allSymbols = [];
    const useWs = !!config.USE_WS;

    if (useWs) {
      try {
        const wsTimeoutMs =
          config.WS_INITIAL_SCAN_TIMEOUT || 10000;

        logger.info(
          { timeoutMs: wsTimeoutMs },
          'poller: attempting WS initial scan'
        );

        allSymbols = await Promise.race([
          this.performWsInitialScan(),
          new Promise((_, reject) => {
            setTimeout(() => {
              reject(new Error('WS scan timeout'));
            }, wsTimeoutMs);
          })
        ]);

        if (
          !Array.isArray(allSymbols) ||
          allSymbols.length === 0
        ) {
          logger.warn(
            'poller: WS initial scan returned no symbols; falling back to REST'
          );
          allSymbols = [];
        } else {
          logger.info(
            { count: allSymbols.length },
            'poller: WS initial scan provided symbols'
          );
        }
      } catch (err) {
        logger.debug(
          { err },
          'poller: WS initial scan failed or timed out; falling back to REST'
        );
        allSymbols = [];
      }
    }

    if (
      !Array.isArray(allSymbols) ||
      allSymbols.length === 0
    ) {
      logger.info(
        'poller: fetching symbols via REST'
      );
      allSymbols = await bybit.fetchAllSymbols();
    }

    if (
      !Array.isArray(allSymbols) ||
      allSymbols.length === 0
    ) {
      logger.warn(
        'poller.initialScan: no symbols discovered'
      );
      return [];
    }

    const db = dbModule.get();

    const insert = db.prepare(
      `
        INSERT OR REPLACE INTO symbols
          (symbol, base, quote, fetched_at)
        VALUES (?, ?, ?, ?)
      `
    );

    const now = Date.now();

    const insertMany = db.transaction((rows) => {
      for (const symbolInfo of rows) {
        if (
          !symbolInfo ||
          !symbolInfo.symbol
        ) {
          continue;
        }

        insert.run(
          symbolInfo.symbol,
          symbolInfo.base ||
            symbolInfo.symbol.replace(
              /USDT(\.P)?$/i,
              ''
            ),
          symbolInfo.quote || 'USDT',
          now
        );
      }
    });

    const validSymbols = allSymbols.filter((symbolInfo) =>
      symbolInfo &&
      symbolInfo.symbol &&
      isUsdtSymbol(symbolInfo.symbol)
    );

    insertMany(validSymbols);

    logger.info(
      {
        total: validSymbols.length
      },
      'poller.initialScan: symbols persisted'
    );

    if (bybitWs && typeof bybitWs.start === 'function' && bybitWs.isEnabled()) {
      try {
        bybitWs.start();
        const subCount = bybitWs.subscribeSymbols(validSymbols);
        logger.info({ subCount }, 'poller.initialScan: subscribed symbols to WS stream');
      } catch (err) {
        logger.warn({ err }, 'poller.initialScan: WS subscription error');
      }
    }

    const seedSymbols =
      bybit.getSeedSymbols(validSymbols);

    if (
      seedSymbols &&
      seedSymbols.length > 0
    ) {
      const invalidSymbols = seedSymbols.filter(
        (seedSymbol) => {
          const symbol = String(
            seedSymbol.symbol || ''
          ).toUpperCase();

          if (
            /USDT[QHUZ0-9]/.test(
              symbol.slice(-6)
            )
          ) {
            return true;
          }

          return !/USDT(\.P)?$/.test(symbol);
        }
      );

      if (invalidSymbols.length > 0) {
        logger.error(
          {
            count: invalidSymbols.length,
            samples: invalidSymbols
              .slice(0, 5)
              .map((item) => item.symbol)
          },
          'poller.initialScan: invalid symbols in seed list'
        );
      }

      if (seed) {
        setImmediate(() => {
          this.backgroundSeedKlines(seedSymbols)
            .catch((err) => {
              logger.debug(
                { err },
                'poller.initialScan: background seeding failed'
              );
            });
        });
      } else {
        logger.debug(
          'poller.initialScan: background seeding disabled'
        );
      }
    } else {
      logger.info(
        'poller.initialScan: no seed symbols to process'
      );
    }

    return validSymbols;
  },

  async performWsInitialScan() {
    try {
      if (
        bybitWs &&
        typeof bybitWs.performInitialScan === 'function'
      ) {
        const result =
          await bybitWs.performInitialScan();

        return Array.isArray(result)
          ? result
          : [];
      }
    } catch (err) {
      logger.debug(
        { err },
        'poller.performWsInitialScan failed'
      );
    }

    return [];
  },

  async backgroundSeedKlines(symbols = []) {
    if (
      !Array.isArray(symbols) ||
      symbols.length === 0
    ) {
      logger.info(
        'backgroundSeedKlines: nothing to seed'
      );
      return;
    }

    logger.info(
      {
        count: symbols.length,
        concurrency: SEED_CONCURRENCY
      },
      'backgroundSeedKlines: starting'
    );

    for (
      let i = 0;
      i < symbols.length;
      i += SEED_CONCURRENCY
    ) {
      const batch = symbols.slice(
        i,
        i + SEED_CONCURRENCY
      );

      const jobs = batch.map((item) => {
        return limiter.schedule(() => {
          return this.seedKlinesForSymbol(item.symbol);
        });
      });

      try {
        await Promise.all(jobs);
      } catch (err) {
        logger.debug(
          { err },
          'backgroundSeedKlines: batch failed; continuing'
        );
      }
    }

    logger.info(
      'backgroundSeedKlines: completed'
    );
  },

  async seedKlinesForSymbol(
    symbol,
    timeframe = null
  ) {
    try {
      const symbolUpper = String(symbol || '')
        .toUpperCase();

      if (!isUsdtSymbol(symbol)) {
        logger.warn(
          { symbol },
          'seedKlinesForSymbol: invalid USDT symbol; skipping'
        );
        return;
      }

      if (
        /USDT[QHUZ0-9]/.test(
          symbolUpper.slice(-6)
        )
      ) {
        logger.warn(
          { symbol },
          'seedKlinesForSymbol: dated variant; skipping'
        );
        return;
      }

      const rootTfs = timeframe
        ? [String(timeframe)]
        : (
            Array.isArray(config.ROOT_TFS)
              ? config.ROOT_TFS
              : buildRootTfs()
          );

      const mtfTfs = Array.isArray(config.MTF_TFS)
        ? config.MTF_TFS.map(String)
        : [];

      const timeframes = Array.from(
        new Set([
          ...rootTfs,
          ...mtfTfs
        ])
      );

      for (const tf of timeframes) {
        const interval =
          normalizeRootTf(tf) === 'D'
            ? 'D'
            : String(tf);

        try {
          const klines = await limiter.schedule(() =>
            bybit.fetchKlines(
              symbol,
              interval,
              config.SEED_KLINES_LIMIT
            )
          );

          if (
            !klines ||
            klines.length === 0
          ) {
            logger.debug(
              { symbol, tf },
              'seedKlinesForSymbol: no klines returned'
            );
            continue;
          }

          const db = dbModule.get();

          const insert = db.prepare(
            `
              INSERT OR IGNORE INTO klines
                (
                  symbol,
                  timeframe,
                  open_time,
                  open,
                  high,
                  low,
                  close,
                  volume
                )
              VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            `
          );

          const insertMany = db.transaction((rows) => {
            for (const kline of rows) {
              insert.run(
                symbol,
                tf,
                kline.open_time,
                kline.open,
                kline.high,
                kline.low,
                kline.close,
                kline.volume
              );
            }
          });

          insertMany(klines);

          logger.debug(
            {
              symbol,
              tf,
              count: klines.length
            },
            'seedKlinesForSymbol: klines persisted'
          );

          try {
            if (
              typeof macdUtil.computeAndStoreMacd ===
              'function'
            ) {
              await macdUtil.computeAndStoreMacd(
                symbol,
                tf
              );
            } else if (
              typeof macdUtil.computeMacdHistogram ===
              'function'
            ) {
              await macdUtil.computeMacdHistogram(
                symbol,
                tf
              );
            }
          } catch (err) {
            logger.debug(
              {
                err,
                symbol,
                tf
              },
              'seedKlinesForSymbol: MACD warm-up failed'
            );
          }
        } catch (err) {
          logger.debug(
            {
              err,
              symbol,
              tf
            },
            'seedKlinesForSymbol: timeframe fetch failed'
          );
        }
      }
    } catch (err) {
      logger.debug(
        {
          err,
          symbol,
          timeframe
        },
        'seedKlinesForSymbol: unexpected error'
      );
    }
  },

  async quickFetchLatestKline(symbol, tf) {
    try {
      const interval = normalizeRootTf(tf) === 'D' ? 'D' : String(tf);

      // Fetch only the 2 latest candles from REST to update state cleanly
      const klines = await limiter.schedule(() =>
        bybit.fetchKlines(symbol, interval, 2)
      );

      if (!klines || klines.length === 0) {
        return;
      }

      const db = dbModule.get();
      const insert = db.prepare(
        `
          INSERT OR IGNORE INTO klines
            (symbol, timeframe, open_time, open, high, low, close, volume)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        `
      );

      const insertMany = db.transaction((rows) => {
        for (const kline of rows) {
          insert.run(
            symbol,
            tf,
            kline.open_time,
            kline.open,
            kline.high,
            kline.low,
            kline.close,
            kline.volume
          );
        }
      });

      insertMany(klines);

      if (typeof macdUtil.computeAndStoreMacd === 'function') {
        await macdUtil.computeAndStoreMacd(symbol, tf);
      } else if (typeof macdUtil.computeMacdHistogram === 'function') {
        await macdUtil.computeMacdHistogram(symbol, tf);
      }
    } catch (err) {
      logger.debug({ err, symbol, tf }, 'quickFetchLatestKline: fetch failed');
    }
  },

  async processMissingDataQueueSafe(missingDataQueue = []) {
    if (!Array.isArray(missingDataQueue) || missingDataQueue.length === 0) {
      return;
    }

    const bgStart = performance.now();
    const COOLDOWN_MS = 5 * 60 * 1000;
    const MAX_REST_FALLBACKS_PER_PASS = 30;
    const MISSING_THRESHOLD = 50;
    const now = Date.now();

    // Circuit Breaker: Trigger WS restart if a massive disconnection is implied
    if (missingDataQueue.length > MISSING_THRESHOLD) {
      logger.warn(
        { missingCount: missingDataQueue.length },
        'poller.circuitBreaker: high missing count detected; triggering WS reconnect check'
      );
      if (bybitWs && typeof bybitWs.start === 'function' && bybitWs.isEnabled()) {
        try {
          bybitWs.start();
        } catch (err) {
          logger.error({ err }, 'poller.circuitBreaker: failed to restart WS connection');
        }
      }
    }

    // Filter out symbols on active cooldown
    const eligibleItems = missingDataQueue.filter((item) => {
      const key = `${item.symbol}|${item.tf}`;
      const lastAttempt = restFallbackCooldowns.get(key) || 0;
      return now - lastAttempt > COOLDOWN_MS;
    });

    const itemsToProcess = eligibleItems.slice(0, MAX_REST_FALLBACKS_PER_PASS);

    if (itemsToProcess.length === 0) {
      logger.debug(
        'poller.bg: all missing items on cooldown or queue capped; skipping REST fallback'
      );
      return;
    }

    logger.info(
      {
        totalMissing: missingDataQueue.length,
        eligible: eligibleItems.length,
        processing: itemsToProcess.length
      },
      'poller.bg: executing rate-controlled lightweight catch-up'
    );

    let fetchedCount = 0;
    for (const item of itemsToProcess) {
      const key = `${item.symbol}|${item.tf}`;
      restFallbackCooldowns.set(key, now);

      try {
        await this.quickFetchLatestKline(item.symbol, item.tf);
        fetchedCount++;
      } catch (err) {
        logger.debug({ err, symbol: item.symbol, tf: item.tf }, 'poller.bg: lightweight fetch error');
      }
    }

    // Purge expired cooldowns
    for (const [key, ts] of restFallbackCooldowns.entries()) {
      if (now - ts > COOLDOWN_MS * 2) {
        restFallbackCooldowns.delete(key);
      }
    }

    logger.info(
      {
        durationMs: (performance.now() - bgStart).toFixed(2),
        fetchedCount
      },
      'poller.perf: rate-controlled kline catch-up completed'
    );
  },

  async scanAllForStartup() {
    if (startupComplete) {
      logger.debug(
        'scanAllForStartup: startup scan already completed'
      );
      return [];
    }

    logger.info(
      'scanAllForStartup: starting full startup pass'
    );

    try {
      const db = dbModule.get();

      const rows = db
        .prepare(
          `
            SELECT symbol
            FROM symbols
            ORDER BY symbol COLLATE NOCASE ASC
          `
        )
        .all();

      const validRows = rows.filter((row) =>
        isUsdtSymbol(row.symbol)
      );

      const invalidCount = rows.length - validRows.length;

      if (invalidCount > 0) {
        logger.warn(
          {
            invalidCount
          },
          'scanAllForStartup: invalid symbols skipped'
        );
      }

      const newSignals = [];
      const pageSize = Number(
        config.PAGE_SIZE || 25
      );

      for (
        let i = 0;
        i < validRows.length;
        i += pageSize
      ) {
        const page = validRows.slice(
          i,
          i + pageSize
        );

        const results = await Promise.all(
          page.map((row) =>
            this.scanSymbolRoots(row.symbol)
          )
        );

        newSignals.push(...results.flat());
      }

      newSignals.sort((a, b) => {
        const symbolA = String(
          a.symbol || ''
        ).toUpperCase();

        const symbolB = String(
          b.symbol || ''
        ).toUpperCase();

        return symbolA.localeCompare(symbolB);
      });

      if (newSignals.length > 0) {
        logger.info(
          {
            count: newSignals.length
          },
          'scanAllForStartup: enqueueing startup notification batch'
        );

        for (const signal of newSignals) {
          registerStartupBatchSignal(signal, signal.signalType || 'rootcandle_update');
        }

        notificationQueue.enqueueStartupBatch(
          newSignals
        );
      } else {
        logger.info(
          'scanAllForStartup: no startup signals found'
        );
      }

      this.initializeBoundaryCandleState(
        validRows,
        db
      );

      startupComplete = true;

      logger.info(
        'scanAllForStartup: completed'
      );

      return newSignals;
    } catch (err) {
      logger.error(
        {
          err
        },
        'scanAllForStartup: unexpected error'
      );

      return [];
    }
  },

  initializeBoundaryCandleState(rows, db) {
    const rootTfs = buildRootTfs();

    for (const row of rows) {
      const symbol = row.symbol;

      for (const tf of rootTfs) {
        const latestOpen = getLatestOpenTime(
          db,
          symbol,
          tf
        );

        if (latestOpen === null) {
          continue;
        }

        dbModule.setState(
          this.getLoop2ProcessedCandleKey(
            symbol,
            tf
          ),
          latestOpen
        );
      }
    }

    logger.info(
      {
        symbols: rows.length,
        timeframes: rootTfs.length
      },
      'poller: initialized boundary candle state'
    );
  },

  async scanSymbolRoots(symbol) {
    const rootTfs = buildRootTfs();
    const results = [];

    if (!isUsdtSymbol(symbol)) {
      logger.warn(
        {
          symbol
        },
        'scanSymbolRoots: invalid USDT symbol; skipping'
      );

      return results;
    }

    for (const tf of rootTfs) {
      try {
        const db = dbModule.get();

        const selectStatement = db.prepare(
          `
            SELECT open_time, close, open
            FROM klines
            WHERE symbol = ?
              AND timeframe = ?
            ORDER BY open_time DESC
            LIMIT 2
          `
        );

        let rows = selectStatement.all(
          symbol,
          tf
        );

        if (
          !rows ||
          rows.length < 2
        ) {
          logger.debug(
            {
              symbol,
              tf
            },
            'scanSymbolRoots: insufficient klines; seeding'
          );

          await this.seedKlinesForSymbol(
            symbol,
            tf
          );

          rows = selectStatement.all(
            symbol,
            tf
          );

          if (
            !rows ||
            rows.length < 2
          ) {
            logger.debug(
              {
                symbol,
                tf
              },
              'scanSymbolRoots: still insufficient klines'
            );

            continue;
          }
        }

        const flip =
          await macdUtil.isMacdFlip(
            symbol,
            tf
          );

        if (!flip) {
          continue;
        }

        const signal =
          await signalManager.handleRootSignal({
            symbol,
            root_tf: tf,
            detected_at: Date.now(),
            candle_open_time: Number(
              rows[0].open_time
            ),
            notifyImmediately: false
          });

        if (signal) {
          results.push(signal);
        }
      } catch (err) {
        logger.debug(
          {
            err,
            symbol,
            tf
          },
          'scanSymbolRoots: error checking flip'
        );
      }
    }

    return results;
  },

  getNextFiveMinuteBoundaryMs(
    nowMs = Date.now()
  ) {
    const remainder =
      nowMs % FIVE_MINUTES_MS;

    return nowMs + (
      remainder === 0
        ? FIVE_MINUTES_MS
        : FIVE_MINUTES_MS - remainder
    );
  },

  startBoundaryScanLoop() {
    setImmediate(() => {
      this.runBoundaryScanLoop()
        .catch((err) => {
          logger.error(
            {
              err
            },
            'poller: boundary scan loop crashed'
          );
        });
    });
  },

  async runBoundaryScanLoop() {
    while (isRunning) {
      const nextBoundary =
        this.getNextFiveMinuteBoundaryMs();

      const delay = Math.max(
        0,
        nextBoundary - Date.now()
      );

      logger.debug(
        {
          nextBoundary: new Date(
            nextBoundary
          ).toISOString(),
          delayMs: delay
        },
        'poller.loop2: waiting for next five-minute boundary'
      );

      await sleep(delay);

      if (!isRunning) {
        break;
      }

      if (boundaryScanInProgress) {
        logger.warn(
          'poller.loop2: previous boundary scan is still running; skipping boundary'
        );
        continue;
      }

      boundaryScanInProgress = true;

      try {
        await this.runBoundaryScanOnce();
      } catch (err) {
        logger.error(
          {
            err
          },
          'poller.loop2: boundary scan failed'
        );
      } finally {
        boundaryScanInProgress = false;
      }
    }
  },

  async runBoundaryScanOnce() {
    clearExpiredProcessedSignalKeys();

    const boundary = new Date();

    logger.info(
      {
        boundary: boundary.toISOString()
      },
      'poller.loop2: starting five-minute boundary scan (mid-candle detection via WS)'
    );

    const db = dbModule.get();

    const rows = db
      .prepare(
        `
          SELECT symbol
          FROM symbols
          ORDER BY symbol COLLATE NOCASE ASC
        `
      )
      .all();

    const validRows = rows.filter((row) =>
      isUsdtSymbol(row.symbol)
    );

    const rootTfs = buildRootTfs();
    const newSignals = [];
    const alignmentAlerts = [];

    const latestSignals = dbModule.getLatestSignalsSnapshot() || [];

    for (const row of validRows) {
      const symbol = row.symbol;

      for (const tf of rootTfs) {
        try {
          let latestOpen = getLatestOpenTime(
            db,
            symbol,
            tf
          );

          if (latestOpen === null) {
            await this.seedKlinesForSymbol(symbol, tf);
            latestOpen = getLatestOpenTime(db, symbol, tf);
          }

          if (latestOpen === null) {
            continue;
          }

          const processedStateKey =
            this.getLoop2ProcessedCandleKey(
              symbol,
              tf
            );

          const processedOpen = Number(
            dbModule.getState(
              processedStateKey
            ) || 0
          );

          if (latestOpen > processedOpen) {
            logger.debug(
              {
                symbol,
                tf,
                latestOpen,
                processedOpen
              },
              'poller.loop2: new root candle detected; skipping mid-candle detection for this root candle'
            );
            continue;
          }

          const activeSignals = latestSignals.filter((signal) => {
            return (
              signal.symbol === symbol &&
              normalizeRootTf(signal.root_tf) === tf
            );
          });

          const isAlreadyInSummary = activeSignals.length > 0;

          if (!isAlreadyInSummary) {
            const midCandleStateKey =
              `poller.loop2.midCandle.${symbol}.${tf}.${latestOpen}`;

            const midCandleAlreadyReported =
              dbModule.getState(midCandleStateKey);

            if (!midCandleAlreadyReported) {
              const midCandleFlip =
                await detectMidCandleFlip(
                  symbol,
                  tf
                );

              if (midCandleFlip) {
                const mtfValidation = await validateMtfAlignmentConsensus(symbol);

                const eventId = buildEventId(
                  'midcandle',
                  symbol,
                  tf,
                  latestOpen
                );

                const midSignalCandidate = {
                  symbol,
                  root_tf: tf,
                  detected_at: Date.now(),
                  candle_open_time: latestOpen,
                  eventId,
                  signalType: 'midcandle_update',
                  notifyImmediately: true
                };

                if (isStartupBatchSignal(midSignalCandidate, 'midcandle')) {
                  logger.debug(
                    {
                      symbol,
                      tf,
                      latestOpen,
                      eventId
                    },
                    'poller.loop2: skipping startup-batch duplicate mid-candle signal'
                  );
                  continue;
                }

                if (!alreadyProcessedSignal(midSignalCandidate, 'midcandle')) {
                  const signal =
                    await signalManager.handleRootSignal(midSignalCandidate);

                  if (signal) {
                    const decision =
                      mtfValidation.isAligned
                        ? 'accept'
                        : 'monitor';

                    const finalSignal = {
                      ...signal,
                      eventId,
                      notificationType: 'midcandle_update',
                      signalType: 'midcandle_update',
                      state: decision,
                      meta: {
                        ...(signal.meta || {}),
                        mtfScore: mtfValidation.mtfScore,
                        mtfAligned: mtfValidation.isAligned,
                        alignment: mtfValidation.alignment || {},
                        decision,
                        acceptReason: mtfValidation.isAligned
                          ? 'mtf_alignment_met'
                          : 'mtf_alignment_monitor',
                        tvScore: 0,
                        tvSource: 'loop2'
                      }
                    };

                    newSignals.push(finalSignal);

                    logger.info(
                      {
                        symbol,
                        tf,
                        latestOpen,
                        eventId,
                        mtfScore: (mtfValidation.mtfScore * 100).toFixed(0) + '%',
                        decision,
                        mtfAligned: mtfValidation.isAligned
                      },
                      'poller.loop2: mid-candle histogram flip detected and enqueued with decision tag'
                    );
                  }

                  dbModule.setState(
                    midCandleStateKey,
                    Date.now()
                  );
                }
              }
            }
          } else {
            const activeSignal = activeSignals[0];
            const mtfValidation = await validateMtfAlignmentConsensus(symbol);

            const alignmentStateKey =
              `poller.loop2.alignment.${symbol}.${tf}`;

            const previousAlignedState =
              dbModule.getState(
                alignmentStateKey
              );

            const previousAligned =
              previousAlignedState === 'true' ||
              previousAlignedState === true;

            const isAllPositive =
              mtfValidation.totalCount > 0 &&
              mtfValidation.positiveCount === mtfValidation.totalCount;

            const shouldAlert =
              isAllPositive &&
              !previousAligned;

            if (shouldAlert) {
              const alertSignalCandidate = {
                ...activeSignal,
                symbol,
                root_tf: tf,
                detected_at: Date.now(),
                eventId: buildEventId('mtf_align', symbol, tf, Date.now()),
                signalType: 'mtf_alignment',
                notificationType: 'mtf_alignment',
                state: 'monitor',
                meta: {
                  ...(activeSignal.meta || {}),
                  alignment: mtfValidation.alignment || {},
                  decision: 'monitor',
                  acceptReason: 'mtf_alignment_alert',
                  tvScore: 0,
                  tvSource: 'loop2',
                  mtfScore: mtfValidation.mtfScore
                }
              };

              if (!alreadyProcessedSignal(alertSignalCandidate, 'alignment') && !isStartupBatchSignal(alertSignalCandidate, 'alignment')) {
                dbModule.setState(
                  alignmentStateKey,
                  String(true)
                );

                const alertEventId = buildEventId('mtf_align', symbol, tf, Date.now());

                alignmentAlerts.push({
                  ...activeSignal,
                  symbol,
                  root_tf: tf,
                  detected_at: Date.now(),
                  eventId: alertEventId,
                  signalType: 'mtf_alignment',
                  notificationType: 'mtf_alignment',
                  state: 'monitor',
                  meta: {
                    ...(activeSignal.meta || {}),
                    alignment: mtfValidation.alignment || {},
                    decision: 'monitor',
                    acceptReason: 'mtf_alignment_alert',
                    tvScore: 0,
                    tvSource: 'loop2',
                    mtfScore: mtfValidation.mtfScore
                  }
                });

                logger.info(
                  {
                    symbol,
                    tf,
                    mtfScore: (mtfValidation.mtfScore * 100).toFixed(0) + '%',
                    positiveCount: mtfValidation.positiveCount,
                    totalCount: mtfValidation.totalCount
                  },
                  'poller.loop2: MTF alignment alert created for existing active signal (100% consensus met)'
                );
              }
            } else if (!isAllPositive && previousAligned) {
              dbModule.setState(
                alignmentStateKey,
                String(false)
              );

              logger.info(
                {
                  symbol,
                  tf,
                  mtfScore: (mtfValidation.mtfScore * 100).toFixed(0) + '%'
                },
                'poller.loop2: MTF alignment dropped below 100%'
              );
            } else if (previousAlignedState === undefined) {
              dbModule.setState(
                alignmentStateKey,
                String(false)
              );
            }
          }
        } catch (err) {
          logger.debug(
            {
              err,
              symbol,
              tf
            },
            'poller.loop2: symbol/timeframe scan failed'
          );
        }
      }
    }

    for (const signal of newSignals) {
      try {
        if (isStartupBatchSignal(signal, signal.signalType || 'midcandle_update')) {
          logger.debug(
            {
              symbol: signal.symbol,
              root_tf: signal.root_tf,
              eventId: signal.eventId
            },
            'poller.loop2: skipping startup-batch duplicate midcandle enqueue'
          );
          continue;
        }

        const queued =
          notificationQueue.enqueueSignal(
            signal,
            'midcandle_update'
          );

        logger.info(
          {
            symbol: signal.symbol,
            root_tf: signal.root_tf,
            queued,
            decision: signal.meta && signal.meta.decision
          },
          'poller.loop2: midcandle update enqueued'
        );
      } catch (err) {
        logger.error(
          {
            err,
            signal
          },
          'poller.loop2: FAILED to enqueue midcandle update'
        );
      }
    }

    for (const alert of alignmentAlerts) {
      try {
        const queued =
          notificationQueue.enqueueSignal(
            alert,
            'mtf_alignment'
          );

        logger.info(
          {
            symbol: alert.symbol,
            root_tf: alert.root_tf,
            queued,
            mtfScore: (
              alert.meta.mtfScore * 100
            ).toFixed(0) + '%'
          },
          'poller.loop2: realtime alignment alert enqueued'
        );
      } catch (err) {
        logger.error(
          {
            err,
            alert
          },
          'poller.loop2: FAILED to enqueue alignment alert'
        );
      }
    }

    logger.info(
      {
        newSignals: newSignals.length,
        alignmentAlerts: alignmentAlerts.length
      },
      'poller.loop2: exact five-minute boundary scan completed'
    );
  },

  startRootTfCandleOpenLoop() {
    setImmediate(() => {
      this.runRootTfCandleOpenLoop()
        .catch((err) => {
          logger.error(
            {
              err
            },
            'poller: root TF candle open loop crashed'
          );
        });
    });
  },

  async runRootTfCandleOpenLoop() {
    while (isRunning) {
      const rootTfs = buildRootTfs();
      const nowMs = Date.now();

      let nextBoundaryMs = Infinity;

      for (const tf of rootTfs) {
        const next = getNextTimeframeBoundaryMs(
          tf,
          nowMs
        );

        if (next < nextBoundaryMs) {
          nextBoundaryMs = next;
        }
      }

      if (!isFinite(nextBoundaryMs)) {
        logger.warn(
          'poller: next root TF boundary calculation failed; falling back to 1 hour'
        );
        nextBoundaryMs =
          nowMs + 60 * 60 * 1000;
      }

      const dueTfs = [];
      for (const tf of rootTfs) {
        const next = getNextTimeframeBoundaryMs(tf, nowMs);
        if (next === nextBoundaryMs) {
          dueTfs.push(tf);
        }
      }

      const delay = Math.max(
        0,
        nextBoundaryMs - Date.now()
      );

      logger.debug(
        {
          nextBoundaryMs: new Date(
            nextBoundaryMs
          ).toISOString(),
          delayMs: delay,
          dueTfs
        },
        'poller: waiting for next root TF candle-open boundary'
      );

      await sleep(delay);

      if (!isRunning) {
        break;
      }

      await sleep(1500);

      try {
        const tfsToRun = dueTfs.length > 0 ? dueTfs : buildRootTfs();

        let scannedSuccess = false;
        for (let attempt = 1; attempt <= 3; attempt++) {
          logger.info({ attempt, tfsToRun }, 'poller: executing root TF candle open scan pass');

          scannedSuccess = await this.runRootTfCandleOpenOnce(tfsToRun, Date.now());

          if (scannedSuccess || attempt === 3) break;

          logger.warn({ attempt }, 'poller: new klines not fully ready; retrying in 2 seconds...');
          await sleep(2000);
        }
      } catch (err) {
        logger.error(
          {
            err
          },
          'poller: root TF candle open scan failed'
        );
      }
    }
  },

  async runRootTfCandleOpenOnce(tfsInput, nowMs = Date.now()) {
    const scanStartTime = performance.now();
    let tfsToProcess = Array.isArray(tfsInput) ? tfsInput : [];

    if (tfsToProcess.length === 0) {
      const rootTfs = buildRootTfs();
      const epochStart = Date.UTC(1970, 0, 1);

      for (const tf of rootTfs) {
        const tfMs = getRootTfMs(tf);
        const isBoundary = Math.abs((nowMs - epochStart) % tfMs) < 5 * 60 * 1000;
        if (isBoundary) {
          tfsToProcess.push(tf);
        }
      }
    }

    if (tfsToProcess.length === 0) {
      return false;
    }

    logger.info(
      {
        timeframes: tfsToProcess,
        timestamp: new Date(nowMs).toISOString()
      },
      'poller: root TF candle open boundary scan started'
    );

    const db = dbModule.get();
    const rows = db
      .prepare(
        `
          SELECT symbol
          FROM symbols
          ORDER BY symbol COLLATE NOCASE ASC
        `
      )
      .all();

    const validRows = rows.filter((row) => isUsdtSymbol(row.symbol));

    const readyCandidates = [];
    const missingDataQueue = [];
    let detectedNewCandles = false;

    // STEP 1: FAST PATH VS. SLOW PATH SEPARATION
    const dbCheckStart = performance.now();

    for (const row of validRows) {
      const symbol = row.symbol;

      for (const tf of tfsToProcess) {
        const latestOpen = getLatestOpenTime(db, symbol, tf);
        const processedStateKey = this.getLoop2ProcessedCandleKey(symbol, tf);
        const processedOpen = Number(dbModule.getState(processedStateKey) || 0);

        if (latestOpen !== null && latestOpen > processedOpen) {
          detectedNewCandles = true;
          readyCandidates.push({
            symbol,
            tf,
            latestOpen,
            processedStateKey
          });
        } else {
          missingDataQueue.push({ symbol, tf });
        }
      }
    }

    const dbCheckDuration = (performance.now() - dbCheckStart).toFixed(2);
    logger.info(
      {
        durationMs: dbCheckDuration,
        readyCount: readyCandidates.length,
        missingCount: missingDataQueue.length
      },
      'poller.perf: [1/4] Fast path DB classification completed'
    );

    // STEP 2: SLOW PATH - RATE-CONTROLLED LIGHTWEIGHT CATCH-UP & CIRCUIT BREAKER
    if (missingDataQueue.length > 0) {
      logger.info(
        { missingCount: missingDataQueue.length },
        'poller: offloading missing symbols to rate-controlled background catch-up'
      );

      setImmediate(async () => {
        await this.processMissingDataQueueSafe(missingDataQueue);
      });
    }

    if (readyCandidates.length === 0) {
      logger.info(
        { durationMs: (performance.now() - scanStartTime).toFixed(2) },
        'poller: no new WebSocket candle data present in DB yet; fast path complete'
      );
      return false;
    }

    // STEP 3: CONCURRENT COMPUTATION ON FAST PATH CANDIDATES
    const computeStart = performance.now();
    const allBoundarySignals = [];

    const candidatePromises = readyCandidates.map(async (candidate) => {
      const { symbol, tf, latestOpen, processedStateKey } = candidate;

      try {
        const flip = await macdUtil.isMacdFlip(symbol, tf);

        if (flip) {
          const mtfValidation = await validateMtfAlignmentConsensus(symbol);
          const eventId = buildEventId('rootcandle', symbol, tf, latestOpen);

          const rootCandidate = {
            symbol,
            root_tf: tf,
            detected_at: Date.now(),
            candle_open_time: latestOpen,
            eventId,
            signalType: 'rootcandle_update',
            notifyImmediately: true
          };

          if (isStartupBatchSignal(rootCandidate, 'rootcandle')) {
            logger.debug(
              { symbol, tf, latestOpen, eventId },
              'poller: skipping startup-batch duplicate root candle signal'
            );
          } else if (!alreadyProcessedSignal(rootCandidate, 'rootcandle')) {
            const signal = await signalManager.handleRootSignal(rootCandidate);

            if (signal) {
              const decision = mtfValidation.isAligned ? 'accept' : 'monitor';

              const finalSignal = {
                ...signal,
                eventId,
                notificationType: 'new_root_candle',
                signalType: 'rootcandle_update',
                state: decision,
                meta: {
                  ...(signal.meta || {}),
                  mtfScore: mtfValidation.mtfScore,
                  mtfAligned: mtfValidation.isAligned,
                  alignment: mtfValidation.alignment || {},
                  decision,
                  acceptReason: mtfValidation.isAligned
                    ? 'mtf_alignment_met'
                    : 'mtf_alignment_monitor',
                  tvScore: 0,
                  tvSource: 'loop2'
                }
              };

              const midCandleStateKey = `poller.loop2.midCandle.${symbol}.${tf}.${latestOpen}`;
              dbModule.setState(midCandleStateKey, Date.now());

              logger.info(
                {
                  symbol,
                  tf,
                  latestOpen,
                  eventId,
                  mtfScore: (mtfValidation.mtfScore * 100).toFixed(0) + '%',
                  decision,
                  mtfAligned: mtfValidation.isAligned
                },
                'poller: root candle flip detected and enqueued with decision tag'
              );

              return finalSignal;
            }
          }
        }

        dbModule.setState(processedStateKey, latestOpen);
      } catch (err) {
        logger.debug(
          { err, symbol, tf },
          'poller: error evaluating fast-path candidate'
        );
      }

      return null;
    });

    const evaluatedResults = await Promise.all(candidatePromises);
    for (const res of evaluatedResults) {
      if (res) {
        allBoundarySignals.push(res);
      }
    }

    const computeDuration = (performance.now() - computeStart).toFixed(2);
    logger.info(
      {
        durationMs: computeDuration,
        processedCandidates: readyCandidates.length,
        signalsFound: allBoundarySignals.length
      },
      'poller.perf: [2/4] Concurrent MACD and MTF evaluation completed'
    );

    // STEP 4: INSTANT SUMMARY & RECOMMENDED BLOCK BROADCAST
    const broadcastStart = performance.now();

    if (allBoundarySignals.length > 0) {
      logger.info(
        {
          count: allBoundarySignals.length,
          timeframes: tfsToProcess
        },
        'poller: enqueueing root TF candle open notifications via summary + per-block flow'
      );

      notificationQueue.enqueueRootCandleOpenBatch(
        allBoundarySignals,
        tfsToProcess.join(',')
      );
    }

    // SUMMARY BLOCK
    const summaryBlock = {
      timestamp: new Date(nowMs).toISOString(),
      scanType: 'root_tf_candle_open',
      timeframes: tfsToProcess,
      totalSymbolsScanned: validRows.length,
      fastPathProcessed: readyCandidates.length,
      slowPathQueued: missingDataQueue.length,
      newCandlesDetected: detectedNewCandles,
      signalsGenerated: allBoundarySignals.length,
      acceptSignals: allBoundarySignals.filter(s => s.meta?.decision === 'accept').length,
      monitorSignals: allBoundarySignals.filter(s => s.meta?.decision === 'monitor').length,
      avgMtfScore: allBoundarySignals.length > 0
        ? (allBoundarySignals.reduce((sum, s) => sum + (s.meta?.mtfScore || 0), 0) / allBoundarySignals.length * 100).toFixed(1) + '%'
        : 'N/A',
      totalDurationMs: (performance.now() - scanStartTime).toFixed(2),
      detailedBreakdown: tfsToProcess.map(tf => {
        const tfSignals = allBoundarySignals.filter(s => normalizeRootTf(s.root_tf) === tf);
        return {
          timeframe: tf,
          signalCount: tfSignals.length,
          acceptCount: tfSignals.filter(s => s.meta?.decision === 'accept').length,
          monitorCount: tfSignals.filter(s => s.meta?.decision === 'monitor').length
        };
      })
    };

    logger.info(summaryBlock, 'poller.rootTfCandleOpen: SUMMARY BLOCK');

    // SIGNAL PER BLOCK
    for (const signal of allBoundarySignals) {
      const signalBlock = {
        symbol: signal.symbol,
        timeframe: normalizeRootTf(signal.root_tf),
        candleOpenTime: new Date(signal.candle_open_time).toISOString(),
        eventId: signal.eventId,
        decision: signal.meta?.decision || 'unknown',
        mtfScore: (signal.meta?.mtfScore * 100).toFixed(1) + '%',
        mtfAligned: signal.meta?.mtfAligned || false,
        alignmentDetails: signal.meta?.alignment || {},
        acceptReason: signal.meta?.acceptReason || 'none',
        confidence: signal.meta?.mtfAligned ? 'high' : 'medium'
      };

      logger.info(signalBlock, 'poller.rootTfCandleOpen: SIGNAL DETAIL');
    }

    // RECOMMENDED BLOCKS
    const acceptedSignals = allBoundarySignals.filter(s => s.meta?.decision === 'accept');
    const monitorSignals = allBoundarySignals.filter(s => s.meta?.decision === 'monitor');

    if (acceptedSignals.length > 0) {
      const acceptedBlock = {
        actionType: 'ACCEPT',
        count: acceptedSignals.length,
        reason: 'MTF alignment threshold met (≥ 60%)',
        signals: acceptedSignals.map(s => ({
          symbol: s.symbol,
          timeframe: normalizeRootTf(s.root_tf),
          mtfScore: (s.meta?.mtfScore * 100).toFixed(1) + '%',
          positiveCount: s.meta?.alignment ? Object.values(s.meta.alignment).filter(a => a?.positive).length : 0
        })),
        recommendation: 'Process these signals for potential trade entry'
      };

      logger.info(acceptedBlock, 'poller.rootTfCandleOpen: RECOMMENDED BLOCK (ACCEPT)');
    }

    if (monitorSignals.length > 0) {
      const monitorBlock = {
        actionType: 'MONITOR',
        count: monitorSignals.length,
        reason: 'MTF alignment below threshold (< 60%)',
        signals: monitorSignals.map(s => ({
          symbol: s.symbol,
          timeframe: normalizeRootTf(s.root_tf),
          mtfScore: (s.meta?.mtfScore * 100).toFixed(1) + '%',
          positiveCount: s.meta?.alignment ? Object.values(s.meta.alignment).filter(a => a?.positive).length : 0
        })),
        recommendation: 'Monitor these signals; wait for MTF alignment improvement or additional confluence'
      };

      logger.info(monitorBlock, 'poller.rootTfCandleOpen: RECOMMENDED BLOCK (MONITOR)');
    }

    if (acceptedSignals.length === 0 && monitorSignals.length === 0 && detectedNewCandles) {
      logger.info(
        { timeframes: tfsToProcess },
        'poller.rootTfCandleOpen: RECOMMENDED BLOCK (NO ACTION) - New candles detected but no MACD flips'
      );
    }

    const totalScanDuration = (performance.now() - scanStartTime).toFixed(2);
    logger.info(
      {
        totalScanDurationMs: totalScanDuration,
        broadcastDurationMs: (performance.now() - broadcastStart).toFixed(2)
      },
      'poller.perf: [4/4] Root TF candle open scan completed successfully'
    );

    return detectedNewCandles;
  },

  getLoop2ProcessedCandleKey(symbol, tf) {
    return `poller.loop2.processedCandle.${symbol}.${tf}`;
  },

  stop() {
    isRunning = false;
    logger.info('poller: stopping loops');
  }
};
