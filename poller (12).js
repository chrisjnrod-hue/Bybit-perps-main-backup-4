const dbModule = require('../db');
const bybit = require('./bybitRest');
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

  /*
   * Exclude dated contracts and other symbols where characters follow
   * the USDT suffix.
   */
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

  // Align to a UTC-based interval boundary
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

  return (
    (previousHistogram <= 0 && currentHistogram > 0) ||
    (previousHistogram >= 0 && currentHistogram < 0)
  );
}

async function detectMidCandleFlip(symbol, timeframe) {
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
      'poller: isMacdFlip fallback failed'
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

module.exports = {
  /*
   * Start only the recurring polling loop.
   *
   * Startup discovery, startup seeding, and the startup flip scan are
   * deliberately owned by index.js. Do not run initialScan() or
   * scanAllForStartup() here.
   */
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
    const {
      seed = true
    } = options;

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

    insertMany(
      allSymbols.filter((symbolInfo) =>
        symbolInfo &&
        symbolInfo.symbol
      )
    );

    logger.info(
      {
        total: allSymbols.length
      },
      'poller.initialScan: symbols persisted'
    );

    const seedSymbols =
      bybit.getSeedSymbols(allSymbols);

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

    return allSymbols;
  },

  async performWsInitialScan() {
    try {
      const wsManager = require('./bybitWs');

      if (
        wsManager &&
        typeof wsManager.performInitialScan === 'function'
      ) {
        const result =
          await wsManager.performInitialScan();

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
        return limiter.schedule(() =>
          this.seedKlinesForSymbol(item.symbol)
        );
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

          /*
           * This repository currently exposes computeMacdHistogram()
           * rather than computeAndStoreMacd(). Warm the calculation when
           * available, but do not fail seeding if MACD warm-up fails.
           */
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

      const invalidCount =
        rows.length - validRows.length;

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

        notificationQueue.enqueueStartupBatch(
          newSignals
        );
      } else {
        logger.info(
          'scanAllForStartup: no startup signals found'
        );
      }

      /*
       * The startup pass has already examined the latest available
       * candle. Loop 2 must not treat that existing candle as new at
       * the first five-minute boundary after deployment.
       */
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
    const boundary = new Date();

    logger.info(
      {
        boundary: boundary.toISOString()
      },
      'poller.loop2: starting five-minute boundary scan (mid-candle detection)'
    );

    await this.initialScan({
      seed: false
    });

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

    for (const row of validRows) {
      const symbol = row.symbol;

      for (const tf of rootTfs) {
        try {
          await this.seedKlinesForSymbol(
            symbol,
            tf
          );

          const latestOpen = getLatestOpenTime(
            db,
            symbol,
            tf
          );

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

          /*
           * Root candle-open detection is handled by the dedicated
           * root-TF candle-open loop. This loop should ignore all
           * newly opened candles that are already being processed
           * by the root-open loop.
           */
          if (latestOpen > processedOpen) {
            logger.debug(
              {
                symbol,
                tf,
                latestOpen,
                processedOpen
              },
              'poller.loop2: new root candle detected; handled separately by root-TF candle-open scanner'
            );

            continue;
          }

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
              const eventId = buildEventId(
                'midcandle',
                symbol,
                tf,
                latestOpen
              );

              const signal =
                await signalManager.handleRootSignal({
                  symbol,
                  root_tf: tf,
                  detected_at: Date.now(),
                  candle_open_time: latestOpen,
                  eventId,
                  signalType: 'midcandle_update',
                  notifyImmediately: false
                });

              if (signal) {
                dbModule.setState(
                  midCandleStateKey,
                  Date.now()
                );

                newSignals.push({
                  ...signal,
                  eventId,
                  notificationType: 'midcandle_update'
                });

                logger.info(
                  {
                    symbol,
                    tf,
                    latestOpen,
                    eventId
                  },
                  'poller.loop2: mid-candle histogram flip detected'
                );
              }
            }
          }

          const latestSignals =
            dbModule.getLatestSignalsSnapshot();

          const activeSignals =
            latestSignals.filter((signal) => {
              return (
                signal.symbol === symbol &&
                signal.root_tf === tf
              );
            });

          if (activeSignals.length === 0) {
            continue;
          }

          const alignment =
            await signalManager.evaluateMtfAlignment(
              symbol
            );

          const alignmentStateKey =
            `poller.loop2.alignment.${symbol}.${tf}`;

          const previousAlignment =
            dbModule.getState(
              alignmentStateKey
            );

          const nextAlignmentJson =
            JSON.stringify(alignment || {});

          if (
            previousAlignment === nextAlignmentJson
          ) {
            continue;
          }

          dbModule.setState(
            alignmentStateKey,
            alignment || {}
          );

          const alignmentValues =
            Object.values(alignment || {});

          const positiveCount =
            alignmentValues.filter((value) => {
              return value && value.positive;
            }).length;

          const alignmentCount =
            alignmentValues.length;

          alignmentAlerts.push({
            symbol,
            root_tf: tf,
            detected_at: Date.now(),
            state: 'monitor',
            meta: {
              alignment: alignment || {},
              decision: 'monitor',
              acceptReason: 'mtf_alignment_alert',
              tvScore: 0,
              tvSource: 'loop2',
              mtfScore: alignmentCount > 0
                ? positiveCount / alignmentCount
                : 0
            }
          });
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

    /*
     * Loop 2 sends one detail block per new mid-candle signal and one
     * block per alignment alert. It does not send a summary batch.
     */
    for (const signal of newSignals) {
      try {
        notificationQueue.enqueueSignal(
          signal,
          'midcandle_update'
        );

        logger.info(
          {
            symbol: signal.symbol,
            root_tf: signal.root_tf
          },
          'poller.loop2: midcandle update enqueued'
        );
      } catch (err) {
        logger.warn(
          {
            err,
            signal
          },
          'poller.loop2: failed to enqueue midcandle update'
        );
      }
    }

    for (const alert of alignmentAlerts) {
      try {
        notificationQueue.enqueueSignal(
          alert,
          'mtf_alignment'
        );

        logger.info(
          {
            symbol: alert.symbol,
            root_tf: alert.root_tf
          },
          'poller.loop2: realtime alignment alert enqueued'
        );
      } catch (err) {
        logger.warn(
          {
            err,
            alert
          },
          'poller.loop2: failed to enqueue alignment alert'
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
      const nextSchedules = {};

      for (const tf of rootTfs) {
        nextSchedules[tf] = getNextTimeframeBoundaryMs(tf);
      }

      const nextBoundaryMs = Math.min(
        ...Object.values(nextSchedules)
      );

      const delay = Math.max(
        0,
        nextBoundaryMs - Date.now()
      );

      logger.debug(
        {
          nextBoundaryMs,
          nextBoundary: new Date(nextBoundaryMs).toISOString(),
          delayMs: delay,
          timeframes: rootTfs
        },
        'poller: waiting for next root TF candle-open boundary'
      );

      await sleep(delay);

      if (!isRunning) {
        break;
      }

      const nowMs = Date.now();
      const openingTfs = [];

      for (const [tf, boundaryMs] of Object.entries(nextSchedules)) {
        /*
         * Catch slight scheduling drift. Wake up slightly early or
         * a bit late and still treat the boundary as "open".
         */
        if (
          nowMs >= boundaryMs - 5000 &&
          nowMs - boundaryMs < 60000
        ) {
          openingTfs.push(tf);
        }
      }

      if (openingTfs.length > 0) {
        try {
          await this.runRootTfCandleOpenOnce(openingTfs);
        } catch (err) {
          logger.error(
            {
              err
            },
            'poller: root TF candle open scan failed'
          );
        }
      }
    }
  },

  async runRootTfCandleOpenOnce(openingTfs = []) {
    if (!Array.isArray(openingTfs) || openingTfs.length === 0) {
      return;
    }

    logger.info(
      {
        timeframes: openingTfs
      },
      'poller: root TF candle-open scan started'
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

    const tfSignalMap = {};
    for (const tf of openingTfs) {
      tfSignalMap[tf] = [];
    }

    for (const row of validRows) {
      const symbol = row.symbol;

      for (const tf of openingTfs) {
        try {
          await this.seedKlinesForSymbol(
            symbol,
            tf
          );

          const latestOpen = getLatestOpenTime(
            db,
            symbol,
            tf
          );

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
            const flip =
              await macdUtil.isMacdFlip(
                symbol,
                tf
              );

            if (flip) {
              const eventId = buildEventId(
                'root_open',
                symbol,
                tf,
                latestOpen
              );

              const signal =
                await signalManager.handleRootSignal({
                  symbol,
                  root_tf: tf,
                  detected_at: Date.now(),
                  candle_open_time: latestOpen,
                  eventId,
                  signalType: 'new_root_candle',
                  notifyImmediately: false
                });

              if (signal) {
                tfSignalMap[tf].push({
                  ...signal,
                  eventId,
                  notificationType: 'new_root_candle'
                });
              }
            }

            dbModule.setState(
              processedStateKey,
              latestOpen
            );
          }
        } catch (err) {
          logger.debug(
            {
              err,
              symbol,
              tf
            },
            'poller: root TF candle-open scan failed for symbol/timeframe'
          );
        }
      }
    }

    for (const tf of openingTfs) {
      const signals = tfSignalMap[tf] || [];

      if (signals.length === 0) {
        logger.info(
          {
            tf
          },
          'poller: no root TF signals detected at candle open'
        );

        continue;
      }

      try {
        notificationQueue.enqueueRootCandleOpenBatch(
          signals,
          tf
        );

        logger.info(
          {
            tf,
            count: signals.length
          },
          'poller: root TF candle-open batch enqueued'
        );
      } catch (err) {
        logger.warn(
          {
            err,
            tf
          },
          'poller: failed to enqueue root TF candle-open batch'
        );
      }
    }
  },

  getLoop2ProcessedCandleKey(symbol, tf) {
    return `poller.loop2.lastRootOpen.${symbol}.${tf}`;
  }
};
