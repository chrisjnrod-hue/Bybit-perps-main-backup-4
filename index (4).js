const path = require('path');
require('dotenv').config();

const express = require('express');
const pino = require('pino');

const config = require('./config');
const db = require('./db');
const poller = require('./services/poller');
const wsManager = require('./services/bybitWs');
const signalManager = require('./services/signalManager');
const telegram = require('./services/telegram');
const tradeManager = require('./services/tradeManager');
const debugRoutes = require('./routes/debug');

const logger = pino({
  level: config.LOG_LEVEL || 'info'
});

process.on('uncaughtException', (err) => {
  logger.error(
    { err },
    'UNCAUGHT EXCEPTION - the process may terminate'
  );
});

process.on('unhandledRejection', (reason) => {
  logger.error(
    { reason },
    'UNHANDLED REJECTION - promise rejected without handler'
  );
});

const app = express();

app.use(express.json());
app.use('/debug', debugRoutes);

const PORT = process.env.PORT || config.PORT || 3000;

let server;
let heartbeatInterval;
let startupStarted = false;

function getPersistedSymbols() {
  const dbInstance = db.get();

  return dbInstance
    .prepare(
      `
        SELECT symbol
        FROM symbols
        ORDER BY symbol COLLATE NOCASE ASC
      `
    )
    .all()
    .map((row) => row.symbol)
    .filter(Boolean);
}

function validateSymbolForWs(symbol) {
  const sym = String(symbol || '').toUpperCase();

  if (!sym) {
    return false;
  }

  // Reject dated variants (USDTQ, USDTH, USDTZ, USDT0-9)
  if (/USDT[QHUZ0-9]/.test(sym.slice(-6))) {
    return false;
  }

  // Accept only USDT or USDT.P perpetuals
  if (!/USDT(\.P)?$/.test(sym)) {
    return false;
  }

  return true;
}

async function runStartup() {
  if (startupStarted) {
    logger.warn(
      'Startup already started; ignoring duplicate startup call'
    );

    return;
  }

  startupStarted = true;

  logger.info(
    'Startup: initializing database'
  );

  db.init();

  // Initialize Telegram before any notification can be queued.
  logger.info(
    'Startup: initializing Telegram'
  );

  telegram.init();

  // This probe is intentionally non-blocking.
  try {
    const bybitRest =
      require('./services/bybitRest');

    bybitRest
      .probeHosts(3000)
      .then((base) => {
        if (base) {
          logger.info(
            { base },
            'probeHosts completed in background'
          );
        } else {
          logger.warn(
            'probeHosts completed in background with no selected base'
          );
        }
      })
      .catch((err) => {
        logger.debug(
          { err },
          'probeHosts background failure'
        );
      });
  } catch (err) {
    logger.debug(
      { err },
      'probeHosts startup call failed'
    );
  }

  /*
   * Discover symbols only. Do not start background seeding here because
   * startup must complete its controlled seed before the startup flip pass.
   */
  try {
    logger.info(
      'Startup: discovering symbols'
    );

    await poller.initialScan({
      seed: false
    });
  } catch (err) {
    logger.warn(
      { err },
      'initialScan failed during startup; continuing'
    );
  }

  /*
   * Start the WebSocket manager before signal processing.
   * Symbols are subscribed centrally here, exactly once.
   */
  const wsStarted = wsManager.start();

  if (wsStarted) {
    try {
      const allSymbols =
        getPersistedSymbols();

      // Filter out invalid symbols before subscribing
      const validSymbols = allSymbols.filter(
        (symbol) => {
          return validateSymbolForWs(symbol);
        }
      );

      const invalidCount =
        allSymbols.length -
        validSymbols.length;

      if (invalidCount > 0) {
        logger.warn(
          {
            total: allSymbols.length,
            valid: validSymbols.length,
            invalid: invalidCount,
            samples: allSymbols
              .filter((s) => !validateSymbolForWs(s))
              .slice(0, 5)
          },
          'Startup: filtering invalid symbols before WS subscription'
        );
      }

      const subscribed =
        wsManager.subscribeSymbols(
          validSymbols,
          config.MTF_TFS
        );

      logger.info(
        {
          discoveredSymbols: allSymbols.length,
          validSymbols: validSymbols.length,
          subscribedSymbols: subscribed,
          timeframes: config.MTF_TFS
        },
        'Startup: central WS subscriptions queued'
      );
    } catch (err) {
      logger.warn(
        { err },
        'Startup: central WS subscription setup failed'
      );
    }
  } else {
    logger.warn(
      'Startup: WS disabled; REST polling fallback remains active'
    );
  }

  /*
   * Register listeners once before live klines arrive.
   */
  tradeManager.registerWs(wsManager);
  signalManager.start();

  /*
   * Seed the configured startup subset synchronously. This prevents
   * scanAllForStartup() from racing with the background seed started by
   * initialScan().
   */
  try {
    const startupSeedCount = Number(
      process.env.STARTUP_SEED_SYMBOLS ||
      config.STARTUP_SEED_SYMBOLS ||
      50
    );

    const dbInstance = db.get();

    const rows = dbInstance
      .prepare(
        `
          SELECT symbol
          FROM symbols
          ORDER BY symbol COLLATE NOCASE ASC
          LIMIT ?
        `
      )
      .all(startupSeedCount);

    const seedList = rows
      .map((row) => ({
        symbol: row.symbol
      }))
      .filter((item) =>
        validateSymbolForWs(item.symbol)
      );

    if (
      seedList.length > 0 &&
      typeof poller.backgroundSeedKlines ===
      'function'
    ) {
      logger.info(
        {
          count: seedList.length
        },
        'Startup: synchronously seeding startup symbols'
      );

      await poller.backgroundSeedKlines(seedList);
    } else {
      logger.info(
        'Startup: no valid symbols available for targeted seeding'
      );
    }
  } catch (err) {
    logger.warn(
      { err },
      'Startup: targeted seeding failed; continuing'
    );
  }

  /*
   * This is the only startup signal scan.
   *
   * scanAllForStartup() enqueues the startup batch. The notification queue
   * owns delivery of the startup summary. Do not call
   * signalManager.sendStartupSummary() here as well.
   */
  try {
    logger.info(
      'Startup: running full startup flip pass'
    );

    await poller.scanAllForStartup();

    logger.info(
      'Startup: full startup flip pass completed'
    );
  } catch (err) {
    logger.warn(
      { err },
      'Startup: full startup flip pass failed; continuing'
    );
  }

  /*
   * Start the recurring five-minute boundary loop last.
   */
  poller.start();

  app.get('/', (req, res) => {
    res.json({
      ok: true,
      version: '0.3.0'
    });
  });

  server = app.listen(PORT, () => {
    logger.info(
      { PORT },
      'Server listening'
    );
  });

  heartbeatInterval = setInterval(() => {
    logger.info(
      {
        ts: new Date().toISOString()
      },
      'heartbeat'
    );
  }, 60_000);

  logger.info('Startup complete');
}

async function start() {
  try {
    logger.info('Starting app');
    await runStartup();
  } catch (err) {
    logger.error(
      { err },
      'Failed to start application'
    );

    process.exit(1);
  }
}

async function gracefulShutdown(signal) {
  logger.info(
    { signal },
    'Starting graceful shutdown'
  );

  try {
    if (heartbeatInterval) {
      clearInterval(heartbeatInterval);
      heartbeatInterval = null;
    }

    if (
      server &&
      typeof server.close === 'function'
    ) {
      logger.info(
        'Closing HTTP server'
      );

      await new Promise((resolve) => {
        server.close(resolve);
      });
    }

    try {
      if (
        wsManager &&
        typeof wsManager.closeAll ===
        'function'
      ) {
        await wsManager.closeAll();

        logger.info(
          'WS manager closed all connections'
        );
      } else if (
        wsManager &&
        Array.isArray(wsManager.connections)
      ) {
        wsManager.connections.forEach(
          (connection) => {
            try {
              if (connection.ws) {
                connection.ws.close();
              }
            } catch (_) {
              // Ignore individual connection close failures.
            }
          }
        );
      }
    } catch (err) {
      logger.warn(
        { err },
        'Failed to close WS manager cleanly'
      );
    }

    try {
      if (
        db &&
        typeof db.close === 'function'
      ) {
        db.close();
        logger.info('Database closed');
      } else {
        const dbInstance = db.get();

        if (
          dbInstance &&
          typeof dbInstance.close ===
          'function'
        ) {
          dbInstance.close();
          logger.info(
            'Database closed through db.get()'
          );
        }
      }
    } catch (err) {
      logger.warn(
        { err },
        'Error closing database'
      );
    }

    await new Promise((resolve) => {
      setTimeout(resolve, 500);
    });
  } catch (err) {
    logger.error(
      { err },
      'Error during graceful shutdown'
    );
  } finally {
    logger.info(
      'Shutdown complete, exiting process'
    );

    process.exit(0);
  }
}

process.on('SIGTERM', () => {
  gracefulShutdown('SIGTERM');
});

process.on('SIGINT', () => {
  gracefulShutdown('SIGINT');
});

start();
