const dbModule = require('../db');
const config = require('../config');
const logger = require('pino')();
const bybit = require('./bybitRest');

let wsRegistered = false;

function safeParseMeta(meta) {
  try {
    return meta ? JSON.parse(meta) : {};
  } catch (e) {
    return {};
  }
}

module.exports = {
  async openTrade({ symbol, root_tf, alignment }) {
    const db = dbModule.get();
    const openCount = db
      .prepare(
        'SELECT COUNT(*) as c FROM trades WHERE status = ?'
      )
      .get('open').c || 0;

    if (openCount >= config.MAX_OPEN_TRADES) {
      logger.info(
        { openCount },
        'max open trades reached'
      );

      return null;
    }

    let balanceInfo = null;

    try {
      balanceInfo =
        await bybit.getWalletBalance('USDT');
    } catch (err) {
      // ignore
    }

    const available = Number(
      balanceInfo?.available ||
      balanceInfo?.availableBalance ||
      balanceInfo?.balance ||
      0
    );

    const perTradeAllocation =
      available > 0
        ? available / Math.max(1, config.MAX_OPEN_TRADES)
        : null;

    const entryPrice =
      await this.fetchPrice(symbol);

    if (!entryPrice || entryPrice <= 0) {
      logger.warn(
        { symbol },
        'No entry price available; abort openTrade'
      );

      return null;
    }

    let qty = 1;

    if (perTradeAllocation) {
      const rawQty =
        perTradeAllocation / entryPrice;

      qty = Math.max(
        0.0001,
        Number(rawQty.toFixed(4))
      );
    }

    const side = 'Buy';
    const tpPrice =
      entryPrice * (1 + config.DEFAULT_TP_PERCENT / 100);
    const slPrice =
      entryPrice * (1 - config.DEFAULT_SL_PERCENT / 100);

    const meta = {
      root_tf,
      alignment,
      balanceInfo: { available, perTradeAllocation },
      breakeven_started: false,
      breakeven_applied: false
    };

    const record = db
      .prepare(
        'INSERT INTO trades (symbol, opened_at, side, size, entry_price, tp, sl, status, meta) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
      )
      .run(
        symbol,
        Date.now(),
        side,
        qty,
        entryPrice,
        tpPrice,
        slPrice,
        'opening',
        JSON.stringify(meta)
      );

    const tradeId = record.lastInsertRowid;

    if (!config.OPENTRADE) {
      db
        .prepare(
          'UPDATE trades SET status = ? WHERE id = ?'
        )
        .run('open', tradeId);

      logger.info(
        { tradeId, symbol, qty, entryPrice },
        'OPENTRADE disabled — recorded as open'
      );

      return tradeId;
    }

    try {
      const orderResp =
        await bybit.placeMarketOrderV5({
          category: 'linear',
          symbol,
          side,
          qty,
          reduceOnly: false,
          tp: tpPrice,
          sl: slPrice
        });

      db
        .prepare(
          'UPDATE trades SET status = ?, meta = ? WHERE id = ?'
        )
        .run(
          'open',
          JSON.stringify({
            ...meta,
            orderResp
          }),
          tradeId
        );

      logger.info(
        { tradeId, symbol },
        'Order placed and recorded'
      );

      return tradeId;
    } catch (err) {
      logger.error(
        { err, symbol },
        'Failed to place order'
      );

      db
        .prepare(
          'UPDATE trades SET status = ?, meta = ? WHERE id = ?'
        )
        .run(
          'failed',
          JSON.stringify({
            ...meta,
            err: String(err)
          }),
          tradeId
        );

      return null;
    }
  },

  async fetchPrice(symbol) {
    const db = dbModule.get();
    const row = db
      .prepare(
        'SELECT close FROM klines WHERE symbol=? ORDER BY open_time DESC LIMIT 1'
      )
      .get(symbol);

    if (row) {
      return row.close;
    }

    const srow = db
      .prepare(
        'SELECT price FROM symbols WHERE symbol = ?'
      )
      .get(symbol);

    if (srow && srow.price) {
      return srow.price;
    }

    return 0;
  },

  registerWs(wsManager) {
    if (wsRegistered) {
      logger.debug(
        'tradeManager: WebSocket listener already registered'
      );

      return;
    }

    if (
      !wsManager ||
      typeof wsManager.on !== 'function'
    ) {
      logger.warn(
        'tradeManager.registerWs called with invalid wsManager'
      );

      return;
    }

    wsManager.on('kline', async (kline) => {
      if (!kline || !kline.symbol) {
        return;
      }

      try {
        /*
         * bybitWs emits the kline fields directly:
         *
         * {
         *   symbol,
         *   timeframe,
         *   open_time,
         *   open,
         *   high,
         *   low,
         *   close,
         *   volume,
         *   confirm,
         *   raw
         * }
         */
        await this.evaluateBreakevenForSymbol(
          kline.symbol,
          kline.timeframe,
          kline
        );
      } catch (err) {
        logger.debug(
          {
            err,
            symbol: kline.symbol,
            timeframe: kline.timeframe
          },
          'tradeManager: error evaluating breakeven on kline'
        );
      }
    });

    wsRegistered = true;

    logger.info(
      'tradeManager: registered one WS kline listener for breakeven handling'
    );
  },

  async closeLeastProfitableTrade() {
    if (!config.CLOSE_LEAST_PROFITABLE_ENABLED) {
      return;
    }

    const db = dbModule.get();
    const openTrades = db
      .prepare(
        'SELECT * FROM trades WHERE status = ? ORDER BY id ASC'
      )
      .all('open');

    if (!openTrades || openTrades.length === 0) {
      logger.debug(
        'closeLeastProfitableTrade: no open trades'
      );

      return;
    }

    let leastProfitable = null;
    let minProfit = Infinity;

    for (const t of openTrades) {
      const currentPrice =
        await this.fetchPrice(t.symbol);

      if (!currentPrice || currentPrice <= 0) {
        continue;
      }

      const isLong =
        String(t.side || '')
          .toLowerCase() !== 'sell';

      let profit = 0;

      if (isLong) {
        profit =
          (currentPrice - t.entry_price) *
          t.size;
      } else {
        profit =
          (t.entry_price - currentPrice) *
          t.size;
      }

      if (profit < minProfit) {
        minProfit = profit;
        leastProfitable = t;
      }
    }

    if (!leastProfitable) {
      logger.info(
        'closeLeastProfitableTrade: no valid trades to close'
      );

      return;
    }

    logger.info(
      {
        tradeId: leastProfitable.id,
        symbol: leastProfitable.symbol,
        profit: minProfit
      },
      'Closing least profitable trade before boundary'
    );

    try {
      if (config.OPENTRADE) {
        await bybit.closePosition({
          category: 'linear',
          symbol: leastProfitable.symbol,
          side:
            leastProfitable.side === 'Buy'
              ? 'Sell'
              : 'Buy'
        });
      }

      db
        .prepare(
          'UPDATE trades SET status = ? WHERE id = ?'
        )
        .run('closed', leastProfitable.id);

      logger.info(
        {
          tradeId: leastProfitable.id,
          symbol: leastProfitable.symbol
        },
        'Trade closed successfully'
      );
    } catch (err) {
      logger.error(
        {
          err,
          tradeId: leastProfitable.id
        },
        'Failed to close least profitable trade'
      );
    }
  },

  async evaluateBreakevenForSymbol(symbol, tf, klineData) {
    const db = dbModule.get();
    const openTrades = db
      .prepare(
        'SELECT * FROM trades WHERE symbol = ? AND status = ?'
      )
      .all(symbol, 'open');

    if (!openTrades || openTrades.length === 0) {
      return;
    }

    const currentPrice =
      klineData &&
      (klineData.close || klineData.c)
        ? Number(klineData.close || klineData.c)
        : await this.fetchPrice(symbol);

    if (!currentPrice || currentPrice <= 0) {
      return;
    }

    for (const t of openTrades) {
      const meta = safeParseMeta(t.meta);
      const effectiveMode =
        config.BREAK_EVEN_MODE === 'off' &&
        config.BREAK_EVEN_ACTIVE
          ? 'fixed'
          : config.BREAK_EVEN_MODE;

      if (effectiveMode === 'off') {
        continue;
      }

      const isLong =
        String(t.side || '')
          .toLowerCase() !== 'sell';

      let gainPct = 0;

      if (isLong) {
        gainPct =
          ((currentPrice - t.entry_price) /
            t.entry_price) *
          100;
      } else {
        gainPct =
          ((t.entry_price - currentPrice) /
            t.entry_price) *
          100;
      }

      if (
        gainPct <
        config.BREAK_EVEN_TRIGGER_PERCENT
      ) {
        continue;
      }

      if (effectiveMode === 'fixed') {
        if (meta.breakeven_applied) {
          continue;
        }

        const newSl = isLong
          ? t.entry_price *
            (1 + config.BREAK_EVEN_PERCENT / 100)
          : t.entry_price *
            (1 - config.BREAK_EVEN_PERCENT / 100);

        await this.applyNewStopLoss(
          t,
          newSl,
          meta,
          true
        );
      } else if (
        effectiveMode === 'trailing_lower_highs'
      ) {
        const entryBasedSl = isLong
          ? t.entry_price *
            (1 + config.BREAK_EVEN_PERCENT / 100)
          : t.entry_price *
            (1 - config.BREAK_EVEN_PERCENT / 100);

        if (!meta.breakeven_started) {
          await this.applyNewStopLoss(
            t,
            entryBasedSl,
            meta,
            false
          );

          meta.breakeven_started = true;
          meta.breakeven_applied = true;
          db
            .prepare(
              'UPDATE trades SET meta = ? WHERE id = ?'
            )
            .run(
              JSON.stringify(meta),
              t.id
            );

          continue;
        }

        const lookback = Math.max(
          2,
          config.TRAILING_LOOKBACK || 3
        );

        const rows = db
          .prepare(
            'SELECT high, low, open_time FROM klines WHERE symbol = ? AND timeframe = ? ORDER BY open_time DESC LIMIT ?'
          )
          .all(symbol, tf, lookback);

        if (!rows || rows.length < 2) {
          continue;
        }

        let lowerHighDetected = true;

        for (let i = 1; i < rows.length; i++) {
          if (
            !(rows[i - 1].high < rows[i].high)
          ) {
            lowerHighDetected = false;
            break;
          }
        }

        if (!lowerHighDetected) {
          continue;
        }

        const candidateSl = rows[0].low;
        const currentSL = Number(t.sl || 0);
        let shouldApply = false;
        let newSl = currentSL;

        if (isLong) {
          if (
            candidateSl > currentSL &&
            candidateSl <= currentPrice
          ) {
            newSl = candidateSl;
            shouldApply = true;
          }

          if (
            entryBasedSl > currentSL &&
            entryBasedSl <= currentPrice
          ) {
            if (entryBasedSl > newSl) {
              newSl = entryBasedSl;
              shouldApply = true;
            }
          }
        } else {
          if (
            candidateSl < currentSL &&
            candidateSl >= currentPrice
          ) {
            newSl = candidateSl;
            shouldApply = true;
          }

          if (
            entryBasedSl < currentSL &&
            entryBasedSl >= currentPrice
          ) {
            if (entryBasedSl < newSl) {
              newSl = entryBasedSl;
              shouldApply = true;
            }
          }
        }

        if (shouldApply && newSl !== currentSL) {
          await this.applyNewStopLoss(
            t,
            newSl,
            meta,
            false
          );
        }
      }
    }
  },

  async applyNewStopLoss(
    tradeRow,
    newSl,
    meta,
    finalize
  ) {
    const db = dbModule.get();
    const tId = tradeRow.id;

    meta = meta || safeParseMeta(tradeRow.meta);

    try {
      if (config.OPENTRADE) {
        const resp =
          await bybit.setPositionTradingStop({
            symbol: tradeRow.symbol,
            stopLoss: newSl
          });

        meta.breakeven_applied =
          meta.breakeven_applied || finalize;
        meta.breakeven_started = true;
        meta.lastBreakevenAt = Date.now();
        meta.lastBreakevenResp = resp;

        db
          .prepare(
            'UPDATE trades SET sl = ?, meta = ? WHERE id = ?'
          )
          .run(
            newSl,
            JSON.stringify(meta),
            tId
          );

        logger.info(
          {
            tradeId: tId,
            symbol: tradeRow.symbol,
            newSl,
            resp
          },
          'SL updated on exchange and in DB'
        );
      } else {
        meta.breakeven_applied =
          meta.breakeven_applied || finalize;
        meta.breakeven_started = true;
        meta.lastBreakevenAt = Date.now();

        db
          .prepare(
            'UPDATE trades SET sl = ?, meta = ? WHERE id = ?'
          )
          .run(
            newSl,
            JSON.stringify(meta),
            tId
          );

        logger.info(
          {
            tradeId: tId,
            symbol: tradeRow.symbol,
            newSl
          },
          'Local SL updated (OPENTRADE=false)'
        );
      }
    } catch (err) {
      logger.warn(
        { err, tradeId: tId, newSl },
        'Failed to set trading-stop; will retry on next kline event'
      );
    }
  }
};
