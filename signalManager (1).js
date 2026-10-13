const dbModule = require('../db');
const macd = require('./macd');
const tradeManager = require('./tradeManager');
const marketData = require('./marketData');
const tradingview = require('./tradingview');
const notificationQueue = require('./notificationQueue');
const config = require('../config');
const logger = require('pino')();

let openTradesAllowed = true;

function setOpenTradesAllowed(value) {
  openTradesAllowed = !!value;

  logger.info(
    { openTradesAllowed },
    'signalManager: openTradesAllowed set'
  );
}

/*
 * This lock only prevents the same event from being processed
 * concurrently. It must not use only symbol:root_tf because a symbol
 * can legitimately produce:
 *
 *   root_open:BTCUSDT:60:candle
 *   midcandle:BTCUSDT:60:candle
 *
 * during the same root candle.
 */
const inProgress = new Map();

function normalizeEventType(signalType) {
  if (
    typeof signalType === 'string' &&
    signalType.trim()
  ) {
    return signalType.trim();
  }

  return null;
}

function buildEventKey({
  symbol,
  root_tf,
  eventId,
  candle_open_time,
  detected_at
}) {
  const eventPart =
    eventId !== null &&
    eventId !== undefined &&
    String(eventId).trim()
      ? String(eventId).trim()
      : (
          candle_open_time !== null &&
          candle_open_time !== undefined &&
          Number.isFinite(Number(candle_open_time))
            ? `candle:${Number(candle_open_time)}`
            : `detected:${Number(detected_at)}`
        );

  return [
    String(symbol || ''),
    String(root_tf || ''),
    eventPart
  ].join(':');
}

function buildFallbackMarketData() {
  return {
    price: 0,
    volume_24h_usdt: 0,
    volume_change_pct: null,
    market_cap: null
  };
}

function calculateMtfScore(alignment) {
  const values = Object.values(alignment || {});

  if (values.length === 0) {
    return 0;
  }

  const positiveCount = values.filter((value) => {
    return value && value.positive;
  }).length;

  return positiveCount / values.length;
}

module.exports = {
  start() {
    logger.info('SignalManager started');
  },

  setOpenTradesAllowed,

  /**
   * Process a root, root-open, or mid-candle signal.
   *
   * notifyImmediately:
   *   true  - enqueue immediately
   *   false - return the signal to the caller so the caller can batch it
   *
   * signalType:
   *   new_root_candle
   *   midcandle_update
   *   mtf_alignment
   *   or another queue-supported type
   */
  async handleRootSignal({
    symbol,
    root_tf,
    detected_at = Date.now(),
    candle_open_time = null,
    eventId = null,
    signalType = null,
    notifyImmediately = true
  } = {}) {
    if (!symbol || !root_tf) {
      logger.warn(
        {
          symbol,
          root_tf
        },
        'handleRootSignal: symbol and root_tf are required'
      );

      return null;
    }

    const normalizedSignalType =
      normalizeEventType(signalType);

    const key = buildEventKey({
      symbol,
      root_tf,
      eventId,
      candle_open_time,
      detected_at
    });

    if (inProgress.has(key)) {
      logger.debug(
        { key, symbol, root_tf, eventId },
        'handleRootSignal: event already in progress'
      );

      return null;
    }

    inProgress.set(key, true);

    try {
      logger.info(
        {
          symbol,
          root_tf,
          eventId,
          signalType: normalizedSignalType,
          candle_open_time
        },
        'Root signal received'
      );

      /*
       * Fetch fresh market data. Failure is non-fatal because the
       * signal should still be available for notification and review.
       */
      let mdata = null;

      try {
        mdata =
          await marketData.updateSymbolMarketData(
            symbol
          );

        if (!mdata) {
          mdata = buildFallbackMarketData();
        }
      } catch (err) {
        logger.warn(
          {
            err,
            symbol
          },
          'handleRootSignal: market data fetch failed, using zeros'
        );

        mdata = buildFallbackMarketData();
      }

      /*
       * Fetch the cached or fresh TradingView rating.
       */
      let tv = {
        score: 0,
        source: 'error'
      };

      try {
        logger.debug(
          { symbol },
          'handleRootSignal: fetching TV rating'
        );

        const tvRes =
          await tradingview.getOrFetchTvRatingCached(
            symbol
          );

        if (
          tvRes &&
          typeof tvRes.score === 'number'
        ) {
          tv = {
            score: tvRes.score,
            source: tvRes.source || 'unknown'
          };

          logger.info(
            {
              symbol,
              score: tv.score,
              source: tv.source
            },
            'TV rating acquired'
          );
        } else {
          logger.warn(
            {
              symbol
            },
            'TV rating fetch returned invalid result'
          );
        }
      } catch (err) {
        logger.warn(
          {
            err: err && err.message,
            symbol
          },
          'handleRootSignal: TV rating fetch failed'
        );
      }

      /*
       * Evaluate MTF alignment using the current MACD histogram data.
       */
      const alignment =
        await this.evaluateMtfAlignment(symbol);

      const mtfScore =
        calculateMtfScore(alignment);

      const decision =
        await this.applyDecision(alignment);

      const meta = {
        tvScore: tv.score || 0,
        tvSource: tv.source || 'error',
        mtfScore,
        alignment,
        acceptReason:
          decision && decision.reason
            ? decision.reason
            : null,
        decision:
          decision && decision.decision
            ? decision.decision
            : 'monitor',
        marketData: mdata || {}
      };

      /*
       * Keep this insert compatible with the existing database API.
       * Event metadata remains on signalObj and is available to the
       * notification queue even if the current schema has no eventId
       * or candle_open_time columns.
       */
      dbModule.insertSignal({
        symbol,
        root_tf,
        detected_at,
        state: 'detected',
        meta
      });

      const signalObj = {
        key,
        symbol,
        root_tf,
        detected_at,
        candle_open_time,
        eventId,
        signalType: normalizedSignalType,
        notificationType: normalizedSignalType,
        state: 'detected',
        meta
      };

      /*
       * Immediate notifications are used by callers that do not need
       * a summary batch. Boundary and root-open scans pass false and
       * enqueue the returned object themselves.
       */
      if (notifyImmediately) {
        const queueType =
          normalizedSignalType || 'realtime';

        logger.debug(
          {
            symbol,
            root_tf,
            eventId,
            queueType,
            notificationType: signalObj.notificationType
          },
          'handleRootSignal: enqueuing immediate notification'
        );

        notificationQueue.enqueueSignal(
          signalObj,
          queueType
        );
      } else {
        logger.debug(
          {
            symbol,
            root_tf,
            eventId,
            signalType: normalizedSignalType
          },
          'handleRootSignal: notifyImmediately=false, returning signal'
        );
      }

      /*
       * Trade opening is still performed only for accepted signals.
       * Existing tradeManager safeguards should determine whether an
       * already-open trade is allowed.
       */
      if (
        decision &&
        decision.decision === 'accept'
      ) {
        if (!config.OPENTRADE) {
          logger.info(
            {
              symbol,
              root_tf,
              eventId
            },
            'Accept but OPENTRADE disabled; skipping openTrade'
          );
        } else if (!openTradesAllowed) {
          logger.info(
            {
              symbol,
              root_tf,
              eventId
            },
            'Accept but open trades are not yet enabled'
          );
        } else {
          let passFilters = true;

          if (config.MIN_MARKET_CAP > 0) {
            if (
              !mdata ||
              !mdata.market_cap ||
              Number(mdata.market_cap) <
                config.MIN_MARKET_CAP
            ) {
              passFilters = false;

              logger.info(
                {
                  symbol,
                  market_cap: mdata?.market_cap
                },
                'Filtered out by MIN_MARKET_CAP for opening'
              );
            }
          }

          if (config.MIN_24H_USDT_VOLUME > 0) {
            if (
              !mdata ||
              !mdata.volume_24h_usdt ||
              Number(mdata.volume_24h_usdt) <
                config.MIN_24H_USDT_VOLUME
            ) {
              passFilters = false;

              logger.info(
                {
                  symbol,
                  volume_24h_usdt:
                    mdata?.volume_24h_usdt
                },
                'Filtered out by MIN_24H_USDT_VOLUME for opening'
              );
            }
          }

          if (
            Number.isFinite(
              Number(config.MIN_24H_VOLUME_CHANGE_PCT)
            )
          ) {
            const minimumVolumeChange =
              Number(config.MIN_24H_VOLUME_CHANGE_PCT);

            const change =
              mdata?.volume_change_pct;

            if (
              change === null ||
              change === undefined
            ) {
              if (minimumVolumeChange > 0) {
                passFilters = false;

                logger.info(
                  {
                    symbol
                  },
                  'No previous volume available; filtered by MIN_24H_VOLUME_CHANGE_PCT'
                );
              }
            } else if (
              Number(change) <
              minimumVolumeChange
            ) {
              passFilters = false;

              logger.info(
                {
                  symbol,
                  volume_change_pct: change
                },
                'Filtered out by MIN_24H_VOLUME_CHANGE_PCT for opening'
              );
            }
          }

          if (passFilters) {
            try {
              await tradeManager.openTrade({
                symbol,
                root_tf,
                alignment,
                meta,
                eventId,
                signalType: normalizedSignalType
              });

              logger.info(
                {
                  symbol,
                  root_tf,
                  eventId
                },
                'handleRootSignal: trade opening initiated'
              );
            } catch (err) {
              logger.error(
                {
                  err,
                  symbol,
                  root_tf,
                  eventId
                },
                'handleRootSignal: openTrade error'
              );
            }
          } else {
            logger.info(
              {
                symbol,
                root_tf,
                eventId
              },
              'Accepted signal blocked by market filters'
            );
          }
        }
      }

      return signalObj;
    } catch (err) {
      logger.error(
        {
          err,
          symbol,
          root_tf,
          eventId,
          signalType: normalizedSignalType
        },
        'handleRootSignal error'
      );

      return null;
    } finally {
      /*
       * Delete only this event's lock. A later root-open or mid-candle
       * event for the same symbol/timeframe remains independent.
       */
      inProgress.delete(key);
    }
  },

  /**
   * Return detailed MTF alignment for the configured MTF timeframes.
   */
  async evaluateMtfAlignment(symbol) {
    const result = {};
    const mtfTfs = Array.isArray(config.MTF_TFS)
      ? config.MTF_TFS
      : [];

    for (const configuredTf of mtfTfs) {
      const tf = String(configuredTf);

      try {
        const hist =
          await macd.computeMacdHistogram(
            symbol,
            tf
          );

        if (
          !Array.isArray(hist) ||
          hist.length === 0
        ) {
          result[tf] = {
            ok: false,
            positive: false
          };

          continue;
        }

        const last = hist[hist.length - 1];
        const previous =
          hist[hist.length - 2] || last;

        const lastHistogram =
          Number(last?.histogram);

        const previousHistogram =
          Number(previous?.histogram);

        result[tf] = {
          histogram: lastHistogram,
          macd: last?.MACD,
          signal: last?.signal,
          rising:
            Number.isFinite(lastHistogram) &&
            Number.isFinite(previousHistogram)
              ? lastHistogram > previousHistogram
              : false,
          positive:
            Number.isFinite(lastHistogram)
              ? lastHistogram > 0
              : false,
          ok:
            Number.isFinite(lastHistogram)
        };
      } catch (err) {
        logger.debug(
          {
            err,
            symbol,
            tf
          },
          'evaluateMtfAlignment error for timeframe'
        );

        result[tf] = {
          ok: false,
          positive: false
        };
      }
    }

    return result;
  },

  /**
   * Determine signal acceptance from MTF alignment.
   *
   * Rules:
   * - all configured timeframes positive: accept
   * - only daily timeframe negative and rising: accept
   * - one or more negative timeframes: monitor
   * - no usable data: reject
   */
  async applyDecision(alignment = {}) {
    const tfList = Object.keys(alignment || {});

    if (tfList.length === 0) {
      return {
        decision: 'reject',
        reason: 'no_mtf_data'
      };
    }

    const usableTfList = tfList.filter((tf) => {
      return alignment[tf] && alignment[tf].ok !== false;
    });

    if (usableTfList.length === 0) {
      return {
        decision: 'reject',
        reason: 'no_mtf_data'
      };
    }

    const allPositive = usableTfList.every((tf) => {
      return (
        alignment[tf] &&
        alignment[tf].positive
      );
    });

    if (allPositive) {
      return {
        decision: 'accept',
        reason: 'all_positive'
      };
    }

    const negatives = usableTfList.filter((tf) => {
      return !(
        alignment[tf] &&
        alignment[tf].positive
      );
    });

    if (
      negatives.length === 1 &&
      String(negatives[0]).toUpperCase() === 'D'
    ) {
      const daily = alignment[negatives[0]];

      if (daily && daily.rising) {
        return {
          decision: 'accept',
          reason: 'daily_rising'
        };
      }

      return {
        decision: 'monitor',
        reason: 'daily_not_rising'
      };
    }

    if (negatives.length >= 1) {
      return {
        decision: 'monitor',
        reason: 'some_negative'
      };
    }

    return {
      decision: 'reject',
      reason: 'unknown'
    };
  }
};
