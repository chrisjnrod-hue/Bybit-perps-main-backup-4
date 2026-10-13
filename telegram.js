// telegram.js
const TelegramBot = require('node-telegram-bot-api');
const config = require('../config');
const logger = require('pino')();
const dbModule = require('../db');

let bot = null;

const SUMMARY_TITLE_MAP = {
  startup: '📊 Startup Summary',
  new_root_candle: '🕔 New Root Candle Open',
  new_root_candle_open: '🕔 New Root Candle Open',
  root_tf_candle_open: '🕔 Root TF Candle Open',
  newroottfcandleopen: '🕔 New Root Candle Open',
  mtf_alignment: '⏱️ MTF Alignment Alert',
  midcandle_update: '⏳ Mid-Candle Update'
};

function getNotificationType(signal) {
  if (!signal) {
    return null;
  }

  const type =
    signal.notificationType ||
    signal.signalType;

  if (
    typeof type === 'string' &&
    type.trim()
  ) {
    return type.trim();
  }

  return null;
}

function normalizeNotificationType(signal) {
  const type = getNotificationType(signal);

  if (!type) {
    return null;
  }

  const normalized = String(type).trim().toLowerCase();

  const aliases = {
    'new_root_candle_open': 'new_root_candle',
    'root_tf_candle_open': 'new_root_candle',
    'newroottfcandleopen': 'new_root_candle'
  };

  return aliases[normalized] || normalized;
}

module.exports = {
  init() {
    if (!config.TELEGRAM_BOT_TOKEN) {
      logger.warn(
        'Telegram token not configured; telegram disabled'
      );
      return;
    }

    if (!config.TELEGRAM_CHAT_ID) {
      logger.warn(
        'Telegram chat ID not configured; telegram disabled'
      );
      return;
    }

    if (!bot) {
      bot = new TelegramBot(
        config.TELEGRAM_BOT_TOKEN,
        {
          polling: false
        }
      );

      logger.info(
        'Telegram bot initialized'
      );
    }
  },

  ensureInitialized() {
    if (!bot) {
      this.init();
    }

    if (!bot) {
      throw new Error(
        'Telegram bot is not initialized or Telegram is disabled'
      );
    }

    return bot;
  },

  getLabel(
    index,
    { lowercase = true } = {}
  ) {
    if (
      typeof index !== 'number' ||
      index < 0
    ) {
      return '';
    }

    let i = index + 1;
    const chars = [];

    while (i > 0) {
      i -= 1;

      chars.unshift(
        String.fromCharCode(
          (i % 26) + 65
        )
      );

      i = Math.floor(i / 26);
    }

    const label = chars.join('');

    return lowercase
      ? label.toLowerCase()
      : label;
  },

  _sleep(ms) {
    return new Promise((resolve) => {
      setTimeout(resolve, ms || 0);
    });
  },

  async _sendMessage(
    text,
    options = {}
  ) {
    this.ensureInitialized();

    const delayMs = Math.max(
      500,
      Number(
        config.TELEGRAM_SEND_DELAY_MS
      ) || 1000
    );

    try {
      return await bot.sendMessage(
        config.TELEGRAM_CHAT_ID,
        text,
        options
      );
    } catch (err) {
      const retryAfter =
        err?.response?.body?.parameters?.retry_after ??
        err?.response?.parameters?.retry_after ??
        0;

      const errorMessage =
        String(
          err?.message || ''
        ).toLowerCase();

      const isRateLimit =
        err?.response?.statusCode === 429 ||
        retryAfter > 0 ||
        errorMessage.includes('429') ||
        errorMessage.includes(
          'too many requests'
        );

      if (!isRateLimit) {
        throw err;
      }

      const retryMs = Math.max(
        delayMs,
        Number(retryAfter) > 0
          ? Number(retryAfter) * 1000
          : delayMs
      );

      logger.warn(
        {
          retryMs,
          retryAfter,
          symbol: options?.symbol || null,
          root_tf: options?.root_tf || null,
          notificationType:
            options?.notificationType || null
        },
        'Telegram: rate limited; backing off before retry'
      );

      await this._sleep(retryMs);

      return await bot.sendMessage(
        config.TELEGRAM_CHAT_ID,
        text,
        options
      );
    }
  },

  buildAlignmentLines(alignment) {
    const lines = [];
    let positiveCount = 0;
    let total = 0;

    for (
      const tf of Object.keys(alignment || {})
    ) {
      const info = alignment[tf];

      total += 1;

      const ok =
        info &&
        typeof info.histogram !== 'undefined';

      const positiveSymbol = ok
        ? (
            info.positive
              ? '🟢'
              : '🔴'
          )
        : '⚪';

      if (
        info &&
        info.positive
      ) {
        positiveCount += 1;
      }

      const histogram =
        ok
          ? `hist=${Number(
              info.histogram
            ).toFixed(6)}`
          : '';

      const rising =
        ok
          ? (
              info.rising
                ? '↑'
                : '↓'
            )
          : '';

      lines.push(
        `${tf}: ${positiveSymbol} ${
          ok
            ? (
                info.positive
                  ? 'POS'
                  : 'NEG'
              )
            : 'unknown'
        } ${histogram} ${rising}`.trim()
      );
    }

    const mtfScore =
      total > 0
        ? positiveCount / total
        : 0;

    return {
      lines: lines.join('\n'),
      mtfScore,
      positiveCount,
      total
    };
  },

  formatMarketData(md = {}) {
    const price =
      typeof md.price === 'number'
        ? md.price
        : md.price
          ? Number(md.price)
          : null;

    const volume24 =
      typeof md.volume_24h_usdt === 'number'
        ? md.volume_24h_usdt
        : md.volume_24h_usdt
          ? Number(md.volume_24h_usdt)
          : null;

    const volumeChange =
      typeof md.volume_change_pct === 'number'
        ? md.volume_change_pct
        : md.volume_change_pct
          ? Number(md.volume_change_pct)
          : null;

    const marketCap =
      typeof md.market_cap === 'number'
        ? md.market_cap
        : md.market_cap
          ? Number(md.market_cap)
          : null;

    const lines = [
      `💰 Price: ${
        price !== null && price > 0
          ? '$' + price.toLocaleString(
              'en-US',
              {
                maximumFractionDigits: 8
              }
            )
          : '0'
      }`,
      `💵 24h Volume: ${
        volume24 !== null && volume24 > 0
          ? '$' + volume24.toLocaleString(
              'en-US',
              {
                maximumFractionDigits: 2
              }
            ) + ' USDT'
          : '0 USDT'
      }`,
      `📈 Volume Change: ${
        volumeChange !== null
          ? volumeChange.toFixed(2) + '%'
          : 'n/a'
      }`,
      `💎 Market Cap: ${
        marketCap && marketCap > 0
          ? '$' + marketCap.toLocaleString(
              'en-US',
              {
                maximumFractionDigits: 0
              }
            )
          : 'n/a'
      }`
    ];

    return lines.join('\n');
  },

  buildSignalMessage(signal) {
    const {
      symbol,
      root_tf,
      detected_at,
      meta = {}
    } = signal || {};

    const timeStr =
      detected_at
        ? new Date(
            detected_at
          ).toISOString()
        : new Date().toISOString();

    const alignment =
      meta.alignment || {};

    const tvScore =
      typeof meta.tvScore === 'number'
        ? meta.tvScore
        : (
            meta.tvScore
              ? Number(meta.tvScore)
              : 0
          );

    const tvSource =
      meta.tvSource || 'error';

    const mtfScore =
      typeof meta.mtfScore === 'number'
        ? meta.mtfScore
        : null;

    const decision =
      meta.decision || 'monitor';

    const reason =
      meta.acceptReason ||
      meta.reason ||
      'n/a';

    const {
      lines: alignmentLines,
      mtfScore: computedMtfScore
    } = this.buildAlignmentLines(
      alignment
    );

    const usedMtfScore =
      mtfScore !== null
        ? mtfScore
        : computedMtfScore;

    const tvPercent =
      Math.round(
        (tvScore || 0) * 100
      );

    const mtfPercent =
      Math.round(
        (usedMtfScore || 0) * 100
      );

    const scoringLine =
      `📊 Scoring:\nTV: ${tvPercent}% (${tvSource}) • MTF: ${mtfPercent}%`;

    const mtfHeader =
      '🛰️ MTF Status:';

    const marketBlock =
      `💱 Market Data:\n${
        this.formatMarketData(
          meta.marketData || {}
        )
      }`;

    const notificationType =
      normalizeNotificationType(signal);

    const eventTitle =
      notificationType
        ? (
            SUMMARY_TITLE_MAP[
              notificationType
            ] || null
          )
        : null;

    const msgParts = [
      ...(eventTitle
        ? [
            eventTitle,
            ''
          ]
        : []),
      `🎯 Signal: ${symbol} (${root_tf})`,
      `⏰ Time: ${timeStr}`,
      `${
        decision === 'accept'
          ? '✅ Decision'
          : '⚠️ Decision'
      }: ${decision} (reason: ${reason})`,
      '',
      scoringLine,
      '',
      mtfHeader,
      alignmentLines || 'No MTF data',
      '',
      marketBlock
    ];

    return msgParts.join('\n');
  },

  async sendNewSignalSingleBlock(
    signal,
    forcedType = null
  ) {
    const normalizedSignal = signal
      ? {
          ...signal,
          notificationType:
            forcedType ||
            signal.notificationType ||
            signal.signalType ||
            null
        }
      : null;

    if (!normalizedSignal) {
      throw new Error(
        'Telegram: cannot send an empty signal'
      );
    }

    const message =
      this.buildSignalMessage(
        normalizedSignal
      );

    await this._sendMessage(
      message,
      {
        symbol:
          normalizedSignal.symbol,
        root_tf:
          normalizedSignal.root_tf,
        notificationType:
          normalizedSignal.notificationType ||
          null,
        eventId:
          normalizedSignal.eventId ||
          null
      }
    );

    logger.debug(
      {
        symbol:
          normalizedSignal.symbol,
        root_tf:
          normalizedSignal.root_tf,
        eventId:
          normalizedSignal.eventId ||
          null,
        notificationType:
          normalizedSignal.notificationType ||
          null
      },
      'Telegram: signal detail block sent'
    );

    return true;
  },

  async sendMidCandleUpdateBlock(signal) {
    if (!signal) {
      throw new Error(
        'Telegram: cannot send an empty mid-candle signal'
      );
    }

    const normalizedSignal = {
      ...signal,
      notificationType:
        'midcandle_update',
      signalType:
        signal.signalType ||
        'midcandle_update'
    };

    const message =
      this.buildSignalMessage(
        normalizedSignal
      );

    await this._sendMessage(
      message,
      {
        symbol:
          normalizedSignal.symbol,
        root_tf:
          normalizedSignal.root_tf,
        notificationType:
          'midcandle_update',
        eventId:
          normalizedSignal.eventId ||
          null
      }
    );

    logger.info(
      {
        symbol:
          normalizedSignal.symbol,
        root_tf:
          normalizedSignal.root_tf,
        eventId:
          normalizedSignal.eventId ||
          null
      },
      'Telegram: mid-candle update sent'
    );

    return true;
  },

  async sendMtfAlignmentAlert(signal) {
    if (!signal) {
      throw new Error(
        'Telegram: cannot send an empty MTF alignment alert'
      );
    }

    this.ensureInitialized();

    const normalizedSignal = {
      ...signal,
      notificationType:
        'mtf_alignment',
      signalType:
        signal.signalType ||
        'mtf_alignment'
    };

    const message =
      this.buildSignalMessage(
        normalizedSignal
      );

    await this._sendMessage(
      message,
      {
        symbol:
          normalizedSignal.symbol,
        root_tf:
          normalizedSignal.root_tf,
        notificationType:
          'mtf_alignment',
        eventId:
          normalizedSignal.eventId ||
          null
      }
    );

    logger.info(
      {
        symbol:
          normalizedSignal.symbol,
        root_tf:
          normalizedSignal.root_tf,
        eventId:
          normalizedSignal.eventId ||
          null
      },
      'Telegram: MTF alignment alert sent'
    );

    return true;
  },

  async sendSummaryBlock({
    snapshot = [],
    title = '📊 Startup Summary',
    signalType = null,
    timeframeFilter = null
  } = {}) {
    this.ensureInitialized();

    try {
      let signals =
        Array.isArray(snapshot)
          ? snapshot
          : [];

      if (timeframeFilter) {
        const targetTf =
          String(timeframeFilter);

        signals =
          signals.filter((signal) => {
            return String(
              signal.root_tf || ''
            ) === targetTf;
          });
      }

      if (signals.length === 0) {
        logger.warn(
          {
            title,
            timeframeFilter
          },
          'Telegram: no signals provided to summary block'
        );
        return false;
      }

      const delayMs = Math.max(
        500,
        Number(
          config.TELEGRAM_SEND_DELAY_MS
        ) || 1000
      );

      const tfCounts = {};
      const symbolSet = new Set();

      for (const signal of signals) {
        const tf =
          String(
            signal.root_tf || 'unknown'
          );

        tfCounts[tf] =
          (tfCounts[tf] || 0) + 1;

        if (signal.symbol) {
          symbolSet.add(
            signal.symbol
          );
        }
      }

      const configuredRootTfs =
        Array.isArray(config.ROOT_TFS) &&
        config.ROOT_TFS.length > 0
          ? config.ROOT_TFS.map(String)
          : [];

      let orderedRootTfs =
        configuredRootTfs.length > 0
          ? [...configuredRootTfs]
          : Object.keys(tfCounts);

      if (timeframeFilter) {
        const targetTf =
          String(timeframeFilter);

        orderedRootTfs = [
          targetTf
        ];
      } else {
        for (
          const tf of Object.keys(tfCounts)
        ) {
          if (
            !orderedRootTfs.includes(tf)
          ) {
            orderedRootTfs.push(tf);
          }
        }
      }

      const summaryParts =
        orderedRootTfs.map((tf) => {
          return `${tf}: ${tfCounts[tf] || 0}`;
        });

      const allSymbols =
        Array.from(symbolSet).sort(
          (a, b) => {
            return a.localeCompare(
              b,
              undefined,
              {
                sensitivity: 'base'
              }
            );
          }
        );

      const symbolLines =
        allSymbols.length > 0
          ? allSymbols.join('\n')
          : 'n/a';

      const header =
        `${title} (${signals.length} signals):\n` +
        `${summaryParts.join(' • ')}\n\n` +
        symbolLines;

      await this._sendMessage(
        header
      );

      logger.info(
        {
          title,
          count: signals.length,
          timeframeFilter
        },
        'Telegram: summary header sent'
      );

      await this._sleep(delayMs);

      const sortedSignals =
        [...signals].sort((a, b) => {
          const symbolOrder =
            String(a.symbol || '').localeCompare(
              String(b.symbol || ''),
              undefined,
              {
                sensitivity: 'base'
              }
            );

          if (symbolOrder !== 0) {
            return symbolOrder;
          }

          return String(
            a.root_tf || ''
          ).localeCompare(
            String(b.root_tf || ''),
            undefined,
            {
              numeric: true
            }
          );
        });

      for (
        let i = 0;
        i < sortedSignals.length;
        i += 1
      ) {
        const signal =
          sortedSignals[i];

        await this.sendNewSignalSingleBlock(
          signal,
          signalType
        );

        logger.debug(
          {
            symbol: signal.symbol,
            root_tf: signal.root_tf,
            eventId: signal.eventId || null,
            index: i + 1,
            total: sortedSignals.length
          },
          'Telegram: summary signal block sent'
        );

        await this._sleep(delayMs);
      }

      let openCount = 0;

      try {
        const row =
          dbModule
            .get()
            .prepare(
              `
                SELECT COUNT(*) AS cnt
                FROM trades
                WHERE status = 'open'
              `
            )
            .get();

        openCount =
          row
            ? Number(row.cnt || 0)
            : 0;
      } catch (err) {
        logger.debug(
          { err },
          'Telegram: failed to read open trades count'
        );

        openCount = 0;
      }

      const maxOpenTrades =
        Number(
          config.MAX_OPEN_TRADES
        ) || 0;

      const maxSlots =
        Math.max(
          0,
          maxOpenTrades - openCount
        );

      const recommendedHeader =
        `📈 Recommended to Open (${maxSlots} slots available):`;

      await this._sendMessage(
        recommendedHeader
      );

      await this._sleep(delayMs);

      const candidates =
        signals
          .map((signal) => {
            return {
              symbol:
                signal.symbol,
              root_tf:
                signal.root_tf,
              tvScore:
                Number(
                  signal.meta?.tvScore || 0
                ),
              mtfScore:
                Number(
                  signal.meta?.mtfScore || 0
                ),
              acceptDecision:
                signal.meta?.decision ||
                'monitor',
              reason:
                signal.meta?.acceptReason ||
                'n/a'
            };
          })
          .filter((candidate) => {
            return (
              candidate.acceptDecision ===
              'accept'
            );
          })
          .sort((a, b) => {
            if (
              b.tvScore !== a.tvScore
            ) {
              return b.tvScore - a.tvScore;
            }

            return b.mtfScore - a.mtfScore;
          });

      const recommended =
        candidates.slice(
          0,
          Math.max(0, maxSlots)
        );

      if (recommended.length === 0) {
        await this._sendMessage(
          'No recommended signals (all rejections or filtered)'
        );
      } else {
        for (
          let i = 0;
          i < recommended.length;
          i += 1
        ) {
          const recommendation =
            recommended[i];

          const label =
            this.getLabel(i, {
              lowercase: true
            });

          const tvPercent =
            Math.round(
              (recommendation.tvScore || 0) *
              100
            );

          const mtfPercent =
            Math.round(
              (recommendation.mtfScore || 0) *
              100
            );

          const simulatedNote =
            config.OPENTRADE
              ? ''
              : ' [SIMULATED]';

          const line =
            `${label}) ` +
            `${recommendation.symbol} ` +
            `${recommendation.root_tf} - ` +
            `TV:${tvPercent}% ` +
            `MTF:${mtfPercent}% - ` +
            `${recommendation.reason}` +
            simulatedNote;

          await this._sendMessage(
            line
          );

          logger.debug(
            {
              symbol:
                recommendation.symbol,
              root_tf:
                recommendation.root_tf,
              index: i + 1,
              total: recommended.length
            },
            'Telegram: recommended block sent'
          );

          await this._sleep(delayMs);
        }
      }

      logger.info(
        {
          title,
          count: signals.length,
          timeframeFilter
        },
        'Telegram: summary flow completed'
      );

      return true;
    } catch (err) {
      logger.error(
        {
          err,
          title,
          timeframeFilter
        },
        'Telegram: summary flow failed'
      );
      throw err;
    }
  },

  async sendStartupSummary({
    snapshot = []
  } = {}) {
    await this.sendSummaryBlock({
      snapshot,
      title:
        SUMMARY_TITLE_MAP.startup,
      signalType: null
    });

    return true;
  },

  async sendRootCandleSummary({
    snapshot = [],
    tf = null
  } = {}) {
    const filtered =
      Array.isArray(snapshot)
        ? (
            tf
              ? snapshot.filter((signal) => {
                  return String(
                    signal.root_tf || ''
                  ) === String(tf);
                })
              : snapshot
          )
        : [];

    const title =
      tf
        ? `🕔 New Root Candle Open (${tf})`
        : SUMMARY_TITLE_MAP.new_root_candle;

    await this.sendSummaryBlock({
      snapshot: filtered,
      title,
      signalType:
        'new_root_candle',
      timeframeFilter:
        tf || null
    });

    return true;
  },

  async sendRootCandleOpenSummary({
    snapshot = [],
    tf = null
  } = {}) {
    if (!Array.isArray(snapshot)) {
      logger.warn(
        { tf },
        'Telegram: sendRootCandleOpenSummary received invalid snapshot'
      );
      return false;
    }

    const filtered =
      tf
        ? snapshot.filter((signal) => {
            return String(signal.root_tf || '') === String(tf);
          })
        : snapshot;

    const title =
      tf
        ? `🕔 New Root Candle Open (${tf})`
        : SUMMARY_TITLE_MAP.new_root_candle;

    await this.sendSummaryBlock({
      snapshot: filtered,
      title,
      signalType: 'new_root_candle',
      timeframeFilter: tf || null
    });

    return true;
  }
};
