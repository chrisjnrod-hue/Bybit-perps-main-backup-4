// notificationQueue.js
const logger = require('pino')();

const QUEUE_STATE = {
  IDLE: 'idle',
  STARTUP_SUMMARY: 'startup_summary',
  ROOT_CANDLE_SUMMARY: 'root_candle_summary',
  PROCESSING: 'processing'
};

const SIGNAL_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const ITEM_TIMEOUT_MS = 120 * 1000;
const PROCESSOR_WATCHDOG_MS = 180 * 1000;

function getNotificationType(signal) {
  if (!signal) {
    return 'signal';
  }

  const candidate =
    signal.notificationType ||
    signal.signalType;

  if (
    typeof candidate === 'string' &&
    candidate.trim()
  ) {
    return candidate.trim();
  }

  return 'signal';
}

function getSignalId(signal) {
  if (!signal) {
    return null;
  }

  if (
    signal.eventId !== undefined &&
    signal.eventId !== null &&
    String(signal.eventId).trim()
  ) {
    return String(signal.eventId).trim();
  }

  if (!signal.symbol || !signal.root_tf) {
    return null;
  }

  const type = getNotificationType(signal);

  const candleValue =
    signal.candle_open_time !== undefined &&
    signal.candle_open_time !== null
      ? Number(signal.candle_open_time)
      : null;

  if (Number.isFinite(candleValue)) {
    return [signal.symbol, signal.root_tf, candleValue, type].join('_');
  }

  const detectedValue =
    signal.detected_at !== undefined &&
    signal.detected_at !== null
      ? Number(signal.detected_at)
      : Date.now();

  return [signal.symbol, signal.root_tf, detectedValue, type].join('_');
}

function resolveQueueItemType(type, signal = null) {
  const rawType = String(type || '')
    .trim()
    .toLowerCase();

  const aliasMap = {
    'new_root_candle_open': 'root_candle_open_batch',
    'root_tf_candle_open': 'root_candle_open_batch',
    'newroottfcandleopen': 'root_candle_open_batch',
    'root_candle_open_batch': 'root_candle_open_batch',
    'new_root_candle': 'realtime',
    'root_candle_update': 'realtime',
    'rootcandle_update': 'realtime',
    'startup_batch': 'startup_batch',
    'root_candle_batch': 'root_candle_batch',
    'realtime': 'realtime',
    'mtf_alignment': 'mtf_alignment',
    'mtfalign': 'mtf_alignment',
    'midcandle_update': 'midcandle_update',
    'candle_update': 'candle_update',
    'startup': 'startup_batch'
  };

  if (aliasMap[rawType]) {
    return aliasMap[rawType];
  }

  if (!rawType) {
    const notificationType = String(getNotificationType(signal) || '')
      .trim()
      .toLowerCase();

    const signalMap = {
      'new_root_candle_open': 'root_candle_open_batch',
      'root_tf_candle_open': 'root_candle_open_batch',
      'newroottfcandleopen': 'root_candle_open_batch',
      'new_root_candle': 'realtime',
      'root_candle_update': 'realtime',
      'rootcandle_update': 'realtime',
      'mtf_alignment': 'mtf_alignment',
      'mtfalign': 'mtf_alignment',
      'midcandle_update': 'midcandle_update',
      'candle_update': 'candle_update'
    };

    return signalMap[notificationType] || 'realtime';
  }

  const map = {
    'new_root_candle': 'realtime',
    'root_candle_update': 'realtime',
    'rootcandle_update': 'realtime',
    'root_candle_open_batch': 'root_candle_open_batch',
    'new_root_candle_open': 'root_candle_open_batch',
    'root_tf_candle_open': 'root_candle_open_batch',
    'newroottfcandleopen': 'root_candle_open_batch',
    'root_candle_batch': 'root_candle_batch',
    'startup_batch': 'startup_batch',
    'realtime': 'realtime',
    'mtf_alignment': 'mtf_alignment',
    'mtfalign': 'mtf_alignment',
    'midcandle_update': 'midcandle_update',
    'candle_update': 'candle_update'
  };

  return map[rawType] || rawType;
}

class NotificationQueue {
  constructor() {
    this.queue = [];
    this.state = QUEUE_STATE.IDLE;
    this.processing = false;
    this.processingStartedAt = null;

    this.startupSummaryInProgress = false;
    this.rootCandleSummaryInProgress = false;

    this.pendingRootCandleOpenBatches = [];

    this.sentSignalIds = new Set();
    this.pendingSignalIds = new Set();
    this.sentSignalTimestamps = new Map();
  }

  _sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  _withTimeout(promiseFactory, timeoutMs = ITEM_TIMEOUT_MS, label = 'queue item') {
    let timeoutHandle = null;

    return Promise.race([
      promiseFactory(),
      new Promise((_, reject) => {
        timeoutHandle = setTimeout(() => {
          reject(new Error(`${label} timed out after ${timeoutMs}ms`));
        }, timeoutMs);
      })
    ]).finally(() => {
      if (timeoutHandle) {
        clearTimeout(timeoutHandle);
      }
    });
  }

  clearExpiredSignalCache(ttlMs = SIGNAL_CACHE_TTL_MS) {
    const now = Date.now();

    for (const [signalId, sentAt] of this.sentSignalTimestamps.entries()) {
      if (now - sentAt > ttlMs) {
        this.sentSignalIds.delete(signalId);
        this.sentSignalTimestamps.delete(signalId);
      }
    }
  }

  isKnownSignal(signal) {
    this.clearExpiredSignalCache();

    const signalId = getSignalId(signal);

    if (!signalId) {
      return true;
    }

    return (
      this.sentSignalIds.has(signalId) ||
      this.pendingSignalIds.has(signalId)
    );
  }

  reserveSignal(signal) {
    const signalId = getSignalId(signal);

    if (signalId) {
      this.pendingSignalIds.add(signalId);
    }

    return signalId;
  }

  markSignalSent(signalId) {
    if (!signalId) {
      return;
    }

    this.pendingSignalIds.delete(signalId);
    this.sentSignalIds.add(signalId);
    this.sentSignalTimestamps.set(signalId, Date.now());
  }

  releaseSignal(signalId) {
    if (!signalId) {
      return;
    }

    this.pendingSignalIds.delete(signalId);
  }

  normalizeSignal(signal, fallbackType = null) {
    if (!signal || !signal.symbol || !signal.root_tf) {
      return null;
    }

    const nextSignal = { ...signal };

    const resolvedType =
      signal.notificationType ||
      signal.signalType ||
      fallbackType ||
      getNotificationType(signal);

    nextSignal.notificationType = resolvedType;

    if (signal.signalType === undefined && resolvedType) {
      nextSignal.signalType = resolvedType;
    }

    if (signal.eventId !== undefined && signal.eventId !== null) {
      nextSignal.eventId = String(signal.eventId);
    }

    if (signal.candle_open_time !== undefined && signal.candle_open_time !== null) {
      const candleOpenTime = Number(signal.candle_open_time);

      if (Number.isFinite(candleOpenTime)) {
        nextSignal.candle_open_time = candleOpenTime;
      }
    }

    return nextSignal;
  }

    enqueueSignal(signal, type = 'realtime') {
    if (!signal || !signal.symbol || !signal.root_tf) {
      logger.warn({ signal }, 'NotificationQueue: invalid signal, skipping');
      return false;
    }

    const normalized = this.normalizeSignal(
      signal,
      type === 'realtime' ? null : type
    );

    if (!normalized) {
      logger.warn({ signal, type }, 'NotificationQueue: signal normalization failed');
      return false;
    }

    const signalId = getSignalId(normalized);

    if (!signalId) {
      logger.warn({
        symbol: normalized.symbol,
        root_tf: normalized.root_tf,
        type,
        notificationType: normalized.notificationType
      }, 'NotificationQueue: signal has no stable ID, skipping');
      return false;
    }

    if (this.isKnownSignal(normalized)) {
      logger.info({
        signalId,
        symbol: normalized.symbol,
        root_tf: normalized.root_tf,
        type,
        notificationType: normalized.notificationType
      }, 'NotificationQueue: duplicate or pending signal skipped');
      return false;
    }

    this.reserveSignal(normalized);

    const queueType = resolveQueueItemType(type, normalized);

    this.queue.push({
      type: queueType,
      signal: normalized,
      signalId,
      timestamp: Date.now()
    });

    logger.debug({
      signalId,
      type,
      queueType,
      notificationType: normalized.notificationType,
      queueLength: this.queue.length
    }, 'NotificationQueue: signal enqueued');

    this.startProcessing();
    return true;
        }

  enqueueStartupBatch(signals) {
    if (!Array.isArray(signals)) {
      logger.warn('NotificationQueue: invalid startup batch');
      return false;
    }

    if (this.startupSummaryInProgress) {
      logger.info('NotificationQueue: startup summary already in progress, skipping duplicate batch');
      return false;
    }

    return this.enqueueSummaryBatch({
      signals,
      batchType: 'startup_batch',
      fallbackType: 'startup',
      summaryState: QUEUE_STATE.STARTUP_SUMMARY,
      summaryFlag: 'startupSummaryInProgress',
      logLabel: 'startup'
    });
  }

    enqueueRootCandleOpenBatch(signals, tf = null, isStartupPhase = false) {
    if (!Array.isArray(signals)) {
      logger.warn('NotificationQueue: invalid root candle open batch');
      return false;
    }

    if (this.rootCandleSummaryInProgress) {
      logger.info(
        { tf, inputCount: signals.length },
        'NotificationQueue: root candle summary already in progress; queueing batch for retry'
      );

      this.pendingRootCandleOpenBatches.push({
        signals: [...signals],
        tf,
        isStartupPhase,
        timestamp: Date.now()
      });

      return true;
    }

    const uniqueSignals = [];
    const reservedIds = new Set();

    for (const signal of signals) {
      const normalized = this.normalizeSignal(signal, 'new_root_candle');

      if (!normalized) {
        continue;
      }

      const signalWithType = {
        ...normalized,
        notificationType: 'new_root_candle',
        signalType: normalized.signalType || 'new_root_candle'
      };

      const signalId = getSignalId(signalWithType);

      if (!signalId || reservedIds.has(signalId)) {
        logger.debug({
          signalId,
          tf,
          notificationType: signalWithType.notificationType
        }, 'NotificationQueue: filtering duplicate within root-candle batch');
        continue;
      }

      reservedIds.add(signalId);
      this.reserveSignal(signalWithType);
      uniqueSignals.push(signalWithType);
    }

    if (uniqueSignals.length === 0) {
      logger.warn(
        { tf, inputCount: signals.length },
        'NotificationQueue: root TF candle-open batch contained no new signals'
      );
      return false;
    }

    this.rootCandleSummaryInProgress = true;
    this.state = QUEUE_STATE.ROOT_CANDLE_SUMMARY;

    this.queue.push({
      type: 'root_candle_open_batch',
      signals: uniqueSignals,
      signalIds: uniqueSignals.map((signal) => getSignalId(signal)),
      tf,
      timestamp: Date.now()
    });

    logger.info({
      tf,
      total: signals.length,
      unique: uniqueSignals.length,
      queueLength: this.queue.length
    }, 'NotificationQueue: root TF candle-open batch enqueued successfully');

    this.startProcessing();
    return true;
    }

  enqueueRootCandleOpenBatch(signals, tf = null, isStartupPhase = false) {
    if (!Array.isArray(signals)) {
      logger.warn('NotificationQueue: invalid root candle open batch');
      return false;
    }

    if (this.rootCandleSummaryInProgress) {
      logger.info(
        { tf, inputCount: signals.length },
        'NotificationQueue: root candle summary already in progress; queueing batch for retry'
      );

      this.pendingRootCandleOpenBatches.push({
        signals: [...signals],
        tf,
        isStartupPhase,
        timestamp: Date.now()
      });

      return true;
    }

    const uniqueSignals = [];
    const reservedIds = new Set();

    for (const signal of signals) {
      const normalized = this.normalizeSignal(signal, 'new_root_candle');

      if (!normalized) {
        continue;
      }

      const signalWithType = {
        ...normalized,
        notificationType: 'new_root_candle',
        signalType: normalized.signalType || 'new_root_candle'
      };

      const signalId = getSignalId(signalWithType);

      // FIX: Only dedupe within this batch, not globally across batches
      if (!signalId || reservedIds.has(signalId)) {
        logger.debug({
          signalId,
          tf,
          notificationType: signalWithType.notificationType
        }, 'NotificationQueue: filtering duplicate within root-candle batch');
        continue;
      }

      reservedIds.add(signalId);
      this.reserveSignal(signalWithType);
      uniqueSignals.push(signalWithType);
    }

    if (uniqueSignals.length === 0) {
      logger.warn(
        { tf, inputCount: signals.length },
        'NotificationQueue: root TF candle-open batch contained no new signals'
      );
      return false;
    }

    this.rootCandleSummaryInProgress = true;
    this.state = QUEUE_STATE.ROOT_CANDLE_SUMMARY;

    this.queue.push({
      type: 'root_candle_open_batch',
      signals: uniqueSignals,
      signalIds: uniqueSignals.map((signal) => getSignalId(signal)),
      tf,
      timestamp: Date.now()
    });

    logger.info({
      tf,
      total: signals.length,
      unique: uniqueSignals.length,
      queueLength: this.queue.length
    }, 'NotificationQueue: root TF candle-open batch enqueued successfully');

    this.startProcessing();
    return true;
  }

  enqueueSummaryBatch({
    signals,
    batchType,
    fallbackType,
    summaryState,
    summaryFlag,
    logLabel
  }) {
    if (!Array.isArray(signals)) {
      logger.warn({ batchType, logLabel }, 'NotificationQueue: invalid summary signals');
      return false;
    }

    const uniqueSignals = [];
    const reservedIds = new Set();

    for (const signal of signals) {
      const normalized = this.normalizeSignal(signal, fallbackType);

      if (!normalized) {
        continue;
      }

      const signalId = getSignalId(normalized);

      if (!signalId || reservedIds.has(signalId) || this.isKnownSignal(normalized)) {
        logger.debug({
          signalId,
          notificationType: normalized.notificationType
        }, `NotificationQueue: filtering duplicate ${logLabel} signal`);
        continue;
      }

      reservedIds.add(signalId);
      this.reserveSignal(normalized);
      uniqueSignals.push(normalized);
    }

    if (uniqueSignals.length === 0) {
      logger.info({ batchType }, `NotificationQueue: ${logLabel} batch contained no new signals`);
      return false;
    }

    this[summaryFlag] = true;
    this.state = summaryState;

    this.queue.push({
      type: batchType,
      signals: uniqueSignals,
      signalIds: uniqueSignals.map((signal) => getSignalId(signal)),
      timestamp: Date.now()
    });

    logger.info({
      total: signals.length,
      unique: uniqueSignals.length,
      queueLength: this.queue.length
    }, `NotificationQueue: ${logLabel} batch enqueued`);

    this.startProcessing();
    return true;
  }

  startProcessing() {
    if (this.processing) {
      return;
    }

    this.processing = true;
    this.processingStartedAt = Date.now();
    this.state = QUEUE_STATE.PROCESSING;

    logger.debug({
      queueLength: this.queue.length,
      startedAt: new Date(this.processingStartedAt).toISOString()
    }, 'NotificationQueue: startProcessing');

    void this.processQueue();
  }

  async processQueue() {
    if (this.processing === false) {
      this.processing = true;
      this.processingStartedAt = Date.now();
      this.state = QUEUE_STATE.PROCESSING;
    }

    const watchdogStart = Date.now();

    try {
      while (this.queue.length > 0) {
        const elapsed = Date.now() - watchdogStart;
        if (elapsed > PROCESSOR_WATCHDOG_MS) {
          logger.warn({
            queueLength: this.queue.length,
            elapsedMs: elapsed
          }, 'NotificationQueue: processor watchdog triggered; forcing queue reset');

          this.processing = false;
          this.processingStartedAt = null;
          this.state = QUEUE_STATE.IDLE;

          if (this.queue.length > 0) {
            this.startProcessing();
          }
          return;
        }

        const item = this.queue.shift();

        const queueType = resolveQueueItemType(item.type, item.signal);

        logger.debug({
          originalType: item.type,
          resolvedType: queueType,
          queueRemaining: this.queue.length
        }, 'NotificationQueue: processing item');

        try {
          if (queueType === 'startup_batch') {
            await this._withTimeout(
              () => this._processStartupBatch(item.signals),
              ITEM_TIMEOUT_MS,
              'startup batch'
            );
            this.markBatchSent(item);
          } else if (queueType === 'root_candle_batch') {
            await this._withTimeout(
              () => this._processRootCandleBatch(item.signals),
              ITEM_TIMEOUT_MS,
              'root candle batch'
            );
            this.markBatchSent(item);
          } else if (queueType === 'root_candle_open_batch') {
            await this._withTimeout(
              () => this._processRootCandleOpenBatch(item.signals, item.tf),
              ITEM_TIMEOUT_MS,
              'root candle open batch'
            );
            this.markBatchSent(item);
          } else if (queueType === 'realtime') {
            await this._withTimeout(
              () => this._processRealtimeSignal(item.signal),
              ITEM_TIMEOUT_MS,
              'realtime signal'
            );
            this.markSignalSent(item.signalId);
          } else if (queueType === 'mtf_alignment') {
            await this._withTimeout(
              () => this._processMtfAlignment(item.signal),
              ITEM_TIMEOUT_MS,
              'mtf alignment signal'
            );
            this.markSignalSent(item.signalId);
          } else if (queueType === 'midcandle_update') {
            await this._withTimeout(
              () => this._processMidCandleUpdate(item.signal),
              ITEM_TIMEOUT_MS,
              'mid candle update'
            );
            this.markSignalSent(item.signalId);
          } else if (queueType === 'candle_update') {
            await this._withTimeout(
              () => this._processCandleUpdate(item.signal),
              ITEM_TIMEOUT_MS,
              'candle update'
            );
            this.markSignalSent(item.signalId);
          } else {
            throw new Error(`Unsupported notification queue item type: ${queueType}`);
          }
        } catch (err) {
          this.releaseItem(item);

          logger.error({
            err,
            itemType: item.type,
            resolvedType: queueType
          }, 'NotificationQueue: item processing failed');
        }
      }

      this.state = QUEUE_STATE.IDLE;
      logger.info('NotificationQueue: queue processing completed');
    } catch (err) {
      logger.error({ err }, 'NotificationQueue: fatal processing error');
    } finally {
      this.processing = false;
      this.processingStartedAt = null;
      this.startupSummaryInProgress = false;
      this.rootCandleSummaryInProgress = false;
      this.state = QUEUE_STATE.IDLE;

      if (Array.isArray(this.pendingRootCandleOpenBatches) && this.pendingRootCandleOpenBatches.length > 0) {
        const pending = [...this.pendingRootCandleOpenBatches];
        this.pendingRootCandleOpenBatches = [];

        logger.info(
          { count: pending.length },
          'NotificationQueue: flushing deferred root candle open batches'
        );

        for (const batch of pending) {
          try {
            this.enqueueRootCandleOpenBatch(batch.signals, batch.tf, batch.isStartupPhase);
          } catch (err) {
            logger.error({ err, batch }, 'NotificationQueue: failed to flush deferred root candle open batch');
          }
        }
      }

      if (this.queue.length > 0) {
        logger.info(
          { queueLength: this.queue.length },
          'NotificationQueue: queue still has work; restarting processor'
        );
        this.startProcessing();
      } else {
        logger.debug('NotificationQueue: queue empty; processor idle');
      }
    }
  }

  markBatchSent(item) {
    for (const signalId of item.signalIds || []) {
      this.markSignalSent(signalId);
    }
  }

  releaseItem(item) {
    for (const signalId of item.signalIds || []) {
      this.releaseSignal(signalId);
    }

    if (item.signalId) {
      this.releaseSignal(item.signalId);
    }
  }

  async _processStartupBatch(signals) {
    const telegram = require('./telegram');

    telegram.init();

    logger.info({ count: signals.length }, 'NotificationQueue: starting startup batch flow');

    await telegram.sendStartupSummary({ snapshot: signals });

    logger.info('NotificationQueue: startup batch flow completed');
  }

  async _processRootCandleBatch(signals) {
    const telegram = require('./telegram');

    telegram.init();

    logger.info({ count: signals.length }, 'NotificationQueue: starting root candle batch flow');

    await telegram.sendRootCandleSummary({ snapshot: signals });

    logger.info('NotificationQueue: root candle batch flow completed');
  }

  async _processRootCandleOpenBatch(signals, tf = null) {
    const telegram = require('./telegram');

    telegram.init();

    logger.info(
      { tf, count: signals.length },
      'NotificationQueue: starting root TF candle-open batch flow'
    );

    if (typeof telegram.sendRootCandleOpenSummary !== 'function') {
      logger.error(
        { tf, count: signals.length },
        'NotificationQueue: ⚠️  CRITICAL - missing telegram.sendRootCandleOpenSummary; aborting root TF candle-open dispatch'
      );
      throw new Error('telegram.sendRootCandleOpenSummary is not available');
    }

    try {
      await telegram.sendRootCandleOpenSummary({
        snapshot: signals,
        tf
      });
    } catch (err) {
      logger.error(
        { err, tf, count: signals.length },
        'NotificationQueue: ⚠️  CRITICAL - root TF candle-open batch flow failed'
      );
      throw err;
    }

    logger.info(
      { tf, count: signals.length },
      'NotificationQueue: root TF candle-open batch flow completed successfully'
    );
  }

  async _processRealtimeSignal(signal) {
    const telegram = require('./telegram');

    telegram.init();

    logger.debug({
      symbol: signal.symbol,
      root_tf: signal.root_tf,
      notificationType: signal.notificationType
    }, 'NotificationQueue: sending realtime signal block');

    await telegram.sendNewSignalSingleBlock(signal);

    logger.info({
      symbol: signal.symbol,
      root_tf: signal.root_tf,
      notificationType: signal.notificationType
    }, 'NotificationQueue: realtime signal block sent');
  }

  async _processMtfAlignment(signal) {
    const telegram = require('./telegram');

    telegram.init();

    logger.debug({
      symbol: signal.symbol,
      root_tf: signal.root_tf,
      eventId: signal.eventId,
      notificationType: signal.notificationType
    }, 'NotificationQueue: sending MTF alignment alert');

    await telegram.sendMtfAlignmentAlert(signal);

    logger.info({
      symbol: signal.symbol,
      root_tf: signal.root_tf,
      eventId: signal.eventId,
      notificationType: signal.notificationType
    }, 'NotificationQueue: MTF alignment alert sent');
  }

  async _processMidCandleUpdate(signal) {
    const telegram = require('./telegram');

    telegram.init();

    logger.debug({
      symbol: signal.symbol,
      root_tf: signal.root_tf,
      notificationType: signal.notificationType
    }, 'NotificationQueue: sending mid-candle update block');

    await telegram.sendMidCandleUpdateBlock(signal);

    logger.info({
      symbol: signal.symbol,
      root_tf: signal.root_tf,
      notificationType: signal.notificationType
    }, 'NotificationQueue: mid-candle update block sent');
  }

  async _processCandleUpdate(signal) {
    logger.info({
      symbol: signal.symbol,
      root_tf: signal.root_tf
    }, 'NotificationQueue: candle update not implemented');
  }

  getStatus() {
    return {
      state: this.state,
      processing: this.processing,
      queueLength: this.queue.length,
      startupInProgress: this.startupSummaryInProgress,
      rootCandleInProgress: this.rootCandleSummaryInProgress,
      pendingRootCandleOpenBatches: this.pendingRootCandleOpenBatches.length,
      sentSignalCount: this.sentSignalIds.size,
      pendingSignalCount: this.pendingSignalIds.size
    };
  }

  resetSentSignals() {
    this.sentSignalIds.clear();
    this.pendingSignalIds.clear();
    this.sentSignalTimestamps.clear();

    logger.info('NotificationQueue: signal caches cleared');
  }
}

module.exports = new NotificationQueue();
