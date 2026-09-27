const WebSocket = require('ws');
const EventEmitter = require('events');
const config = require('../config');
const logger = require('pino')();

function envBool(name, defaultValue = false) {
  if (typeof process.env[name] === 'undefined') {
    return defaultValue;
  }

  const value = String(process.env[name])
    .trim()
    .toLowerCase();

  return (
    value === '1' ||
    value === 'true' ||
    value === 'yes'
  );
}

function configBool(value, defaultValue = false) {
  if (value === undefined || value === null) {
    return defaultValue;
  }

  if (typeof value === 'boolean') {
    return value;
  }

  const normalized = String(value)
    .trim()
    .toLowerCase();

  if (['1', 'true', 'yes', 'on'].includes(normalized)) {
    return true;
  }

  if (['0', 'false', 'no', 'off'].includes(normalized)) {
    return false;
  }

  return defaultValue;
}

const MAINNET = envBool('MAINNET', true);

function getWsUrl() {
  const explicit =
    process.env.BYBIT_WS_PUBLIC ||
    (config && config.BYBIT_WS_PUBLIC);

  if (explicit) {
    return String(explicit);
  }

  return MAINNET
    ? 'wss://stream.bybit.com/v5/public/linear'
    : 'wss://stream-testnet.bybit.com/v5/public/linear';
}

function normalizeTimeframe(timeframe) {
  if (timeframe === null || timeframe === undefined) {
    return null;
  }

  const value = String(timeframe)
    .trim()
    .toUpperCase();

  if (value === '1H' || value === 'H') {
    return '60';
  }

  if (value === '1D') {
    return 'D';
  }

  return value;
}

function isUsdtSymbol(symbol) {
  const value = String(symbol || '').toUpperCase();

  if (!value) {
    return false;
  }

  if (/USDT[QHUZ0-9]/.test(value.slice(-6))) {
    return false;
  }

  return /USDT(\.P)?$/.test(value);
}

function validateSymbol(symbol) {
  if (!isUsdtSymbol(symbol)) {
    return false;
  }

  if (!config || !config.SYMBOL_FILTER) {
    return true;
  }

  try {
    return new RegExp(config.SYMBOL_FILTER).test(symbol);
  } catch (err) {
    logger.warn(
      {
        filter: config.SYMBOL_FILTER,
        err: err && err.message
          ? err.message
          : String(err)
      },
      'bybitWs: invalid SYMBOL_FILTER regex'
    );

    return true;
  }
}

function toNumber(value) {
  const number = Number(value);

  return Number.isFinite(number)
    ? number
    : null;
}

function normalizeOpenTime(value) {
  const number = Number(value);

  if (!Number.isFinite(number)) {
    return null;
  }

  return number < 100000000000
    ? number * 1000
    : number;
}

function normalizeKlinePayload(payload, timeframe, symbol) {
  let item = payload;

  if (
    item &&
    typeof item === 'object' &&
    !Array.isArray(item) &&
    item.data !== undefined
  ) {
    item = item.data;
  }

  if (Array.isArray(item)) {
    item = item[0];
  }

  if (!item) {
    return null;
  }

  let openTime;
  let open;
  let high;
  let low;
  let close;
  let volume;
  let confirm = false;

  if (Array.isArray(item)) {
    openTime = normalizeOpenTime(item[0]);
    open = toNumber(item[1]);
    high = toNumber(item[2]);
    low = toNumber(item[3]);
    close = toNumber(item[4]);
    volume = toNumber(item[5]);
    confirm = Boolean(item[8]);
  } else {
    openTime = normalizeOpenTime(
      item.start ??
      item.startTime ??
      item.t ??
      item.open_time
    );

    open = toNumber(item.open ?? item.o);
    high = toNumber(item.high ?? item.h);
    low = toNumber(item.low ?? item.l);
    close = toNumber(item.close ?? item.c);
    volume = toNumber(item.volume ?? item.v);
    confirm = Boolean(item.confirm);
  }

  if (
    openTime === null ||
    open === null ||
    high === null ||
    low === null ||
    close === null ||
    volume === null
  ) {
    return null;
  }

  return {
    symbol,
    timeframe: normalizeTimeframe(timeframe),
    open_time: openTime,
    open,
    high,
    low,
    close,
    volume,
    confirm
  };
}

class WSManager extends EventEmitter {
  constructor() {
    super();

    this.connections = [];
    this.symbolToConn = new Map();
    this.klineBuffer = new Map();

    this.maxSockets = Number(
      config.MAX_CONCURRENT_WS ||
      process.env.MAX_CONCURRENT_WS ||
      20
    );

    this.batchSize = Number(
      config.BATCH_WS_SIZE ||
      process.env.BATCH_WS_SIZE ||
      20
    );

    this.subscribeChunk = Number(
      config.WS_SUBSCRIBE_CHUNK ||
      process.env.WS_SUBSCRIBE_CHUNK ||
      50
    );

    this.pingIntervalMs = Number(
      config.WS_PING_INTERVAL_MS ||
      process.env.WS_PING_INTERVAL_MS ||
      20000
    );

    this.started = false;
    this.stopping = false;
    this.lastKlineAt = 0;
  }

  isEnabled() {
    return configBool(
      config.USE_WS ??
      process.env.USE_WS,
      true
    );
  }

  start() {
    if (this.started) {
      logger.debug('bybitWs: start called more than once');
      return true;
    }

    if (!this.isEnabled()) {
      logger.info(
        'bybitWs: disabled by USE_WS configuration'
      );

      return false;
    }

    this.started = true;
    this.stopping = false;

    logger.info(
      {
        wsUrl: getWsUrl(),
        maxSockets: this.maxSockets,
        batchSize: this.batchSize
      },
      'bybitWs: manager started'
    );

    return true;
  }

  isHealthy(maxAgeMs = 10 * 60 * 1000) {
    const openConnection = this.connections.some((connection) => {
      return (
        connection.ready &&
        connection.ws &&
        connection.ws.readyState === WebSocket.OPEN
      );
    });

    if (!openConnection) {
      return false;
    }

    if (this.lastKlineAt === 0) {
      return true;
    }

    return Date.now() - this.lastKlineAt <= maxAgeMs;
  }

  intervalToTopicPart(timeframe) {
    return normalizeTimeframe(timeframe);
  }

  createConnection() {
    if (this.connections.length >= this.maxSockets) {
      logger.warn(
        {
          maxSockets: this.maxSockets
        },
        'bybitWs: maximum connection count reached'
      );

      return null;
    }

    const ws = new WebSocket(getWsUrl());

    const connection = {
      ws,
      id: `${Date.now()}-${Math.random()
        .toString(16)
        .slice(2)}`,

      symbols: new Set(),

      // Topics waiting to be sent
      pendingTopics: new Set(),

      // Topics currently being sent or waiting for server response
      inFlightTopics: new Set(),

      // Topics confirmed as subscribed
      topics: new Set(),

      ready: false,
      manuallyClosed: false,
      reconnectTimer: null,
      pingTimer: null
    };

    ws.on('open', () => {
      if (connection.manuallyClosed) {
        return;
      }

      connection.ready = true;

      logger.info(
        {
          connId: connection.id,
          wsUrl: getWsUrl()
        },
        'bybitWs: connection opened'
      );

      connection.pingTimer = setInterval(() => {
        if (
          connection.ws &&
          connection.ws.readyState === WebSocket.OPEN
        ) {
          try {
            connection.ws.send(
              JSON.stringify({
                op: 'ping'
              })
            );
          } catch (err) {
            logger.debug(
              {
                connId: connection.id,
                err: err && err.message
                  ? err.message
                  : String(err)
              },
              'bybitWs: ping failed'
            );
          }
        }
      }, this.pingIntervalMs);

      this.flushPending(connection);
    });

    ws.on('message', (message) => {
      this.handleMessage(connection, message);
    });

    ws.on('error', (err) => {
      logger.error(
        {
          connId: connection.id,
          err: err && err.message
            ? err.message
            : String(err)
        },
        'bybitWs: socket error'
      );
    });

    ws.on('close', (code, reason) => {
      connection.ready = false;

      if (connection.pingTimer) {
        clearInterval(connection.pingTimer);
        connection.pingTimer = null;
      }

      if (connection.reconnectTimer) {
        clearTimeout(connection.reconnectTimer);
        connection.reconnectTimer = null;
      }

      const symbolsToRecover = Array.from(
        connection.symbols
      );

      logger.warn(
        {
          connId: connection.id,
          code,
          reason: reason ? reason.toString() : '',
          symbols: symbolsToRecover.length
        },
        'bybitWs: connection closed'
      );

      // Clear all topic state on close
      connection.pendingTopics.clear();
      connection.inFlightTopics.clear();
      connection.topics.clear();

      this.connections = this.connections.filter(
        (item) => item !== connection
      );

      for (const symbol of symbolsToRecover) {
        if (this.symbolToConn.get(symbol) === connection) {
          this.symbolToConn.delete(symbol);
        }
      }

      if (
        this.stopping ||
        connection.manuallyClosed ||
        symbolsToRecover.length === 0
      ) {
        return;
      }

      connection.reconnectTimer = setTimeout(() => {
        connection.reconnectTimer = null;

        if (this.stopping || !this.started) {
          return;
        }

        for (const symbol of symbolsToRecover) {
          if (!this.symbolToConn.has(symbol)) {
            this.subscribeSymbolMTF(symbol);
          }
        }
      }, 1000);
    });

    this.connections.push(connection);

    logger.info(
      {
        connId: connection.id,
        connectionCount: this.connections.length
      },
      'bybitWs: connection created'
    );

    return connection;
  }

  handleMessage(connection, message) {
    let data;

    try {
      data = JSON.parse(message.toString());
    } catch (err) {
      logger.debug(
        {
          connId: connection.id,
          err: err && err.message
            ? err.message
            : String(err)
        },
        'bybitWs: failed to parse message'
      );

      return;
    }

    logger.debug(
      {
        connId: connection.id,
        data
      },
      'bybitWs: incoming frame'
    );

    if (data && data.op === 'ping') {
      // Bybit responds to client ping with {"success": true, "ret_msg": "pong", "op": "ping"}.
      // Do not respond with {"op": "pong"} as Bybit will reject it with "error:invalid op".
      logger.debug(
        { connId: connection.id },
        'bybitWs: ping acknowledged by server'
      );
      return;
    }

    if (
      data &&
      (
        data.success !== undefined ||
        data.retCode !== undefined ||
        data.ret_msg !== undefined
      )
    ) {
      const failed =
        data.success === false ||
        (
          data.retCode !== undefined &&
          Number(data.retCode) !== 0
        );

      const retMsg =
        data.retMsg ||
        data.ret_msg ||
        '';

      if (failed) {
        logger.warn(
          {
            connId: connection.id,
            retCode: data.retCode,
            retMsg,
            success: data.success,
            op: data.op,
            topic: data.topic
          },
          'bybitWs: subscription/API error'
        );

        // If subscription failed, move topics back to pending
        if (
          data.op === 'subscribe' &&
          Array.isArray(data.args)
        ) {
          for (const topic of data.args) {
            connection.topics.delete(topic);
            connection.inFlightTopics.delete(topic);
            connection.pendingTopics.add(topic);
          }
        }

        return;
      }

      logger.debug(
        {
          connId: connection.id,
          op: data.op,
          type: data.type
        },
        'bybitWs: subscription response'
      );

      return;
    }

    if (
      !data ||
      !data.topic ||
      !String(data.topic).startsWith('kline.')
    ) {
      return;
    }

    const topicParts = String(data.topic).split('.');

    if (topicParts.length < 3) {
      return;
    }

    const timeframe = normalizeTimeframe(
      topicParts[1]
    );

    const symbol = topicParts
      .slice(2)
      .join('.');

    if (!validateSymbol(symbol)) {
      return;
    }

    const payloads = Array.isArray(data.data)
      ? data.data
      : [data.data];

    for (const payload of payloads) {
      const kline = normalizeKlinePayload(
        payload,
        timeframe,
        symbol
      );

      if (!kline) {
        continue;
      }

      this.lastKlineAt = Date.now();

      if (!this.klineBuffer.has(symbol)) {
        this.klineBuffer.set(symbol, new Map());
      }

      this.klineBuffer
        .get(symbol)
        .set(timeframe, kline);

      this.emit('kline', {
        ...kline,
        raw: data
      });
    }
  }

  sendTopics(connection, topics, operation = 'subscribe') {
    if (
      !connection ||
      !connection.ws ||
      !Array.isArray(topics) ||
      topics.length === 0
    ) {
      return;
    }

    if (
      connection.ws.readyState !== WebSocket.OPEN ||
      !connection.ready
    ) {
      for (const topic of topics) {
        if (
          typeof topic === 'string' &&
          topic.trim() &&
          !connection.topics.has(topic) &&
          !connection.inFlightTopics.has(topic)
        ) {
          connection.pendingTopics.add(topic);
        }
      }

      return;
    }

    if (
      operation !== 'subscribe' &&
      operation !== 'unsubscribe'
    ) {
      logger.warn(
        {
          connId: connection.id,
          operation
        },
        'bybitWs: unsupported operation'
      );

      return;
    }

    const uniqueTopics = Array.from(
      new Set(
        topics.filter((topic) => {
          return (
            typeof topic === 'string' &&
            topic.trim()
          );
        })
      )
    );

    const sendableTopics = uniqueTopics.filter((topic) => {
      if (operation === 'subscribe') {
        return (
          !connection.topics.has(topic) &&
          !connection.inFlightTopics.has(topic)
        );
      }

      return (
        connection.topics.has(topic) &&
        !connection.inFlightTopics.has(topic)
      );
    });

    if (sendableTopics.length === 0) {
      return;
    }

    // Mark topics as in-flight before sending
    for (const topic of sendableTopics) {
      connection.inFlightTopics.add(topic);
      connection.pendingTopics.delete(topic);
    }

    const payload = {
      op: operation,
      args: sendableTopics
    };

    logger.info(
      {
        connId: connection.id,
        operation,
        args: sendableTopics.slice(0, 5),
        total: sendableTopics.length
      },
      'bybitWs: sending raw ws payload'
    );

    try {
      connection.ws.send(
        JSON.stringify(payload),
        (err) => {
          for (const topic of sendableTopics) {
            connection.inFlightTopics.delete(topic);
          }

          if (err) {
            for (const topic of sendableTopics) {
              connection.pendingTopics.add(topic);
            }

            logger.warn(
              {
                connId: connection.id,
                operation,
                err: err && err.message
                  ? err.message
                  : String(err)
              },
              'bybitWs: topic batch send failed'
            );

            return;
          }

          // ws.send callback confirms frame was written, not server ack
          if (operation === 'subscribe') {
            for (const topic of sendableTopics) {
              connection.topics.add(topic);
            }
          } else {
            for (const topic of sendableTopics) {
              connection.topics.delete(topic);
            }
          }

          logger.debug(
            {
              connId: connection.id,
              operation,
              count: sendableTopics.length
            },
            'bybitWs: topic batch sent'
          );
        }
      );
    } catch (err) {
      for (const topic of sendableTopics) {
        connection.inFlightTopics.delete(topic);
        connection.pendingTopics.add(topic);
      }

      logger.warn(
        {
          connId: connection.id,
          operation,
          err: err && err.message
            ? err.message
            : String(err)
        },
        'bybitWs: topic batch send threw'
      );
    }
  }

  flushPending(connection) {
    if (
      !connection ||
      !connection.ready ||
      connection.ws.readyState !== WebSocket.OPEN ||
      connection.pendingTopics.size === 0
    ) {
      return;
    }

    const topics = Array.from(
      connection.pendingTopics
    );

    for (
      let index = 0;
      index < topics.length;
      index += this.subscribeChunk
    ) {
      this.sendTopics(
        connection,
        topics.slice(
          index,
          index + this.subscribeChunk
        ),
        'subscribe'
      );
    }
  }

  getTargetConnection() {
    let target = this.connections.find((connection) => {
      return (
        !connection.manuallyClosed &&
        connection.symbols.size < this.batchSize
      );
    });

    if (!target) {
      target = this.createConnection();
    }

    return target;
  }

  subscribeSymbolMTF(symbol, timeframes = null) {
    if (!this.started || this.stopping) {
      logger.debug(
        { symbol },
        'bybitWs: subscription skipped because manager is not started'
      );

      return null;
    }

    if (!validateSymbol(symbol)) {
      logger.debug(
        { symbol },
        'bybitWs: invalid symbol; subscription skipped'
      );

      return null;
    }

    const existing = this.symbolToConn.get(symbol);

    if (
      existing &&
      existing.ws &&
      existing.ws.readyState !== WebSocket.CLOSED &&
      !existing.manuallyClosed
    ) {
      return existing;
    }

    if (existing) {
      this.symbolToConn.delete(symbol);
    }

    const connection = this.getTargetConnection();

    if (!connection) {
      return null;
    }

    const configuredTimeframes =
      timeframes ||
      config.MTF_TFS ||
      ['5', '15', '60', '240', 'D'];

    const normalizedTimeframes = Array.from(
      new Set(
        configuredTimeframes
          .map(normalizeTimeframe)
          .filter(Boolean)
      )
    );

    const topics = normalizedTimeframes.map((tf) => {
      return `kline.${tf}.${symbol}`;
    });

    connection.symbols.add(symbol);
    this.symbolToConn.set(symbol, connection);

    for (const topic of topics) {
      if (
        connection.topics.has(topic) ||
        connection.pendingTopics.has(topic) ||
        connection.inFlightTopics.has(topic)
      ) {
        continue;
      }

      connection.pendingTopics.add(topic);
    }

    logger.info(
      {
        symbol,
        connId: connection.id,
        topicCount: topics.length,
        pendingCount: connection.pendingTopics.size
      },
      'bybitWs: symbol subscription queued'
    );

    if (connection.ready) {
      this.flushPending(connection);
    }

    return connection;
  }

  subscribeSymbols(symbols, timeframes = null) {
    if (!Array.isArray(symbols)) {
      return 0;
    }

    let count = 0;

    for (const item of symbols) {
      const symbol =
        typeof item === 'string'
          ? item
          : item && item.symbol;

      if (this.subscribeSymbolMTF(symbol, timeframes)) {
        count++;
      }
    }

    return count;
  }

  unsubscribeSymbol(symbol) {
    const connection = this.symbolToConn.get(symbol);

    if (!connection) {
      return false;
    }

    const topics = Array.from(
      new Set([
        ...Array.from(connection.topics),
        ...Array.from(connection.pendingTopics)
      ])
    ).filter((topic) => {
      return topic.endsWith(`.${symbol}`);
    });

    const subscribedTopics = topics.filter((topic) => {
      return connection.topics.has(topic);
    });

    for (
      let index = 0;
      index < subscribedTopics.length;
      index += this.subscribeChunk
    ) {
      this.sendTopics(
        connection,
        subscribedTopics.slice(
          index,
          index + this.subscribeChunk
        ),
        'unsubscribe'
      );
    }

    connection.symbols.delete(symbol);

    for (const topic of topics) {
      connection.pendingTopics.delete(topic);
      connection.inFlightTopics.delete(topic);
      connection.topics.delete(topic);
    }

    this.symbolToConn.delete(symbol);
    this.klineBuffer.delete(symbol);

    if (connection.symbols.size === 0) {
      connection.manuallyClosed = true;

      if (connection.pingTimer) {
        clearInterval(connection.pingTimer);
        connection.pingTimer = null;
      }

      try {
        connection.ws.close();
      } catch (err) {
        logger.debug(
          { err },
          'bybitWs: empty connection close failed'
        );
      }
    }

    return true;
  }

  async performInitialScan() {
    return Array.from(this.symbolToConn.keys())
      .filter(validateSymbol)
      .map((symbol) => ({
        symbol,
        base: symbol.replace(
          /USDT(\.P)?$/i,
          ''
        ),
        quote: 'USDT'
      }));
  }

  async closeAll() {
    this.stopping = true;
    this.started = false;

    for (const connection of this.connections.slice()) {
      connection.manuallyClosed = true;

      if (connection.reconnectTimer) {
        clearTimeout(connection.reconnectTimer);
        connection.reconnectTimer = null;
      }

      if (connection.pingTimer) {
        clearInterval(connection.pingTimer);
        connection.pingTimer = null;
      }

      connection.pendingTopics.clear();
      connection.inFlightTopics.clear();
      connection.topics.clear();

      try {
        connection.ws.close();
      } catch (err) {
        logger.debug(
          {
            connId: connection.id,
            err: err && err.message
              ? err.message
              : String(err)
          },
          'bybitWs: close failed'
        );
      }
    }

    this.connections = [];
    this.symbolToConn.clear();
    this.klineBuffer.clear();
    this.lastKlineAt = 0;
  }
}

module.exports = new WSManager();
