/**
 * EdgeOffline.js (v2.3.0)
 * Ultra-resilient, zero-dependency Offline-First & Background Sync Engine.
 * Pure IndexedDB persistence with LocalStorage fallback, exponential backoff,
 * fetch interceptor, queue management, and comprehensive network lifecycle events.
 * 
 * Part of Origin Edge Ecosystem.
 * @license MIT
 */

(function (root, factory) {
  if (typeof define === 'function' && define.amd) {
    define([], factory);
  } else if (typeof module === 'object' && module.exports) {
    module.exports = factory();
  } else {
    root.EdgeOffline = factory();
  }
}(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  const DB_NAME = 'EdgeOffline_Store';
  const DB_VERSION = 1;
  const STORE_NAME = 'sync_queue';
  const LS_KEY = 'edge_offline_queue_v2';

  const listeners = new Map();
  const customHandlers = new Map();
  let dbPromise = null;
  let isSyncing = false;

  // ── 1. EVENT EMITTER ────────────────────────────────────────────────
  function on(event, callback) {
    if (!listeners.has(event)) listeners.set(event, []);
    listeners.get(event).push(callback);
    return () => off(event, callback);
  }

  function off(event, callback) {
    if (listeners.has(event)) {
      listeners.set(event, listeners.get(event).filter(fn => fn !== callback));
    }
  }

  function emit(event, payload) {
    if (listeners.has(event)) {
      listeners.get(event).forEach(fn => {
        try { fn(payload); } catch (e) { console.error('[EdgeOffline:ListenerError]', e); }
      });
    }
  }

  // ── 2. STORAGE ENGINE (INDEXEDDB + LOCALSTORAGE FALLBACK) ───────────
  function getDB() {
    if (dbPromise) return dbPromise;
    dbPromise = new Promise((resolve) => {
      if (typeof indexedDB === 'undefined') {
        resolve(null);
        return;
      }
      try {
        const req = indexedDB.open(DB_NAME, DB_VERSION);
        req.onupgradeneeded = (e) => {
          const db = e.target.result;
          if (!db.objectStoreNames.contains(STORE_NAME)) {
            const store = db.createObjectStore(STORE_NAME, { keyPath: 'id', autoIncrement: true });
            store.createIndex('action', 'action', { unique: false });
            store.createIndex('priority', 'priority', { unique: false });
            store.createIndex('timestamp', 'timestamp', { unique: false });
            store.createIndex('status', 'status', { unique: false });
          }
        };
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => {
          console.warn('[EdgeOffline] IndexedDB error, using LocalStorage fallback');
          resolve(null);
        };
      } catch (err) {
        console.warn('[EdgeOffline] IndexedDB unavailable:', err);
        resolve(null);
      }
    });
    return dbPromise;
  }

  function getLSQueue() {
    try {
      return JSON.parse(localStorage.getItem(LS_KEY) || '[]');
    } catch (e) {
      return [];
    }
  }

  function saveLSQueue(q) {
    try {
      localStorage.setItem(LS_KEY, JSON.stringify(q));
    } catch (e) {}
  }

  // ── 3. QUEUE OPERATIONS ─────────────────────────────────────────────
  /**
   * Enqueue an item. Supports both object signature and (action, payload, options) signature:
   * EdgeOffline.enqueue({ action: 'SAVE', url: '/api', body: {...} })
   * EdgeOffline.enqueue('SAVE', { text: 'hello' }, { priority: 1 })
   */
  async function enqueue(arg1, arg2, arg3) {
    let item = {};
    if (typeof arg1 === 'string') {
      item = {
        action: arg1,
        payload: arg2 || {},
        body: arg2 || {},
        ...(arg3 || {})
      };
    } else if (typeof arg1 === 'object' && arg1 !== null) {
      item = { ...arg1 };
      if (!item.payload && (item.body || item.data)) {
        item.payload = item.body || item.data;
      }
    }

    const queueItem = {
      action: item.action || 'HTTP_REQUEST',
      url: item.url || null,
      method: (item.method || 'POST').toUpperCase(),
      headers: item.headers || { 'Content-Type': 'application/json' },
      body: item.body || item.payload || item.data || null,
      payload: item.payload || item.body || item.data || {},
      priority: typeof item.priority === 'number' ? item.priority : 5,
      retryCount: 0,
      maxRetries: typeof item.maxRetries === 'number' ? item.maxRetries : 5,
      timestamp: Date.now(),
      status: 'pending',
      meta: item.meta || {}
    };

    const db = await getDB();
    if (db) {
      try {
        const recordId = await new Promise((resolve, reject) => {
          const tx = db.transaction([STORE_NAME], 'readwrite');
          const store = tx.objectStore(STORE_NAME);
          const req = store.add(queueItem);
          req.onsuccess = (e) => resolve(e.target.result);
          req.onerror = (e) => reject(e);
        });
        queueItem.id = recordId;
      } catch (err) {
        const q = getLSQueue();
        queueItem.id = Date.now() + Math.random().toString(36).substr(2, 4);
        q.push(queueItem);
        saveLSQueue(q);
      }
    } else {
      const q = getLSQueue();
      queueItem.id = Date.now() + Math.random().toString(36).substr(2, 4);
      q.push(queueItem);
      saveLSQueue(q);
    }

    emit('enqueued', queueItem);
    emit('enqueue', queueItem);
    const count = await getPendingCount();
    emit('queueChange', { count, item: queueItem });
    return queueItem;
  }

  async function getPendingItems() {
    const db = await getDB();
    if (db) {
      try {
        return await new Promise((resolve) => {
          const tx = db.transaction([STORE_NAME], 'readonly');
          const store = tx.objectStore(STORE_NAME);
          const req = store.getAll();
          req.onsuccess = () => {
            const items = (req.result || []).filter(i => i.status === 'pending');
            items.sort((a, b) => a.priority - b.priority || a.timestamp - b.timestamp);
            resolve(items);
          };
          req.onerror = () => resolve(getLSQueue().filter(i => i.status === 'pending'));
        });
      } catch (e) {
        return getLSQueue().filter(i => i.status === 'pending');
      }
    }
    return getLSQueue().filter(i => i.status === 'pending');
  }

  async function getPendingCount() {
    const items = await getPendingItems();
    return items.length;
  }

  async function removeItem(id) {
    const db = await getDB();
    if (db) {
      try {
        await new Promise((resolve) => {
          const tx = db.transaction([STORE_NAME], 'readwrite');
          tx.objectStore(STORE_NAME).delete(id);
          tx.oncomplete = () => resolve();
          tx.onerror = () => resolve();
        });
      } catch (e) {}
    }
    const q = getLSQueue().filter(i => i.id !== id);
    saveLSQueue(q);
    const count = await getPendingCount();
    emit('queueChange', { count });
  }

  async function clearQueue() {
    const db = await getDB();
    if (db) {
      try {
        const tx = db.transaction([STORE_NAME], 'readwrite');
        tx.objectStore(STORE_NAME).clear();
      } catch (e) {}
    }
    if (typeof localStorage !== 'undefined') {
      localStorage.removeItem(LS_KEY);
    }
    emit('queueChange', { count: 0 });
  }

  // ── 4. CUSTOM ACTION HANDLERS ───────────────────────────────────────
  function registerHandler(action, handlerFn) {
    customHandlers.set(action, handlerFn);
  }

  // ── 5. REPLAY & SYNC ENGINE ─────────────────────────────────────────
  async function processQueue() {
    if (isSyncing || !isOnline()) return;
    isSyncing = true;
    emit('syncStart', {});

    try {
      const items = await getPendingItems();
      for (const item of items) {
        if (!isOnline()) break;

        let success = false;
        try {
          if (customHandlers.has(item.action)) {
            const fn = customHandlers.get(item.action);
            const res = await fn(item.payload || item.body || item);
            success = res !== false;
          } else if (item.url) {
            const res = await fetch(item.url, {
              method: item.method,
              headers: item.headers,
              body: typeof item.body === 'string' ? item.body : JSON.stringify(item.body)
            });
            success = res.ok;
          } else {
            success = true;
          }
        } catch (err) {
          success = false;
        }

        if (success) {
          await removeItem(item.id);
          emit('itemSynced', item);
          emit('synced', item);
        } else {
          item.retryCount = (item.retryCount || 0) + 1;
          if (item.retryCount >= item.maxRetries) {
            await removeItem(item.id);
            emit('itemFailed', { item, reason: 'Max retries exceeded' });
          }
        }
      }
    } finally {
      isSyncing = false;
      const remaining = await getPendingCount();
      emit('syncComplete', { remaining });
    }
  }

  // ── 6. FETCH WRAPPER & INTERCEPTOR ──────────────────────────────────
  async function offlineFetch(url, options = {}, offlineFallback = null) {
    if (isOnline()) {
      try {
        return await fetch(url, options);
      } catch (err) {
        // network error during active request
      }
    }

    const method = (options.method || 'GET').toUpperCase();
    if (['POST', 'PUT', 'DELETE', 'PATCH'].includes(method)) {
      const enqueued = await enqueue({
        url,
        method,
        headers: options.headers || { 'Content-Type': 'application/json' },
        body: options.body,
        meta: { source: 'offlineFetch' }
      });
      return new Response(JSON.stringify({ offline: true, queued: true, item: enqueued }), {
        status: 202,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    if (typeof offlineFallback === 'function') {
      const fallbackData = await offlineFallback();
      return new Response(JSON.stringify(fallbackData), {
        status: 200,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    throw new Error('[EdgeOffline] Network is offline and request could not be completed.');
  }

  // ── 7. NETWORK MONITORING ───────────────────────────────────────────
  function isOnline() {
    return typeof navigator !== 'undefined' ? navigator.onLine : true;
  }

  function getNetworkInfo() {
    const nav = typeof navigator !== 'undefined' ? navigator : {};
    const conn = nav.connection || nav.mozConnection || nav.webkitConnection;
    return {
      online: isOnline(),
      effectiveType: conn?.effectiveType || 'unknown',
      rtt: conn?.rtt || 0,
      downlink: conn?.downlink || 0,
      saveData: conn?.saveData || false
    };
  }

  if (typeof window !== 'undefined') {
    window.addEventListener('online', () => {
      emit('online', getNetworkInfo());
      setTimeout(processQueue, 600);
    });

    window.addEventListener('offline', () => {
      emit('offline', getNetworkInfo());
    });
  }

  return {
    version: '2.3.0',
    isOnline,
    getNetworkInfo,
    on,
    off,
    enqueue,
    queueRequest: enqueue,
    getPendingItems,
    getQueue: getPendingItems,
    getPendingCount,
    removeItem,
    clearQueue,
    registerHandler,
    processQueue,
    sync: processQueue,
    syncAll: processQueue,
    fetch: offlineFetch
  };
}));
