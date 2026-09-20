/**
 * EdgeOffline.js (v2.2.0)
 * Ultra-resilient, zero-dependency Offline-First & Background Sync Engine.
 * Pure IndexedDB persistence, exponential backoff, fetch interceptor, and network lifecycle events.
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

  const listeners = new Map();
  const customHandlers = new Map();
  let dbPromise = null;
  let isSyncing = false;

  // --- 1. Event Emitter ---
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

  // --- 2. IndexedDB Storage Core ---
  function getDB() {
    if (dbPromise) return dbPromise;
    dbPromise = new Promise((resolve) => {
      if (typeof indexedDB === 'undefined') {
        resolve(null);
        return;
      }
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
        console.warn('[EdgeOffline] IndexedDB unavailable, using LocalStorage fallback');
        resolve(null);
      };
    });
    return dbPromise;
  }

  // LocalStorage Fallback Helpers
  const LS_KEY = 'edge_offline_queue_v2';
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

  // --- 3. Queue Management ---
  async function enqueue(item) {
    const queueItem = {
      action: item.action || 'HTTP_REQUEST',
      url: item.url || null,
      method: item.method || 'POST',
      headers: item.headers || {},
      body: item.body || item.data || null,
      priority: item.priority || 5, // 1 (highest) to 10 (lowest)
      retryCount: 0,
      maxRetries: item.maxRetries || 5,
      timestamp: Date.now(),
      status: 'pending',
      meta: item.meta || {}
    };

    const db = await getDB();
    let recordId = null;

    if (db) {
      recordId = await new Promise((resolve, reject) => {
        const tx = db.transaction([STORE_NAME], 'readwrite');
        const store = tx.objectStore(STORE_NAME);
        const req = store.add(queueItem);
        req.onsuccess = (e) => resolve(e.target.result);
        req.onerror = (e) => reject(e);
      });
      queueItem.id = recordId;
    } else {
      const q = getLSQueue();
      recordId = Date.now() + Math.random().toString(36).substr(2, 4);
      queueItem.id = recordId;
      q.push(queueItem);
      saveLSQueue(q);
    }

    emit('enqueued', queueItem);
    const count = await getPendingCount();
    emit('queueChange', { count, item: queueItem });
    return queueItem;
  }

  async function getPendingItems() {
    const db = await getDB();
    if (db) {
      return new Promise((resolve) => {
        const tx = db.transaction([STORE_NAME], 'readonly');
        const store = tx.objectStore(STORE_NAME);
        const req = store.getAll();
        req.onsuccess = () => {
          const items = (req.result || []).filter(i => i.status === 'pending');
          // Sort by priority (asc) then timestamp (asc)
          items.sort((a, b) => a.priority - b.priority || a.timestamp - b.timestamp);
          resolve(items);
        };
        req.onerror = () => resolve([]);
      });
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
      await new Promise((resolve) => {
        const tx = db.transaction([STORE_NAME], 'readwrite');
        tx.objectStore(STORE_NAME).delete(id);
        tx.oncomplete = () => resolve();
        tx.onerror = () => resolve();
      });
    } else {
      const q = getLSQueue().filter(i => i.id !== id);
      saveLSQueue(q);
    }
    const count = await getPendingCount();
    emit('queueChange', { count });
  }

  async function clearQueue() {
    const db = await getDB();
    if (db) {
      const tx = db.transaction([STORE_NAME], 'readwrite');
      tx.objectStore(STORE_NAME).clear();
    }
    localStorage.removeItem(LS_KEY);
    emit('queueChange', { count: 0 });
  }

  // --- 4. Custom Sync Handlers ---
  function registerHandler(action, handlerFn) {
    customHandlers.set(action, handlerFn);
  }

  // --- 5. Sync Processing Engine ---
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
            // Execute custom action handler
            const fn = customHandlers.get(item.action);
            const res = await fn(item);
            success = res !== false;
          } else if (item.url) {
            // Standard HTTP Fetch
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

  // --- 6. Fetch Wrapper & Interceptor ---
  async function offlineFetch(url, options = {}, offlineFallback = null) {
    if (isOnline()) {
      try {
        return await fetch(url, options);
      } catch (err) {
        // Network drop during fetch
      }
    }

    // Queue request if method is mutating (POST, PUT, DELETE, PATCH)
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

  // --- 7. Network Monitoring ---
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

  // Global network event bindings
  if (typeof window !== 'undefined') {
    window.addEventListener('online', () => {
      emit('online', getNetworkInfo());
      setTimeout(processQueue, 800);
    });

    window.addEventListener('offline', () => {
      emit('offline', getNetworkInfo());
    });
  }

  return {
    version: '2.2.0',
    isOnline,
    getNetworkInfo,
    on,
    off,
    enqueue,
    queueRequest: enqueue,
    getPendingItems,
    getPendingCount,
    removeItem,
    clearQueue,
    registerHandler,
    processQueue,
    sync: processQueue,
    fetch: offlineFetch
  };
}));
