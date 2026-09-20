# ⚡ EdgeOffline (v2.2.0)

> Universal, zero-dependency, ultra-resilient Offline-First & Background Sync Engine.

Designed for web applications, mobile sites, and PWAs that require bulletproof data persistence when connectivity fluctuates or drops entirely.

---

## 📦 Features

- **🛡️ Pure & Non-Intrusive:** Zero DOM injection, zero layout pollution. Pure event-driven JavaScript.
- **💾 IndexedDB Priority Queue:** Outbox pattern with priority sorting, timestamps, and automatic retry management.
- **🔄 Auto Background Sync:** Detects `online` events and transparently drains pending queues.
- **🌐 Fetch Interceptor:** `EdgeOffline.fetch(url, options)` automatically falls back to outbox queue on mutations (POST/PUT/DELETE).
- **📱 PWA Ready:** Full Service Worker caching recipe included.

---

## 🚀 Quick Start

### 1. Script Tag
```html
<script src="dist/edge-offline.min.js"></script>
<script>
  // Listen to network changes
  EdgeOffline.on('offline', () => {
    console.log('User is offline');
  });

  EdgeOffline.on('online', () => {
    console.log('User back online, syncing outbox...');
  });

  // Queue a background operation
  EdgeOffline.queueRequest({
    action: 'CREATE_ORDER',
    url: '/api/order',
    data: { items: [...], total: 180 }
  });
</script>
```

---

## 📜 License
MIT © Origin Edge
