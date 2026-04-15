// sw.js — Service Worker do Sellvance PWA
// Estrategia: network-first com fallback offline para paginas ja visitadas.

const CACHE_NAME = 'sellvance-v1';
const OFFLINE_URLS = [
  '/',
  '/dashboard',
  '/static/offline.html',
];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => {
      return cache.addAll(OFFLINE_URLS).catch(() => {});
    })
  );
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', event => {
  const req = event.request;

  // Nunca cachear POST/PUT/DELETE ou rotas admin
  if (req.method !== 'GET') return;
  if (req.url.includes('/admin/') || req.url.includes('/api/')) return;
  if (req.url.includes('/healthz')) return;

  event.respondWith(
    fetch(req)
      .then(resp => {
        // Cachear apenas respostas ok
        if (resp && resp.status === 200 && resp.type === 'basic') {
          const respClone = resp.clone();
          caches.open(CACHE_NAME).then(cache => {
            cache.put(req, respClone).catch(() => {});
          });
        }
        return resp;
      })
      .catch(() => caches.match(req).then(cached => cached || caches.match('/static/offline.html')))
  );
});

// Listener para notificacoes push (futura integracao)
self.addEventListener('push', event => {
  if (!event.data) return;
  const data = event.data.json();
  const options = {
    body: data.body || '',
    icon: '/static/icon-192.png',
    badge: '/static/icon-192.png',
    vibrate: [100, 50, 100],
    data: data.url || '/'
  };
  event.waitUntil(
    self.registration.showNotification(data.title || 'Sellvance', options)
  );
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  event.waitUntil(
    clients.openWindow(event.notification.data || '/')
  );
});
