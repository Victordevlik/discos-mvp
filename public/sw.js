self.addEventListener('install', e => { self.skipWaiting() })
self.addEventListener('activate', e => { self.clients.claim() })
self.addEventListener('fetch', e => {})
self.addEventListener('push', e => {
  let data = {}
  try { data = e.data ? e.data.json() : {} } catch { data = {} }
  const title = data.title || 'Discos'
  const body = data.body || ''
  const url = data.url || '/'
  const options = {
    body,
    icon: '/favicon.ico',
    badge: '/favicon.ico',
    data: { url, type: data.type || '', inviteId: data.inviteId || '' }
  }
  e.waitUntil(self.registration.showNotification(title, options))
})
self.addEventListener('notificationclick', e => {
  e.notification.close()
  const url = (e.notification && e.notification.data && e.notification.data.url) ? e.notification.data.url : '/'
  e.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then(list => {
      for (const c of list) {
        if ('focus' in c) {
          try { c.navigate(url) } catch {}
          return c.focus()
        }
      }
      if (self.clients.openWindow) return self.clients.openWindow(url)
      return null
    })
  )
})
