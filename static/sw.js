// Service Worker — handles push notifications when tab is closed

self.addEventListener('push', function(event) {
    let data = { title: 'New message', body: 'You have a new message.' };
    try {
        data = JSON.parse(event.data.text());
    } catch(e) {}

    event.waitUntil(
        self.registration.showNotification(data.title, {
            body:    data.body,
            icon:    '/static/icon.png',
            badge:   '/static/icon.png',
            tag:     'chat-message',          // replaces previous notification
            renotify: true,
            vibrate: [200, 100, 200],
            data:    { url: self.location.origin + '/' }
        })
    );
});

// Clicking the notification opens / focuses the chat tab
self.addEventListener('notificationclick', function(event) {
    event.notification.close();
    event.waitUntil(
        clients.matchAll({ type: 'window', includeUncontrolled: true }).then(function(list) {
            for (const client of list) {
                if (client.url === '/' || client.url.includes(self.location.origin)) {
                    return client.focus();
                }
            }
            return clients.openWindow('/');
        })
    );
});
