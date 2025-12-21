# konserve-sync

A synchronization layer for [konserve](https://github.com/replikativ/konserve) key-value stores. Enables real-time replication from a primary store to multiple subscribers over pluggable transports.

## Features

- **Single-writer replication**: Primary store broadcasts updates to subscribers in real-time
- **Differential sync**: On reconnection, only keys with newer server timestamps are transferred
- **Batched initial sync**: Backpressure via acknowledgments prevents overwhelming slow clients
- **Pluggable transports**: Channel-based (testing) and kabel (network) transports included
- **Custom key discovery**: Walk functions for tree-structured data (e.g., Datahike)

## Installation

```clojure
{:deps {io.replikativ/konserve-sync {:git/url "https://github.com/replikativ/konserve-sync"
                                      :git/sha "..."}}}
```

## Quick Start

### Server

```clojure
(require '[konserve-sync.sync :as sync]
         '[konserve-sync.protocol :as proto]
         '[superv.async :refer [S]])

;; Create context and register store
(def ctx (sync/make-context S {:batch-size 20}))
(def store-config {:scope #uuid "12345678-1234-1234-1234-123456789abc"
                   :backend :memory})
(sync/register-store! ctx my-store store-config {})
```

### Client

```clojure
;; Use SAME store config as server
(def store-id (proto/store-id store-config))

(<!! (sync/subscribe! ctx transport store-id local-store
       {:on-error #(println "Error:" %)
        :on-complete #(println "Sync complete")}))
```

## Store Identification

Stores are identified by a UUID computed from their config via `proto/store-id`. **Client and server must use identical configs** for the same logical store.

The `:scope` key represents the store's logical identity:

```clojure
(def store-config
  {:scope #uuid "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
   :backend :file
   :path "/tmp/my-store"})
```

## Transports

### Channel Transport (Testing)

```clojure
(require '[konserve-sync.transport.channels :as ch])

(let [[transport-a transport-b] (ch/channel-pair S)]
  ;; Messages sent on A arrive at B and vice versa
  ...)
```

### Kabel Transport (Network)

```clojure
(require '[konserve-sync.transport.kabel :as kabel-sync])

;; Server: Add sync middleware to kabel peer
(peer/server-peer S handler server-id
  (fn [peer-config]
    (when (nil? @sync-server)
      (reset! sync-server (kabel-sync/kabel-sync-server S ctx nil)))
    ((kabel-sync/sync-server-middleware @sync-server) peer-config))
  identity)

;; Client: Add sync middleware and use transport for subscribe!
(peer/client-peer S client-id
  (kabel-sync/sync-client-middleware ctx transport-atom)
  identity)
```

## Configuration

### Context Options

```clojure
(sync/make-context S
  {:batch-size 20           ;; Keys per batch during initial sync
   :batch-timeout-ms 30000  ;; Timeout waiting for batch ack
   })
```

### Store Registration Options

```clojure
(sync/register-store! ctx store store-config
  {:filter-fn   (fn [key value] true)  ;; Filter which keys to sync
   :walk-fn     (fn [store opts] ...)  ;; Custom key discovery (returns channel of keys)
   :key-sort-fn (fn [key] 0)})         ;; Sort keys for sync order (higher = later)
```

#### :walk-fn - Custom Key Discovery

For tree-structured data, syncing ALL keys is inefficient. The `:walk-fn` discovers only reachable keys:

```clojure
;; Walk function signature: (fn [store opts] -> channel-yielding-set-of-keys)
(sync/register-store! ctx store config
  {:walk-fn my-walk-fn})
```

#### :key-sort-fn - Sync Order Control

When some keys depend on others, use `:key-sort-fn` to control sync order. Keys with higher sort values are sent later:

```clojure
;; Send :db last (after its dependencies)
(sync/register-store! ctx store config
  {:key-sort-fn (fn [k] (if (= k :db) 1 0))})
```

This prevents "not found" errors when callbacks try to use a key before its dependencies are synced.

## Datahike Integration

Datahike stores its database as a `:db` key containing BTSet indices, where each BTSet node is stored as a separate UUID key. The `konserve-sync.walkers.datahike` namespace provides a walker that discovers only reachable keys.

### Server Setup

```clojure
(require '[konserve-sync.walkers.datahike :as dh-walker])

(def sync-store-config
  {:scope #uuid "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
   :backend :file
   :path "/tmp/my-datahike-store"})

;; Register with walker and key-sort-fn
;; - walk-fn: only sync keys reachable from :db
;; - key-sort-fn: send :db last so BTSet nodes arrive first
(sync/register-store! ctx (-> conn d/db :store) sync-store-config
  {:walk-fn dh-walker/datahike-walk-fn
   :key-sort-fn (fn [k] (if (= k :db) 1 0))})
```

### Client Setup (ClojureScript)

```clojure
(require '[konserve-sync.walkers.datahike :as dh-walker]
         '[konserve.tiered :as tiered])

;; Create TieredStore: memory (fast) + IndexedDB (persistent)
(let [frontend (<! (memory/new-mem-store))
      backend (<! (indexeddb/connect-idb-store "my-datahike"))
      store (<! (tiered/connect-tiered-store
                  frontend backend
                  :write-policy :write-through
                  :read-policy :frontend-only))]
  ;; Sync only reachable keys from IndexedDB to memory
  (<! (tiered/perform-walk-sync frontend backend [:db]
        (dh-walker/make-tiered-walk-fn) {}))

  ;; Subscribe to server updates
  (<! (sync/subscribe! ctx @transport store-id store opts))

  ;; Register callback for :db updates
  (sync/register-callback! ctx store-id :db
    (fn [{:keys [value]}]
      (go (<! (refresh-local-db!))))))
```

### What the Walker Discovers

- `:db` - The stored database root
- All BTSet node UUIDs from `eavt`, `aevt`, `avet` indices
- Temporal index nodes (if `keep-history?` is true)
- `:schema-meta-key` - Schema metadata

### Optional Dependency

The walker requires `datahike` on the classpath (not a dependency of konserve-sync):

```clojure
{:deps {io.replikativ/datahike {:mvn/version "0.6.1610"}}}
```

## Sync Protocol

### Initial Sync

1. Client sends `{key -> last-write-timestamp}` for local keys
2. Server identifies keys where client is missing or stale
3. Keys sent in batches with flow control
4. Server sends completion message

### Incremental Updates

After initial sync, writes broadcast automatically:

```clojure
;; Server write -> automatic broadcast
(<!! (k/assoc server-store :key "value"))

;; Client receives via callback
(sync/register-callback! ctx store-id :key
  (fn [{:keys [value]}] (println "Updated:" value)))
```

## API Summary

| Function | Description |
|----------|-------------|
| `make-context` | Create sync context |
| `register-store!` | Register store (server) |
| `subscribe!` | Subscribe to remote store (client) |
| `register-callback!` | Register key update callback |
| `unregister-store!` | Unregister store |
| `unsubscribe!` | Unsubscribe from store |

## License

Copyright 2025 Christian Weilbach. Apache License 2.0.
