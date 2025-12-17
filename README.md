# konserve-sync

A synchronization layer for [konserve](https://github.com/replikativ/konserve) key-value stores. Enables real-time replication from a primary store to multiple subscribers over pluggable transports.

## Overview

konserve-sync provides:

- **Single-writer replication**: A primary store broadcasts updates to subscribers in real-time
- **Timestamp-based differential sync**: On reconnection, only keys with newer server timestamps are transferred
- **Batched initial sync with flow control**: Backpressure via acknowledgments prevents overwhelming slow clients
- **Pluggable transports**: Channel-based (for testing) and kabel (for network) transports included
- **Supervision integration**: Built on [superv.async](https://github.com/replikativ/superv.async) for proper error handling

## Use Cases

### Database Synchronization

Synchronize a Datahike database between server and clients:

```clojure
;; Shared store config - MUST match between server and client
;; The :scope UUID is the logical identity of the store
(def store-config
  {:scope #uuid "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
   :backend :file
   :path "/tmp/my-datahike-store"})

;; Server: Register the Datahike backing store
(def server-ctx (sync/make-context S))
(sync/register-store! server-ctx datahike-store store-config {})

;; Client: Subscribe and receive updates
(def client-ctx (sync/make-context S))
(def store-id (proto/store-id store-config))  ; Compute from config
(sync/subscribe! client-ctx transport store-id local-store
                 {:on-error #(log/error "Sync error:" %)
                  :on-complete #(log/info "Initial sync complete")})
```

### Backend Replication

Replicate stores across backend services for read scaling or redundancy:

```clojure
;; Primary node registers its store
(sync/register-store! ctx primary-store store-config {})

;; Replica nodes subscribe
(sync/subscribe! replica-ctx transport store-id replica-store opts)
```

## Architecture

```
┌─────────────────┐         ┌─────────────────┐
│  Primary Store  │         │  Subscriber 1   │
│   (writer)      │────────▶│  (read replica) │
│                 │         └─────────────────┘
│  register-      │         ┌─────────────────┐
│  store!         │────────▶│  Subscriber 2   │
│                 │         │  (read replica) │
└─────────────────┘         └─────────────────┘
        │
        ▼
   Write hooks capture
   all mutations and
   broadcast to subscribers
```

### Single-Writer Model

konserve-sync is designed for a **single-writer architecture**:

- One primary store handles all writes
- Subscribers receive updates passively
- No conflict resolution needed (timestamps from single source)
- Subscribers can have local read caches

For multi-writer scenarios, you need explicit coordination on top of the stores (e.g., CRDTs with [replikativ](https://github.com/replikativ/replikativ), operational transforms).

## Installation

Add to your `deps.edn`:

```clojure
{:deps {io.replikativ/konserve-sync {:git/url "https://github.com/replikativ/konserve-sync"
                                      :git/sha "..."}}}
```

## Quick Start

### Server Setup

```clojure
(require '[konserve-sync.sync :as sync]
         '[konserve-sync.protocol :as proto]
         '[konserve-sync.transport.channels :as ch]
         '[konserve.memory :refer [new-mem-store]]
         '[superv.async :refer [S]])

;; Create sync context with supervisor
(def server-ctx (sync/make-context S {:batch-size 20}))

;; Define store config - :scope is the logical identity
(def store-config {:scope #uuid "12345678-1234-1234-1234-123456789abc"
                   :backend :memory})

;; Create and register your store
(def server-store (<!! (new-mem-store)))
(sync/register-store! server-ctx server-store store-config {})

;; Create server for accepting connections
(def server (ch/channel-server S))

;; Handle new connections
(tp/on-connection! server
  (fn [transport]
    ;; Connection handler is called for each new client
    (println "Client connected")))
```

### Client Setup

```clojure
(require '[konserve-sync.sync :as sync]
         '[konserve-sync.protocol :as proto]
         '[konserve-sync.transport.channels :as ch])

;; Create client context
(def client-ctx (sync/make-context S))

;; Use SAME store config as server - this is critical!
(def store-config {:scope #uuid "12345678-1234-1234-1234-123456789abc"
                   :backend :memory})

;; Compute store-id from config (must match server)
(def store-id (proto/store-id store-config))

;; Create local store for synced data
(def client-store (<!! (new-mem-store)))

;; Connect to server
(def transport (ch/connect-to-server server))

;; Subscribe to remote store
(<!! (sync/subscribe! client-ctx transport store-id client-store
       {:on-error (fn [e] (println "Error:" e))
        :on-complete (fn [] (println "Initial sync complete"))}))
```

## Transports

### Channel Transport (Testing)

For unit tests and same-process communication:

```clojure
(require '[konserve-sync.transport.channels :as ch])

;; Create connected pair
(let [[transport-a transport-b] (ch/channel-pair S)]
  ;; Messages sent on A arrive at B and vice versa
  ...)

;; Or use server/client model
(def server (ch/channel-server S))
(def client-transport (ch/connect-to-server server))
```

### Kabel Transport (Network)

For network communication via WebSockets:

```clojure
(require '[konserve-sync.sync :as sync]
         '[konserve-sync.transport.kabel :as kabel-sync]
         '[kabel.peer :as peer]
         '[kabel.http-kit :as http-kit]
         '[superv.async :refer [S]])

;; ===== SERVER =====
(def server-ctx (sync/make-context S {:batch-size 20}))
(defonce sync-server (atom nil))

(def handler (http-kit/create-http-kit-handler! S "ws://0.0.0.0:8080" server-id))
(def server-peer
  (peer/server-peer S handler server-id
    (fn [peer-config]
      ;; Create sync-server lazily on first connection
      (when (nil? @sync-server)
        (reset! sync-server (kabel-sync/kabel-sync-server S server-ctx nil)))
      ((kabel-sync/sync-server-middleware @sync-server) peer-config))
    identity))
(peer/start server-peer)

;; ===== CLIENT =====
(def client-ctx (sync/make-context S))
(def transport-atom (atom nil))  ; Will be populated by middleware

(def client-peer
  (peer/client-peer S client-id
    (kabel-sync/sync-client-middleware client-ctx transport-atom)
    identity))

;; Connect - after this returns, @transport-atom contains the transport
(peer/connect S client-peer "ws://server:8080")

;; Now you can subscribe using @transport-atom
(sync/subscribe! client-ctx @transport-atom store-id local-store opts)
```

## Store Identification

Stores are identified by a UUID computed from their normalized configuration via `proto/store-id`. For sync to work, **client and server must compute the same store-id**.

### The :scope Key

The `:scope` key in store config represents the logical identity of a store across different network environments:

```clojure
(def store-config
  {:scope #uuid "a1b2c3d4-e5f6-7890-abcd-ef1234567890"  ; Shared identity
   :backend :file
   :path "/tmp/my-store"})

;; Both client and server use the same config → same store-id
(proto/store-id store-config)
;; => #uuid "30508a38-7d11-5135-966b-081e395d7d0b"
```

### Volatile Keys (Excluded from Hash)

Certain runtime-specific keys are excluded when computing the store-id:
- `:opts` - Runtime options
- `:serializers` - Serializer instances
- `:cache` - Cache implementations
- `:read-handlers` / `:write-handlers` - Fressian handlers

This allows different runtimes to have different serialization setups while still identifying as the same logical store.

### Common Pitfall: Config Mismatch

If you get "Store not found" errors, the client and server configs likely don't match:

```clojure
;; Server uses Datahike's runtime config (includes extra keys!)
(sync/register-store! ctx (-> conn d/db :store)
                      (-> conn d/db :config :store)  ; ← Has extra keys!
                      {})

;; Client uses minimal config
(def store-config {:scope my-scope :backend :file :path "/tmp/db"})
(proto/store-id store-config)  ; ← Different hash!
```

**Solution**: Use a fixed config literal on both sides:

```clojure
;; Define ONCE and share between client/server
(def sync-store-config
  {:scope #uuid "a1b2c3d4-..."
   :backend :file
   :path "/tmp/my-store"})
```

## Datahike Walker

Datahike stores its database as a `:db` key containing BTSet indices, where each BTSet node is stored as a separate UUID key. Over time, a Datahike store accumulates many historical keys, but only a fraction are actually reachable from the current `:db` state.

The `konserve-sync.walkers.datahike` namespace provides a ready-to-use walker that discovers only reachable keys:

```clojure
(require '[konserve-sync.walkers.datahike :as dh-walker])

;; Server: Use walk-fn for reachability-based initial sync
(sync/register-store! ctx datahike-store store-config
  {:walk-fn dh-walker/datahike-walk-fn})

;; Client: Use with tiered store's perform-walk-sync
(require '[konserve.tiered :as tiered])

(tiered/perform-walk-sync frontend backend [:db]
  (dh-walker/make-tiered-walk-fn)
  {})
```

### What the Walker Discovers

The walker traverses from `:db` and collects:
- `:db` - The stored database root
- All BTSet node UUIDs from `eavt`, `aevt`, `avet` indices
- All BTSet node UUIDs from temporal indices (if `keep-history?` is true)
- `:schema-meta-key` - Schema metadata needed for queries

### Performance Impact

Without walker (syncing all keys):
- Syncs entire store including historical/unreachable data
- Sync time grows linearly with store size

With walker (reachability-based):
- Only syncs keys reachable from current `:db` state
- Sync time depends on current database size, not history

### Optional Dependency

The walker requires `datahike` and `persistent-sorted-set` on the classpath. These are **not** dependencies of konserve-sync - add them to your project:

```clojure
;; deps.edn
{:deps {io.replikativ/datahike {:mvn/version "0.6.1610"}}}
```

## Datahike Integration

Complete example of syncing a Datahike database between server and browser client.

### Server (Clojure)

```clojure
(ns my-app.server
  (:require [konserve-sync.sync :as sync]
            [konserve-sync.transport.kabel :as kabel-sync]
            [konserve-sync.walkers.datahike :as dh-walker]
            [kabel.peer :as peer]
            [kabel.http-kit :as http-kit]
            [datahike.api :as d]
            [superv.async :refer [S go-super <?]]))

;; Shared config - copy to client code
(def store-scope #uuid "a1b2c3d4-e5f6-7890-abcd-ef1234567890")
(def sync-store-config
  {:scope store-scope
   :backend :file
   :path "/tmp/my-datahike-store"})

;; Sync context
(def sync-ctx (sync/make-context S {:batch-size 20}))
(defonce sync-server (atom nil))

;; Server peer with sync middleware
(def server-id #uuid "00000000-0000-0000-0000-000000000001")
(def server
  (peer/server-peer S
    (http-kit/create-http-kit-handler! S "ws://localhost:8080" server-id)
    server-id
    (fn [peer-config]
      (when (nil? @sync-server)
        (reset! sync-server (kabel-sync/kabel-sync-server S sync-ctx nil)))
      ((kabel-sync/sync-server-middleware @sync-server) peer-config))
    identity))

;; Register Datahike's store with walker for reachability-based sync
(defn setup-sync! [conn]
  (let [store (-> conn d/db :store)]
    (sync/register-store! sync-ctx store sync-store-config
      {:walk-fn dh-walker/datahike-walk-fn})))

;; Start server
(defn -main []
  (go-super S
    (setup-sync! my-datahike-conn)
    (<? S (peer/start server))))
```

### Client (ClojureScript)

```clojure
(ns my-app.client
  (:require [konserve-sync.sync :as sync]
            [konserve-sync.protocol :as proto]
            [konserve-sync.transport.kabel :as kabel-sync]
            [konserve-sync.walkers.datahike :as dh-walker]
            [konserve.core :as k]
            [konserve.memory :as memory]
            [konserve.indexeddb :as indexeddb]
            [konserve.tiered :as tiered]
            [kabel.peer :as peer]
            [datahike.api :as d]
            [datahike.writing :as dsi]
            [datahike.store :as ds]
            [clojure.core.async :refer [go <!]]
            [superv.async :refer [S]]))

;; SAME config as server - critical!
(def store-scope #uuid "a1b2c3d4-e5f6-7890-abcd-ef1234567890")
(def sync-store-config
  {:scope store-scope
   :backend :file
   :path "/tmp/my-datahike-store"})

;; Client state
(defonce sync-ctx (sync/make-context S))
(defonce sync-transport (atom nil))
(defonce client-store (atom nil))
(defonce local-db (atom nil))

;; Create TieredStore: memory (fast reads) + IndexedDB (persistence)
;; Uses walker to sync only reachable keys from IndexedDB to memory
(defn create-client-store! []
  (go
    (let [frontend (<! (memory/new-mem-store))
          backend (<! (indexeddb/connect-idb-store "my-datahike"))
          store (<! (tiered/connect-tiered-store
                      frontend backend
                      :write-policy :write-through
                      :read-policy :frontend-only))]
      ;; Use walker to sync only keys reachable from :db
      ;; This is much faster than sync-on-connect which syncs ALL keys
      (<! (tiered/perform-walk-sync
            frontend backend
            [:db]  ;; Root keys to start from
            (dh-walker/make-tiered-walk-fn)
            {}))
      (reset! client-store store)
      store)))

;; Subscribe to server's store
(defn subscribe-to-server! []
  (go
    (when-let [store @client-store]
      (when-let [transport @sync-transport]
        (let [store-id (proto/store-id sync-store-config)]
          (<! (sync/subscribe! sync-ctx transport store-id store
                {:on-error #(js/console.error "Sync error:" %)
                 :on-complete #(js/console.log "Initial sync complete")})))))))

;; Refresh local Datahike DB from synced store
(defn refresh-local-db! []
  (go
    (when-let [store @client-store]
      (let [prepared (ds/add-cache-and-handlers store datahike-config)]
        (when-let [stored-db (<! (k/get prepared :db))]
          (let [db (dsi/stored->db stored-db prepared)]
            (reset! local-db db)))))))

;; Initialize: create store, connect, subscribe, register callback
(defn init! []
  (go
    (<! (create-client-store!))
    (<! (subscribe-to-server!))
    ;; Register callback for :db key updates
    (let [store-id (proto/store-id sync-store-config)]
      (sync/register-callback! sync-ctx store-id :db
        (fn [{:keys [value]}]
          (go (<! (refresh-local-db!))))))
    (<! (refresh-local-db!))))
```

### Key Points

1. **Same config on both sides**: Server and client use identical `sync-store-config`
2. **Walker on both sides**: Server uses `walk-fn` in `register-store!`, client uses `perform-walk-sync`
3. **TieredStore for browser**: Memory frontend (fast, sync-compatible) + IndexedDB backend (persistent)
4. **Callback for reactivity**: Register callback on `:db` key to refresh when Datahike DB updates
5. **stored->db reconstruction**: Use Datahike's `stored->db` to reconstruct DB from synced store

## Initial Sync Protocol

When a client subscribes, konserve-sync performs differential synchronization:

1. **Client sends key timestamps**: The client sends a map of `{key -> last-write-timestamp}` for all keys in its local store

2. **Server computes delta**: The server compares timestamps and identifies keys where:
   - Key doesn't exist on client, OR
   - Server's `last-write` is newer than client's timestamp

3. **Batched transfer**: Keys are sent in configurable batches (default 20) with acknowledgment-based flow control

4. **Completion**: Server sends completion message when all keys are transferred

This ensures clients receive all updates they missed, even for keys they already have with stale values.

## Incremental Updates

After initial sync, updates flow in real-time:

```clojure
;; Server-side write
(<!! (k/assoc server-store :new-key "value"))
;; -> Automatically broadcast to all subscribers

;; Client receives update via registered callbacks
(sync/register-callback! client-ctx store-id :watched-key
  (fn [{:keys [key value operation]}]
    (println "Key" key "updated to" value)))
```

## Configuration Options

### Context Options

```clojure
(sync/make-context S
  {:batch-size 20          ;; Keys per batch during initial sync
   :batch-timeout-ms 30000 ;; Timeout waiting for batch ack
   })
```

### Store Registration Options

```clojure
(sync/register-store! ctx store store-config
  {:filter-fn (fn [key value]
                ;; Return true to sync this key, false to exclude
                (not= key :private-key))
   :walk-fn   (fn [store opts]
                ;; Return channel yielding set of keys to sync
                ;; If not provided, uses k/keys (all keys)
                ...)})
```

### Reachability-Based Sync with walk-fn

For stores with tree-structured data (like Datahike), syncing ALL keys is inefficient. The `:walk-fn` option lets you define custom key discovery that walks the data structure to find only reachable keys.

```clojure
;; Custom walk function signature:
;; (fn [store opts] -> channel-yielding-set-of-keys)

;; Server uses walk-fn during initial sync
(sync/register-store! ctx store config
  {:walk-fn my-walk-fn})
```

See [Datahike Walker](#datahike-walker) for a ready-to-use implementation.

## API Reference

### Core Functions

| Function | Description |
|----------|-------------|
| `make-context` | Create a SyncContext for managing sync state |
| `register-store!` | Register a store for sync (server-side) |
| `unregister-store!` | Unregister a store |
| `subscribe!` | Subscribe to a remote store (client-side) |
| `unsubscribe!` | Unsubscribe from a store |
| `register-callback!` | Register callback for key updates |

### Transport Protocol

Transports implement `PSyncTransport`:

```clojure
(defprotocol PSyncTransport
  (send! [this msg] "Send message, returns channel with result")
  (on-message! [this handler] "Register message handler, returns unregister fn")
  (close! [this] "Close the transport"))
```

### Server Protocol

Servers implement `PSyncServer`:

```clojure
(defprotocol PSyncServer
  (on-connection! [this handler] "Register connection handler")
  (broadcast! [this store-id msg] "Broadcast to subscribers"))
```

## Error Handling

konserve-sync uses [superv.async](https://github.com/replikativ/superv.async) for supervision:

```clojure
(require '[superv.async :refer [S go-try <?]])

;; Pass supervisor to context
(def ctx (sync/make-context S opts))

;; Errors propagate through supervisor hierarchy
;; Use your own supervisor for custom error handling:
(def my-supervisor (s/restarting-supervisor))
(def ctx (sync/make-context my-supervisor opts))
```

## Testing

Tests use [Kaocha](https://github.com/lambdaisland/kaocha):

```bash
# Run all tests
clj -M:test -m kaocha.runner

# Run specific test namespace
clj -M:test -m kaocha.runner --focus konserve-sync.sync-test

# Watch mode (re-run on file changes)
clj -M:test -m kaocha.runner --watch
```

## License

Copyright 2025 Christian Weilbach

Distributed under the Apache License 2.0.
