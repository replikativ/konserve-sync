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
;; Server: Register the Datahike backing store
(def server-ctx (sync/make-context S))
(sync/register-store! server-ctx datahike-store {:sync/id store-id} {})

;; Client: Subscribe and receive updates
(def client-ctx (sync/make-context S))
(sync/subscribe! client-ctx transport store-id local-datahike-store
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
(require '[konserve-sync.core :as sync]
         '[konserve-sync.transport.channels :as ch]
         '[konserve.memory :refer [new-mem-store]]
         '[superv.async :refer [S]])

;; Create sync context with supervisor
(def server-ctx (sync/make-context S {:batch-size 20}))

;; Create and register your store
(def server-store (<!! (new-mem-store)))
(def store-id (sync/register-store! server-ctx server-store
                                     {:sync/id #uuid "12345678-..."}
                                     {}))

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
(require '[konserve-sync.core :as sync]
         '[konserve-sync.transport.channels :as ch])

;; Create client context
(def client-ctx (sync/make-context S))

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
(require '[konserve-sync.transport.kabel :as kabel-sync]
         '[kabel.peer :as peer]
         '[kabel.http-kit :as http-kit])

;; Server
(def handler (http-kit/create-http-kit-handler! S "ws://0.0.0.0:8080" server-id))
(def sync-server (kabel-sync/kabel-sync-server S server-ctx nil))
(def server-peer (peer/server-peer S handler server-id
                   (kabel-sync/sync-server-middleware sync-server)
                   identity))
(peer/start server-peer)

;; Client
(def transport-atom (atom nil))
(def client-peer (peer/client-peer S client-id
                   (kabel-sync/sync-client-middleware client-ctx transport-atom)
                   identity))
(peer/connect S client-peer "ws://server:8080")
;; After connection, @transport-atom contains the transport
```

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
                (not= key :private-key))})
```

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

```bash
# Run tests
clj -M:test

# Run specific test namespace
clj -M:test -n konserve-sync.sync-test
```

## License

Copyright 2025 Christian Weilbach

Distributed under the Eclipse Public License 1.0.
