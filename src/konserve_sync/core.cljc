(ns konserve-sync.core
  "Public API for konserve-sync.

   konserve-sync enables real-time synchronization of konserve stores
   across processes, machines, or between server and browser using
   kabel.pubsub for transport.

   ## Quick Start

   Server-side (source store):
   ```clojure
   (require '[konserve-sync.core :as sync])
   (require '[kabel.peer :as peer])
   (require '[kabel.http-kit :as http-kit])
   (require '[superv.async :refer [S <??]])

   ;; Create server peer with sync middleware
   (def handler (http-kit/create-http-kit-handler! S \"ws://localhost:8080\" server-id))
   (def server-peer (peer/server-peer S handler server-id
                      (sync/server-middleware)
                      identity))
   (<?? S (peer/start server-peer))

   ;; Register store for sync
   (sync/register-store! server-peer :my-store my-store
     {:filter-fn (fn [k _] (not= k :private))})
   ```

   Client-side (subscriber):
   ```clojure
   ;; Create client peer with sync middleware
   (def client-peer (peer/client-peer S client-id
                      (sync/client-middleware)
                      identity))
   (<?? S (peer/connect S client-peer \"ws://localhost:8080\"))

   ;; Subscribe to remote store
   (<! (sync/subscribe-store! client-peer :my-store local-store
         {:on-key-update (fn [k v op] (println \"Updated:\" k))
          :on-complete #(println \"Sync complete!\")}))
   ```

   ## Datahike Integration

   For Datahike stores, use the datahike walker for efficient reachability-based sync:

   ```clojure
   (require '[konserve-sync.walkers.datahike :as walkers])

   (sync/register-store! server-peer :datahike-store store
     {:walk-fn walkers/datahike-walk-fn
      :key-sort-fn (fn [k] (if (= k :db) 1 0))})  ; Send :db last
   ```"
  (:require [konserve-sync.pubsub :as pubsub]
            [konserve-sync.transport.kabel-pubsub :as kp]))

;; =============================================================================
;; Middleware
;; =============================================================================

(def server-middleware
  "Create kabel middleware for server-side store sync.

   Returns a middleware function for use with peer/server-peer.

   Example:
   ```clojure
   (def server-peer (peer/server-peer S handler server-id
                      (sync/server-middleware)
                      identity))
   ```"
  kp/server-middleware)

(def client-middleware
  "Create kabel middleware for client-side store sync.

   Returns a middleware function for use with peer/client-peer.

   Example:
   ```clojure
   (def client-peer (peer/client-peer S client-id
                      (sync/client-middleware)
                      identity))
   ```"
  kp/client-middleware)

;; =============================================================================
;; Server-Side API
;; =============================================================================

(def register-store!
  "Register a konserve store for sync (server-side).

   Sets up the store as a pubsub topic with automatic write-hook
   integration to broadcast changes to subscribers.

   Parameters:
   - peer: The kabel server peer atom
   - topic: Topic identifier (keyword or any EDN value)
   - store: The konserve store (source of truth)
   - opts: Options map
     - :filter-fn (fn [key value] -> bool) - Filter which keys to sync
     - :walk-fn (fn [store opts] -> channel) - Custom key discovery function
       (e.g., datahike-walk-fn for reachability-based sync)
     - :key-sort-fn (fn [key] -> comparable) - Sort keys for sync order
     - :batch-size - Items per batch during handshake (default 20)

   Returns the topic.

   Example:
   ```clojure
   (sync/register-store! server-peer :my-store my-store
     {:filter-fn (fn [k _] (not= k :private))
      :batch-size 50})
   ```"
  kp/register-store!)

(def unregister-store!
  "Unregister a store from sync (server-side).

   Removes write-hooks and unregisters the pubsub topic.
   Existing subscribers will stop receiving updates."
  kp/unregister-store!)

(def get-subscribers
  "Get all transports subscribed to a store topic (server-side)."
  kp/get-subscribers)

(def topic-registered?
  "Check if a topic is registered (server-side)."
  kp/topic-registered?)

;; =============================================================================
;; Client-Side API
;; =============================================================================

(def subscribe-store!
  "Subscribe to a remote store and sync to a local store (client-side).

   Initiates connection to a remote store and syncs data to a local store.
   Uses timestamp-based differential sync - only keys where server's
   timestamp is newer than client's are transferred.

   Parameters:
   - peer: The kabel client peer atom
   - topic: The topic/store-id to subscribe to
   - local-store: Local konserve store to sync data into
   - opts: Options map
     - :on-key-update (fn [key value operation]) - Called after each update
       operation is :handshake, :assoc, or :dissoc
     - :on-complete (fn []) - Called when initial sync completes

   Returns a channel that yields:
   - {:ok true} when subscription and initial sync complete
   - {:error ...} on failure

   Example:
   ```clojure
   (go
     (let [result (<! (sync/subscribe-store! client-peer :my-store local-store
                        {:on-key-update (fn [k v op] (println \"Update:\" k op))
                         :on-complete #(println \"Sync complete!\")}))]
       (if (:ok result)
         (println \"Subscribed!\")
         (println \"Error:\" (:error result)))))
   ```"
  kp/subscribe-store!)

(def unsubscribe-store!
  "Unsubscribe from a store (client-side).

   Stops receiving updates from the remote store.

   Parameters:
   - peer: The kabel client peer atom
   - topic: The topic/store-id to unsubscribe from

   Returns a channel that yields {:ok true}."
  kp/unsubscribe-store!)

;; =============================================================================
;; Strategy Constructors (Advanced Use)
;; =============================================================================

(def store-sync-strategy
  "Create a StoreSyncStrategy for client-side use.

   For advanced use cases where you need direct access to the strategy.
   Most users should use subscribe-store! instead.

   Parameters:
   - store: Local konserve store to sync into
   - opts: Options map
     - :on-key-update (fn [key value operation]) - Called after each update"
  pubsub/store-sync-strategy)

(def server-store-strategy
  "Create a StoreSyncStrategy for server-side use.

   For advanced use cases where you need direct access to the strategy.
   Most users should use register-store! instead.

   Parameters:
   - store: Server konserve store (source of truth)
   - opts: Options map
     - :filter-fn (fn [key value] -> bool) - Filter which keys to sync
     - :walk-fn (fn [store opts] -> channel) - Custom key discovery
     - :key-sort-fn (fn [key] -> comparable) - Sort keys for sync order"
  pubsub/server-store-strategy)
