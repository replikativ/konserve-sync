(ns konserve-sync.core
  "Public API for konserve-sync.

   konserve-sync enables real-time synchronization of konserve stores
   across processes, machines, or between server and browser.

   Uses superv.async for proper error handling and supervision.
   Pass a supervisor when creating the context to integrate with
   your existing supervision hierarchy.

   ## Quick Start

   Server-side (source store):
   ```clojure
   (require '[konserve-sync.core :as sync])
   (require '[superv.async :refer [S]])

   ;; Create sync context with supervisor
   (def ctx (sync/make-context S))

   ;; Register store for sync
   (sync/register-store! ctx my-store store-config)

   ;; Handle new connections (transport-specific)
   ;; When a client sends :sync/subscribe, call:
   (sync/serve-subscription! ctx transport msg)
   ```

   Client-side (subscriber):
   ```clojure
   (require '[superv.async :refer [S]])

   ;; Create sync context with supervisor
   (def ctx (sync/make-context S))

   ;; Subscribe to remote store
   (<! (sync/subscribe! ctx transport store-id local-store
         {:on-error (fn [e] (log/error e))}))

   ;; Register callback for specific key updates
   (sync/register-callback! ctx store-id :my-key
     (fn [{:keys [value]}]
       (println \"my-key updated:\" value)))
   ```

   ## Transport Adapters

   konserve-sync is transport-agnostic. Use one of the built-in adapters:

   - `konserve-sync.transport.channels` - core.async channels (local/testing)
   - `konserve-sync.transport.kabel` - kabel peer/middleware (production)

   Or implement the `PSyncTransport` protocol for custom transports."
  (:require [konserve-sync.protocol :as proto]
            [konserve-sync.sync :as sync]
            [konserve-sync.receiver :as receiver]))

;; =============================================================================
;; Context Management
;; =============================================================================

(def make-context
  "Create a new SyncContext.

   Parameters:
   - S: Supervisor from superv.async (required)
   - opts: Options map (optional)
     - :batch-size - Number of keys per batch during initial sync (default 20)
     - :batch-timeout-ms - Timeout for batch acks in ms (default 30000)

   Example:
   ```clojure
   (require '[superv.async :refer [S]])
   (def ctx (make-context S {:batch-size 50}))
   ```"
  sync/make-context)

;; =============================================================================
;; Server-Side API
;; =============================================================================

(def register-store!
  "Register a konserve store for sync (server-side).

   Sets up write-hook integration to capture changes. After registration,
   clients can subscribe to receive updates.

   Parameters:
   - ctx: SyncContext from make-context
   - store: The konserve store (must implement PWriteHookStore)
   - store-config: Configuration used to create the store
   - opts: Options map
     - :filter-fn (fn [key value] -> bool) - Filter which keys to sync

   Returns the store-id (UUID).

   Example:
   ```clojure
   (def store-id (register-store! ctx my-store
                   {:backend :file :path \"/data/store\"}
                   {:filter-fn (fn [k _] (not= k :private))}))
   ```"
  sync/register-store!)

(def unregister-store!
  "Unregister a store from sync.

   Removes write-hook and cleans up resources. Existing subscribers
   will stop receiving updates."
  sync/unregister-store!)

(def serve-subscription!
  "Handle a subscription request from a client (server-side).

   Call this when you receive a :sync/subscribe message from a client.

   Parameters:
   - ctx: SyncContext
   - transport: The client's transport (PSyncTransport)
   - msg: The :sync/subscribe message

   Returns a channel that yields:
   - {:ok true} when subscription is established
   - {:error ex} on failure

   Example (in transport message handler):
   ```clojure
   (when (= :sync/subscribe (:type msg))
     (serve-subscription! ctx client-transport msg))
   ```"
  sync/serve-subscription!)

(def remove-subscriber!
  "Remove a subscriber from a store (server-side).

   Call this when a client disconnects to clean up state.

   Parameters:
   - ctx: SyncContext
   - store-id: UUID of the store
   - transport: The client's transport"
  sync/remove-subscriber!)

;; =============================================================================
;; Client-Side API
;; =============================================================================

(def subscribe!
  "Subscribe to a remote store (client-side).

   Initiates connection to a remote store and syncs data to a local store.
   Initial sync sends all keys the local store doesn't have, then streams
   incremental updates.

   Parameters:
   - ctx: SyncContext
   - transport: Transport to the server (PSyncTransport)
   - store-id: UUID of the store to subscribe to
   - local-store: Local konserve store to sync data into
   - opts: Options map
     - :on-error (fn [{:keys [error msg]}]) - Error handler (required)
     - :on-complete (fn []) - Called when initial sync completes

   Returns a channel that yields:
   - {:ok true} when subscription and initial sync complete
   - {:error ex} on failure

   Example:
   ```clojure
   (go
     (let [result (<! (subscribe! ctx transport store-id local-store
                        {:on-error #(log/error \"Sync error\" %)
                         :on-complete #(log/info \"Sync complete!\")}))]
       (if (:ok result)
         (log/info \"Subscribed successfully\")
         (log/error \"Subscription failed\" (:error result)))))
   ```"
  sync/subscribe!)

(def unsubscribe!
  "Unsubscribe from a remote store (client-side).

   Stops receiving updates and cleans up resources."
  sync/unsubscribe!)

(def register-callback!
  "Register a callback for updates to a specific key.

   Must be called after subscribe! has completed.

   Parameters:
   - ctx: SyncContext
   - store-id: UUID of the subscribed store
   - key: The key to watch
   - callback: (fn [{:keys [store-id key value operation]}])

   Returns a function to unregister the callback.

   Example:
   ```clojure
   (let [unregister (register-callback! ctx store-id :users
                      (fn [{:keys [value]}]
                        (render-users value)))]
     ;; Later, to stop watching:
     (unregister))
   ```"
  sync/register-callback!)

;; =============================================================================
;; Utility Functions
;; =============================================================================

(def store-id
  "Compute store-id from configuration.

   If config contains :sync/id, uses that directly.
   Otherwise, hashes the config to produce a stable UUID.

   Use :sync/id for stable IDs across config changes:
   ```clojure
   {:sync/id #uuid \"12345678-...\"
    :backend :file
    :path \"/data/store\"}
   ```"
  proto/store-id)

(def get-store
  "Get a registered store by ID (server-side)."
  sync/get-store)

(def get-store-ids
  "Get all registered store IDs (server-side)."
  sync/get-store-ids)

(def get-subscribers
  "Get all transports subscribed to a store (server-side)."
  sync/get-subscribers)

;; =============================================================================
;; Message Validation
;; =============================================================================

(def valid-msg?
  "Check if a message is a valid sync protocol message.

   Useful for filtering/validating incoming messages."
  proto/valid-msg?)

(def subscribe-msg?
  "Check if a message is a :sync/subscribe request."
  proto/subscribe-msg?)

(def update-msg?
  "Check if a message is a :sync/update."
  proto/update-msg?)

(def control-msg?
  "Check if a message is a control message (not data).

   Control messages: subscribe, subscribe-ack, subscribe-error,
   batch-complete, batch-ack, complete"
  proto/control-msg?)

