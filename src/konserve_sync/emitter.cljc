(ns konserve-sync.emitter
  "Emitter module for konserve-sync.

   Integrates with konserve write-hooks to detect changes and emit
   sync protocol messages. Supports filtering to control which keys
   are synced.

   The emitter is used by the server/source side to broadcast changes
   to subscribed clients."
  (:require [clojure.core.async :refer [chan put! close!]]
            [konserve.core :as k]
            [konserve-sync.protocol :as proto]))

;; =============================================================================
;; Write Hook Handler
;; =============================================================================

(defn make-write-hook
  "Create a write-hook function that emits sync messages.

   Parameters:
   - store-id: UUID identifying the store
   - update-ch: Channel to put update messages on
   - opts: Options map
     - :filter-fn (fn [key value] -> bool) - Filter which keys to sync
       Default: sync all keys

   Returns a write-hook function suitable for konserve's add-write-hook!

   The hook receives events like:
   {:type :write-hook/after-write
    :api-op :assoc-in | :update-in | :dissoc | :multi-assoc | ...
    :key <key>
    :value <value>
    :kvs {k1 v1, ...} (for multi-assoc)}

   For :multi-assoc, the filter is applied to each key-value pair,
   and only matching pairs are included in the emitted message."
  [store-id update-ch {:keys [filter-fn] :or {filter-fn (constantly true)}}]
  (fn [event]
    ;; Konserve write hooks provide events with :api-op, :key, :value, :kvs
    ;; (no :type field, just check for presence of :api-op)
    (when-let [api-op (:api-op event)]
      (let [{:keys [key value kvs]} event]
        (case api-op
          ;; Single key operations
          (:assoc :assoc-in :update :update-in :bassoc)
          (when (filter-fn key value)
            (put! update-ch (proto/make-update-msg store-id api-op key value nil)))

          :dissoc
          (when (filter-fn key nil)
            (put! update-ch (proto/make-update-msg store-id api-op key nil nil)))

          ;; Multi-key operation - filter each pair
          :multi-assoc
          (let [filtered-kvs (into {}
                                   (filter (fn [[k v]] (filter-fn k v)))
                                   kvs)]
            (when (seq filtered-kvs)
              (put! update-ch (proto/make-update-msg store-id api-op nil nil filtered-kvs))))

          ;; Unknown operation - skip
          nil)))))

;; =============================================================================
;; Emitter State
;; =============================================================================

(defrecord EmitterState
  [store-id      ; UUID of the store
   store         ; The konserve store
   store-config  ; Store configuration (for reference)
   update-ch     ; Channel for outgoing updates
   hook-id       ; ID returned by add-write-hook! (for removal)
   filter-fn     ; Filter function
   active?])     ; atom: boolean

(defn make-emitter-state
  "Create initial emitter state."
  [store-id store store-config update-ch hook-id filter-fn]
  (->EmitterState store-id
                  store
                  store-config
                  update-ch
                  hook-id
                  filter-fn
                  (atom true)))

(defn emitter-active?
  "Check if emitter is active."
  [state]
  @(:active? state))

(defn deactivate-emitter!
  "Mark emitter as inactive."
  [state]
  (reset! (:active? state) false))

;; =============================================================================
;; Emitter Lifecycle
;; =============================================================================

(defn create-emitter!
  "Create an emitter for a store.

   This sets up the write-hook on the store to capture changes and
   emit them as sync messages.

   Parameters:
   - store: The konserve store (must implement PWriteHookStore)
   - store-config: Configuration used to create the store (for store-id)
   - opts: Options map
     - :filter-fn (fn [key value] -> bool) - Filter which keys to sync
     - :buffer-size - Buffer size for update channel (default 100)

   Returns an EmitterState record.

   Note: The caller is responsible for consuming from the update-ch
   and sending messages to subscribers."
  [store store-config {:keys [filter-fn buffer-size]
                       :or {filter-fn (constantly true)
                            buffer-size 100}
                       :as opts}]
  (let [store-id (proto/store-id store-config)
        update-ch (chan buffer-size)
        hook-fn (make-write-hook store-id update-ch opts)
        ;; Register write hook with konserve using a unique hook-id
        hook-id (keyword (str "konserve-sync-" (random-uuid)))
        _ (k/add-write-hook! store hook-id hook-fn)]
    (make-emitter-state store-id store store-config update-ch hook-id filter-fn)))

(defn destroy-emitter!
  "Destroy an emitter and clean up resources.

   - Removes the write hook from the store
   - Closes the update channel
   - Marks the emitter as inactive"
  [emitter-state]
  (let [{:keys [store hook-id update-ch]} emitter-state]
    ;; Remove write hook
    (when hook-id
      (k/remove-write-hook! store hook-id))
    ;; Close update channel
    (close! update-ch)
    ;; Mark inactive
    (deactivate-emitter! emitter-state)))

;; =============================================================================
;; Initial Sync Support
;; =============================================================================

(defn emit-initial-sync!
  "Emit initial sync messages for all keys in a store.

   Used when a new client subscribes to send them the current state.

   Parameters:
   - emitter-state: The emitter state
   - keys-ch: Channel yielding keys from the store (from konserve/keys)
   - remote-keys: Set of keys the remote already has (to skip)

   Puts :key/write messages on the update channel for each key
   the remote doesn't have."
  [emitter-state keys-ch remote-keys]
  ;; Note: This function needs to be async-aware.
  ;; The actual implementation will use go blocks to:
  ;; 1. Iterate over keys from keys-ch
  ;; 2. Filter out keys in remote-keys
  ;; 3. Apply the filter-fn
  ;; 4. Fetch each value
  ;; 5. Emit :key/write messages
  ;; This is a placeholder for the sync.cljc orchestration to use.
  nil)

;; =============================================================================
;; Utility Functions
;; =============================================================================

(defn get-update-ch
  "Get the update channel from an emitter."
  [emitter-state]
  (:update-ch emitter-state))

(defn get-store-id
  "Get the store ID from an emitter."
  [emitter-state]
  (:store-id emitter-state))

(defn get-store
  "Get the store from an emitter."
  [emitter-state]
  (:store emitter-state))

