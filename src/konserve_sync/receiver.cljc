(ns konserve-sync.receiver
  "Receiver module for konserve-sync.

   Handles applying incoming sync updates to a local konserve store.
   Provides callback registration for key updates and error handling.

   The receiver is used by the client/subscriber side to apply changes
   from the server.

   Uses superv.async for proper error handling - store errors are
   propagated through the supervisor."
  (:require [superv.async :refer [go-try <?]]
            [konserve.core :as k]
            [konserve-sync.protocol :as proto]
            [konserve-sync.transport.protocol :as tp]))

;; =============================================================================
;; Error Types
;; =============================================================================

(defn apply-error
  "Create an error for failed update application."
  [operation key cause]
  (ex-info "Failed to apply update"
           {:type :konserve-sync/apply-error
            :operation operation
            :key key}
           cause))

;; =============================================================================
;; Apply Updates
;; =============================================================================

(defn apply-update!
  "Apply a sync update message to a store.

   Parameters:
   - S: Supervisor from superv.async
   - store: The konserve store to update
   - msg: A sync update message (must have :type :sync/update)

   Returns a channel that yields:
   - {:ok true} on success
   - Throws on failure (propagated to supervisor)

   Supports operations:
   - :key/assoc, :key/write - Write value at key
   - :key/dissoc - Delete key
   - :key/multi-assoc - Write multiple key-value pairs"
  [S store msg]
  (go-try S
    (when-not (proto/update-msg? msg)
      (throw (ex-info "Not an update message" {:msg msg})))

    (let [{:keys [operation key value kvs]} msg]
      (case operation
        ;; Single key write
        (:key/assoc :key/write)
        (do
          (<? S (k/assoc store key value))
          {:ok true})

        ;; Single key delete
        :key/dissoc
        (do
          (<? S (k/dissoc store key))
          {:ok true})

        ;; Multi-key write
        :key/multi-assoc
        (do
          ;; Note: konserve doesn't have a native multi-assoc
          ;; We apply each key-value pair sequentially using loop
          (loop [remaining (seq kvs)]
            (when remaining
              (let [[k v] (first remaining)]
                (<? S (k/assoc store k v))
                (recur (next remaining)))))
          {:ok true})

        ;; Unknown operation
        (throw (ex-info "Unknown operation" {:operation operation :msg msg}))))))

;; =============================================================================
;; Callback Management
;; =============================================================================

(defrecord CallbackRegistry
  [callbacks])  ; atom: {key {callback-id callback-fn}}

(defn make-callback-registry
  "Create an empty callback registry."
  []
  (->CallbackRegistry (atom {})))

(defn register-callback!
  "Register a callback for updates to a specific key.

   Parameters:
   - registry: The callback registry
   - key: The key to watch
   - callback: (fn [{:keys [store-id key value operation]}]) - Called on update

   Returns a function to unregister the callback."
  [registry key callback]
  (let [callback-id (random-uuid)]
    (swap! (:callbacks registry) assoc-in [key callback-id] callback)
    (fn []
      (swap! (:callbacks registry) update key dissoc callback-id)
      ;; Clean up empty key entries
      (swap! (:callbacks registry)
             (fn [cbs]
               (if (empty? (get cbs key))
                 (dissoc cbs key)
                 cbs))))))

(defn invoke-callbacks!
  "Invoke all callbacks registered for a key.

   Parameters:
   - registry: The callback registry
   - msg: The update message

   Returns nil. Errors in callbacks are caught and logged."
  [registry msg]
  (let [{:keys [key operation value kvs]} msg
        cbs @(:callbacks registry)]
    (case operation
      ;; Single key operations
      (:key/assoc :key/write :key/dissoc)
      (when-let [key-cbs (get cbs key)]
        (doseq [[_cb-id cb-fn] key-cbs]
          (try
            (cb-fn {:store-id (:store-id msg)
                    :key key
                    :value value
                    :operation operation})
            (catch #?(:clj Exception :cljs :default) _e
              ;; Log error but don't propagate
              nil))))

      ;; Multi-key - invoke for each key
      :key/multi-assoc
      (doseq [[k v] kvs]
        (when-let [key-cbs (get cbs k)]
          (doseq [[_cb-id cb-fn] key-cbs]
            (try
              (cb-fn {:store-id (:store-id msg)
                      :key k
                      :value v
                      :operation :key/assoc})
              (catch #?(:clj Exception :cljs :default) _e
                nil)))))

      ;; Unknown - ignore
      nil)))

;; =============================================================================
;; Update Handler
;; =============================================================================

(defn make-update-handler
  "Create a message handler that applies updates to a store.

   Parameters:
   - S: Supervisor from superv.async
   - store: The konserve store to update
   - callback-registry: (optional) CallbackRegistry for key callbacks
   - on-error: (fn [{:keys [error msg]}]) - Called when an update fails

   Returns a handler function suitable for transport.on-message!

   The handler:
   1. Applies the update to the store
   2. Invokes registered callbacks
   3. Calls on-error if update fails"
  [S store callback-registry on-error]
  (fn [msg]
    (when (proto/update-msg? msg)
      (go-try S
        (let [result (<? S (apply-update! S store msg))]
          (if (:error result)
            ;; Propagate error
            (on-error result)
            ;; Success - invoke callbacks
            (when callback-registry
              (invoke-callbacks! callback-registry msg))))))))

;; =============================================================================
;; Receiver State
;; =============================================================================

(defrecord ReceiverState
  [S              ; Supervisor
   store-id       ; UUID of the store being synced
   store          ; The local konserve store
   callbacks      ; CallbackRegistry
   unregister-fn  ; Function to unregister from transport
   on-error       ; Error callback
   active?])      ; atom: boolean

(defn make-receiver-state
  "Create initial receiver state."
  [S store-id store callbacks unregister-fn on-error]
  (->ReceiverState S
                   store-id
                   store
                   callbacks
                   unregister-fn
                   on-error
                   (atom true)))

(defn receiver-active?
  "Check if receiver is active."
  [state]
  @(:active? state))

(defn deactivate-receiver!
  "Mark receiver as inactive."
  [state]
  (reset! (:active? state) false))

;; =============================================================================
;; Receiver Lifecycle
;; =============================================================================

(defn create-receiver!
  "Create a receiver that applies updates from a transport to a store.

   Parameters:
   - S: Supervisor from superv.async
   - store-id: UUID of the store to receive updates for
   - store: The local konserve store
   - transport: Transport implementing PSyncTransport
   - opts: Options map
     - :on-error (fn [{:keys [error msg]}]) - Error handler (required)

   Returns a ReceiverState record.

   The receiver filters messages by store-id and only applies updates
   for the matching store."
  [S store-id store transport {:keys [on-error]}]
  {:pre [(some? on-error)]}
  (let [callbacks (make-callback-registry)
        handler (make-update-handler S store callbacks on-error)
        ;; Filter by store-id
        filtered-handler (fn [msg]
                          (when (= store-id (:store-id msg))
                            (handler msg)))
        ;; Register with transport
        unregister-fn (tp/on-message! transport filtered-handler)]
    (make-receiver-state S store-id store callbacks unregister-fn on-error)))

(defn destroy-receiver!
  "Destroy a receiver and clean up resources.

   - Unregisters from transport
   - Marks receiver as inactive"
  [receiver-state]
  (let [{:keys [unregister-fn]} receiver-state]
    (when unregister-fn
      (unregister-fn))
    (deactivate-receiver! receiver-state)))

;; =============================================================================
;; Convenience Functions
;; =============================================================================

(defn register-key-callback!
  "Register a callback for a specific key on a receiver.

   Convenience wrapper around register-callback!

   Parameters:
   - receiver-state: The receiver state
   - key: The key to watch
   - callback: (fn [{:keys [store-id key value operation]}])

   Returns a function to unregister the callback."
  [receiver-state key callback]
  (register-callback! (:callbacks receiver-state) key callback))
