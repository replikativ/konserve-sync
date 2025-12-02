(ns konserve-sync.sync
  "Sync orchestration module for konserve-sync.

   Provides the SyncContext for managing sync state, and implements
   the initial sync and incremental streaming protocols.

   Server-side: register-store!, serve-subscription!
   Client-side: subscribe!

   All async operations use superv.async for proper error handling
   and supervision. Pass in a supervisor when creating the context."
  (:require [clojure.core.async :refer [chan put! close! alts! timeout]]
            [superv.async :refer [go-try go-loop-try <? >? put?]]
            [konserve.core :as k]
            [konserve-sync.protocol :as proto]
            [konserve-sync.emitter :as emitter]
            [konserve-sync.receiver :as receiver]
            [konserve-sync.transport.protocol :as tp]))

;; =============================================================================
;; Error Types
;; =============================================================================

(defn store-error
  "Create a store operation error."
  [op key cause]
  (ex-info "Store operation failed"
           {:type :konserve-sync/store-error
            :operation op
            :key key}
           cause))

(defn sync-error
  "Create a sync operation error."
  [phase store-id cause]
  (ex-info "Sync operation failed"
           {:type :konserve-sync/sync-error
            :phase phase
            :store-id store-id}
           cause))

(defn transport-error
  "Create a transport error."
  [op transport cause]
  (ex-info "Transport operation failed"
           {:type :konserve-sync/transport-error
            :operation op}
           cause))

;; =============================================================================
;; SyncContext State
;; =============================================================================

(defrecord SyncContext
  [S       ; supervisor from superv.async - passed in from outside
   state   ; atom containing sync state
   opts])  ; configuration options

;; State atom structure:
;; {:stores {store-id {:store <konserve-store>
;;                     :config <store-config>
;;                     :emitter <EmitterState>}}
;;  :subscriptions {store-id {:subscribers #{transport ...}
;;                            :pending-acks {transport batch-idx}}}
;;  :receivers {store-id {:receiver <ReceiverState>
;;                        :transport <transport>}}}

(defn make-context
  "Create a new SyncContext.

   Parameters:
   - S: Supervisor from superv.async (required)
   - opts: Options map
     - :batch-size - Number of keys per batch during initial sync (default 20)
     - :batch-timeout-ms - Timeout for batch acks in ms (default 30000)

   Example:
   ```clojure
   (require '[superv.async :refer [S]])
   (def ctx (make-context S {:batch-size 50}))
   ```"
  ([S]
   (make-context S {}))
  ([S opts]
   (->SyncContext S
                  (atom {:stores {}
                         :subscriptions {}
                         :receivers {}})
                  (merge {:batch-size 20
                          :batch-timeout-ms 30000}
                         opts))))

;; =============================================================================
;; Server-Side: Store Registration
;; =============================================================================

(defn register-store!
  "Register a store for sync (server-side).

   Sets up the write-hook to capture changes. The store becomes
   available for clients to subscribe to.

   Parameters:
   - ctx: SyncContext
   - store: The konserve store
   - store-config: Configuration used to create the store
   - opts: Options map
     - :filter-fn (fn [key value] -> bool) - Filter which keys to sync

   Returns the store-id."
  [ctx store store-config opts]
  (let [S (:S ctx)
        store-id (proto/store-id store-config)
        emitter-state (emitter/create-emitter! store store-config opts)]
    (swap! (:state ctx) assoc-in [:stores store-id]
           {:store store
            :config store-config
            :emitter emitter-state})
    ;; Start forwarding updates to subscribers with supervision
    (go-loop-try S [msg (<? S (emitter/get-update-ch emitter-state))]
      (when msg
        (when (emitter/emitter-active? emitter-state)
          ;; Broadcast to all subscribers
          (let [subs (get-in @(:state ctx) [:subscriptions store-id :subscribers])]
            (loop [remaining (seq subs)]
              (when remaining
                (let [transport (first remaining)
                      result (<? S (tp/send! transport msg))]
                  (when (:error result)
                    ;; Log but continue - don't fail the whole broadcast
                    ;; The error is reported to supervisor
                    nil))
                (recur (next remaining))))))
        (recur (<? S (emitter/get-update-ch emitter-state)))))
    store-id))

(defn unregister-store!
  "Unregister a store from sync.

   Removes write-hook and cleans up resources."
  [ctx store-id]
  (when-let [store-data (get-in @(:state ctx) [:stores store-id])]
    (emitter/destroy-emitter! (:emitter store-data))
    (swap! (:state ctx) update :stores dissoc store-id)))

;; =============================================================================
;; Server-Side: Subscription Handling
;; =============================================================================

(defn- send-initial-keys!
  "Send initial sync messages for keys the remote doesn't have or has stale versions.

   Compares timestamps to detect stale keys - if server's last-write is newer
   than client's, the key is sent.

   Uses batching with acks for flow control.
   Propagates store errors through the supervisor."
  [ctx store-id transport remote-key-timestamps]
  (let [S (:S ctx)]
    (go-try S
      (let [{:keys [batch-size batch-timeout-ms]} (:opts ctx)
            store-data (get-in @(:state ctx) [:stores store-id])
            store (:store store-data)
            emitter (:emitter store-data)
            filter-fn (or (:filter-fn emitter) (constantly true))]

        (if-not store
          {:error (sync-error :initial-sync store-id
                              (ex-info "Store not registered" {:store-id store-id}))}

          ;; Get all keys with metadata and filter by timestamp
          ;; k/keys returns maps like {:key :foo, :type :edn, :last-write #inst ...}
          (let [all-key-metas (<? S (k/keys store))
                ;; Filter to keys that need syncing:
                ;; 1. Key doesn't exist on client (not in remote-key-timestamps)
                ;; 2. Server's last-write is newer than client's
                keys-to-send (filter
                               (fn [{:keys [key last-write]}]
                                 (let [client-timestamp (get remote-key-timestamps key)]
                                   (and (filter-fn key nil)
                                        (or (nil? client-timestamp)
                                            (pos? (compare last-write client-timestamp))))))
                               all-key-metas)
                ;; Extract just the keys for batching
                keys-to-send (map :key keys-to-send)
                batches (partition-all batch-size keys-to-send)]

            ;; Send each batch
            (loop [remaining-batches batches
                   batch-idx 0]
              (if (empty? remaining-batches)
                ;; All done
                {:ok true :batches-sent batch-idx}

                ;; Send this batch
                (let [batch (first remaining-batches)]
                  ;; Send update for each key in batch
                  (loop [keys-remaining (seq batch)]
                    (when keys-remaining
                      (let [key (first keys-remaining)
                            value (<? S (k/get store key))
                            msg (proto/make-initial-sync-msg store-id key value)
                            send-result (<? S (tp/send! transport msg))]
                        (when (:error send-result)
                          (throw (transport-error :send transport (:error send-result))))
                        (recur (next keys-remaining)))))

                  ;; Send batch-complete
                  (let [send-result (<? S (tp/send! transport (proto/make-batch-complete-msg store-id batch-idx)))]
                    (when (:error send-result)
                      (throw (transport-error :send transport (:error send-result)))))

                  ;; Wait for ack (with timeout)
                  (swap! (:state ctx) assoc-in
                         [:subscriptions store-id :pending-acks transport] batch-idx)

                  ;; Wait for ack or timeout
                  (let [ack-received (atom false)
                        start-time #?(:clj (System/currentTimeMillis)
                                      :cljs (.now js/Date))]
                    (loop []
                      (let [current-ack (get-in @(:state ctx)
                                                [:subscriptions store-id :acked transport])]
                        (cond
                          ;; Ack received for this batch
                          (= current-ack batch-idx)
                          (reset! ack-received true)

                          ;; Timeout
                          (> (- #?(:clj (System/currentTimeMillis)
                                   :cljs (.now js/Date))
                                start-time)
                             batch-timeout-ms)
                          nil

                          ;; Keep waiting
                          :else
                          (do
                            (<? S (timeout 100))
                            (recur)))))

                    (if @ack-received
                      (recur (rest remaining-batches) (inc batch-idx))
                      {:error (sync-error :batch-ack store-id
                                          (ex-info "Batch ack timeout"
                                                   {:store-id store-id
                                                    :batch-idx batch-idx}))})))))))))))

(defn serve-subscription!
  "Handle a subscription request from a client.

   Parameters:
   - ctx: SyncContext
   - transport: The client's transport (PSyncTransport)
   - msg: The subscription request message

   Returns a channel that yields:
   - {:ok true} when subscription is established
   - {:error ex} on failure"
  [ctx transport msg]
  (let [S (:S ctx)]
    (go-try S
      (let [{:keys [store-id local-key-timestamps id]} msg]
        (if-not (get-in @(:state ctx) [:stores store-id])
          ;; Store not found
          (do
            (<? S (tp/send! transport (proto/make-subscribe-error-msg store-id id "Store not found")))
            {:error (sync-error :subscribe store-id
                                (ex-info "Store not found" {:store-id store-id}))})

          ;; Store exists - start subscription
          (do
            ;; Send ack
            (<? S (tp/send! transport (proto/make-subscribe-ack-msg store-id id)))

            ;; Add to subscribers
            (swap! (:state ctx) update-in [:subscriptions store-id :subscribers]
                   (fnil conj #{}) transport)

            ;; Set up handler for batch acks
            (tp/on-message! transport
                            (fn [ack-msg]
                              (when (and (= :sync/batch-ack (:type ack-msg))
                                         (= store-id (:store-id ack-msg)))
                                (swap! (:state ctx) assoc-in
                                       [:subscriptions store-id :acked transport]
                                       (:batch-idx ack-msg)))))

            ;; Send initial keys (comparing timestamps)
            (let [result (<? S (send-initial-keys! ctx store-id transport (or local-key-timestamps {})))]
              (if (:error result)
                result
                (do
                  ;; Send complete message
                  (<? S (tp/send! transport (proto/make-complete-msg store-id)))
                  {:ok true})))))))))

(defn remove-subscriber!
  "Remove a subscriber from a store.

   Called when a client disconnects."
  [ctx store-id transport]
  (swap! (:state ctx) update-in [:subscriptions store-id :subscribers]
         disj transport)
  (swap! (:state ctx) update-in [:subscriptions store-id :pending-acks]
         dissoc transport)
  (swap! (:state ctx) update-in [:subscriptions store-id :acked]
         dissoc transport))

;; =============================================================================
;; Client-Side: Subscription
;; =============================================================================

(defn subscribe!
  "Subscribe to a remote store (client-side).

   Parameters:
   - ctx: SyncContext
   - transport: Transport to the server (PSyncTransport)
   - store-id: UUID of the store to subscribe to
   - local-store: Local konserve store to sync to
   - opts: Options map
     - :on-error (fn [{:keys [error msg]}]) - Error handler (required)
     - :on-complete (fn []) - Called when initial sync completes

   Returns a channel that yields:
   - {:ok true} when subscription is established and initial sync completes
   - {:error ex} on failure"
  [ctx transport store-id local-store {:keys [on-error on-complete] :as opts}]
  {:pre [(some? on-error)]}
  (let [S (:S ctx)]
    (go-try S
      (let [;; Get local keys with timestamps for differential sync
            ;; k/keys returns maps like {:key :foo, :type :edn, :last-write #inst "..."}
            local-keys-metas (<? S (k/keys local-store))
            local-key-timestamps (into {}
                                       (map (fn [{:keys [key last-write]}]
                                              [key last-write]))
                                       local-keys-metas)

            ;; Create subscription message with timestamps
            sub-msg (proto/make-subscribe-msg store-id local-key-timestamps)

            ;; Track completion
            complete-ch (chan 1)]

        ;; Set up message handler for this subscription
        (let [receiver-state (receiver/create-receiver! S store-id local-store transport opts)

              ;; Additional handler for control messages
              control-handler
              (tp/on-message! transport
                              (fn [msg]
                                (when (= store-id (:store-id msg))
                                  (case (:type msg)
                                    :sync/subscribe-ack
                                    nil  ; Subscription acknowledged

                                    :sync/subscribe-error
                                    (put! complete-ch {:error (sync-error :subscribe store-id
                                                                          (ex-info (:error msg) {:msg msg}))})

                                    :sync/batch-complete
                                    ;; Send ack back
                                    (do
                                      (tp/send! transport
                                                (proto/make-batch-ack-msg store-id (:batch-idx msg)))
                                      nil)

                                    :sync/complete
                                    (do
                                      (when on-complete (on-complete))
                                      (put! complete-ch {:ok true}))

                                    ;; Other messages handled by receiver
                                    nil))))]

          ;; Store receiver state
          (swap! (:state ctx) assoc-in [:receivers store-id]
                 {:receiver receiver-state
                  :transport transport
                  :control-handler control-handler})

          ;; Send subscription request
          (<? S (tp/send! transport sub-msg))

          ;; Wait for completion or timeout
          (let [[result _] (alts! [complete-ch (timeout 60000)])]
            (or result
                {:error (sync-error :subscribe store-id
                                    (ex-info "Subscription timeout" {:store-id store-id}))})))))))

(defn unsubscribe!
  "Unsubscribe from a remote store.

   Cleans up receiver and removes subscription state."
  [ctx store-id]
  (when-let [receiver-data (get-in @(:state ctx) [:receivers store-id])]
    (receiver/destroy-receiver! (:receiver receiver-data))
    (when-let [control-handler (:control-handler receiver-data)]
      (control-handler))  ; Unregister
    (swap! (:state ctx) update :receivers dissoc store-id)))

;; =============================================================================
;; Utility Functions
;; =============================================================================

(defn get-store
  "Get a registered store by ID."
  [ctx store-id]
  (get-in @(:state ctx) [:stores store-id :store]))

(defn get-store-ids
  "Get all registered store IDs."
  [ctx]
  (keys (get @(:state ctx) :stores)))

(defn get-subscribers
  "Get all transports subscribed to a store."
  [ctx store-id]
  (get-in @(:state ctx) [:subscriptions store-id :subscribers]))

(defn register-callback!
  "Register a callback for key updates on a subscription.

   Must be called after subscribe! Returns unregister function."
  [ctx store-id key callback]
  (when-let [receiver-data (get-in @(:state ctx) [:receivers store-id])]
    (receiver/register-key-callback! (:receiver receiver-data) key callback)))
