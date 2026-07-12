(ns konserve-sync.pubsub
  "StoreSyncStrategy for kabel.pubsub - synchronizes konserve stores.

   This module provides integration with kabel.pubsub for store synchronization.
   It implements timestamp-based differential sync:
   - Clients send {key -> last-write-timestamp} on subscribe
   - Server sends keys where its timestamp is newer than client's
   - Incremental updates (publishes) are applied to local store

   ## Server-Side Usage

   ```clojure
   (require '[kabel.pubsub :as pubsub])
   (require '[konserve-sync.pubsub :as ks-pubsub])

   ;; Register a konserve store as a pubsub topic
   (ks-pubsub/register-store! peer :my-store-topic server-store
     {:filter-fn (fn [k _] (not= k :private))
      :walk-fn custom-walk-fn  ; optional
      :key-sort-fn (fn [k] (if (= k :db) 1 0))})  ; optional
   ```

   ## Client-Side Usage

   ```clojure
   ;; Create client strategy with local store
   (def client-strategy (ks-pubsub/store-sync-strategy local-store {}))

   ;; Subscribe
   (pubsub/subscribe! peer #{:my-store-topic}
     {:strategies {:my-store-topic client-strategy}})
   ```"
  (:require #?(:clj [clojure.core.async :as async :refer [go go-loop chan put! close! <! >!]]
               :cljs [clojure.core.async :as async :refer [chan put! close!] :refer-macros [go go-loop]])
            [konserve.core :as k]
            [kabel.pubsub :as pubsub]
            [kabel.pubsub.protocol :as proto]
            [konserve-sync.log :as log]))

;; =============================================================================
;; Store Sync Strategy
;; =============================================================================

(defrecord StoreSyncStrategy
           [store        ; The konserve store (local on client, source on server)
            opts         ; {:filter-fn, :walk-fn, :key-sort-fn, :on-key-update}
            role])       ; :server or :client

(defn- get-local-key-timestamps
  "Get {key -> last-write} map from a konserve store.
   Returns a channel yielding the map."
  [store]
  (go
    (let [key-metas (<! (k/keys store))]
      (into {}
            (map (fn [{:keys [key last-write]}]
                   [key last-write]))
            key-metas))))

(defn- get-keys-to-sync
  "Get keys that need to be synced (server-side).
   Compares server timestamps against client timestamps.
   Returns a channel yielding seq of {:key k :value v}.

   `:always-send-fn` (optional, default: never) — a predicate on the key. Matching
   keys are sent on EVERY handshake, even when the subscriber's copy is already
   current. Use it for the small set of MUTABLE pointer cells (e.g. datahike's
   branch heads) that a subscriber gates on: pair it with `:key-sort-fn` so those
   keys sort LAST, and \"pointer applied\" then means \"every value I was missing
   has already been applied\" — a subscriber can expose its state on that signal
   alone. Timestamp-deduping such a pointer would break the gate, because an
   up-to-date subscriber would receive nothing to gate on. Keep the predicate
   narrow: matching keys cost a value transfer per handshake; the bulk
   (content-addressed, write-once values) still dedups on timestamps."
  [store client-timestamps {:keys [filter-fn walk-fn key-sort-fn always-send-fn]
                            :or {filter-fn (constantly true)
                                 always-send-fn (constantly false)}}]
  (go
    (let [;; Get keys - use walk-fn if provided, otherwise k/keys
          all-key-metas (if walk-fn
                          ;; walk-fn returns just keys, fetch metadata
                          (let [walked-keys (<! (walk-fn store {:sync? true}))]
                            (log/debug! {:id ::walk-fn-result
                                         :msg "Walk function returned keys"
                                         :data {:count (count walked-keys)}})
                            (loop [remaining (seq walked-keys)
                                   result []]
                              (if-not remaining
                                result
                                (let [k (first remaining)
                                      meta (<! (k/get-meta store k))]
                                  (recur (next remaining)
                                         (if meta
                                           (conj result {:key k :last-write (:last-write meta)
                                                         :immutable? (:immutable? meta)})
                                           result))))))
                          ;; Default: get all keys via k/keys
                          (<! (k/keys store)))

          ;; Filter to keys that need syncing
          keys-to-send (filter
                        (fn [{:keys [key last-write]}]
                          (let [client-timestamp (get client-timestamps key)]
                            (and (filter-fn key nil)
                                 (or (always-send-fn key)   ;; mutable gate pointers — see docstring
                                     (nil? client-timestamp)
                                     (pos? (compare last-write client-timestamp))))))
                        all-key-metas)

          ;; Sort if key-sort-fn provided
          sorted-keys (cond->> (map :key keys-to-send)
                        key-sort-fn (sort-by key-sort-fn))
          ;; keys whose stored metadata marks them immutable (content-addressed,
          ;; write-once) — the handshake item carries this so a reconnecting peer
          ;; that already holds the value skips re-storing (and re-publishing) it.
          immutable-keys (into #{} (comp (filter :immutable?) (map :key)) keys-to-send)]

      (log/debug! {:id ::keys-to-sync
                   :msg "Computed keys to sync"
                   :data {:count (count sorted-keys)}})

      ;; Fetch values for each key
      (loop [remaining (seq sorted-keys)
             result []]
        (if-not remaining
          result
          (let [k (first remaining)
                v (<! (k/get store k))]
            (recur (next remaining)
                   (conj result (cond-> {:key k :value v}
                                  (immutable-keys k) (assoc :meta {:immutable? true}))))))))))

(extend-type StoreSyncStrategy
  proto/PSyncStrategy

  (-init-client-state [this]
    ;; Client sends {key -> timestamp} for differential sync
    (if (= :client (:role this))
      (do
        (log/debug! {:id ::init-client-state
                     :msg "Initializing client state for differential sync"})
        (get-local-key-timestamps (:store this)))
      ;; Server doesn't send client state
      (let [ch (chan 1)]
        (close! ch)
        ch)))

  (-handshake-items [this client-state]
    ;; Server yields items to send during handshake
    (if (= :server (:role this))
      (let [ch (chan 100)]
        (go
          (log/debug! {:id ::handshake-items-start
                       :msg "Computing handshake items"
                       :data {:client-keys-count (count client-state)}})
          (let [items (<! (get-keys-to-sync (:store this)
                                            (or client-state {})
                                            (:opts this)))]
            (log/debug! {:id ::handshake-items-computed
                         :msg "Sending handshake items"
                         :data {:count (count items)}})
            (doseq [item items]
              (>! ch item))
            (close! ch)))
        ch)
      ;; Client doesn't produce handshake items
      (let [ch (chan)]
        (close! ch)
        ch)))

  (-apply-handshake-item [this {:keys [key value meta]}]
    ;; Client applies handshake item to local store
    (let [ch (chan 1)]
      (if (= :client (:role this))
        (go
          (try
            (if (and (:immutable? meta) (<! (k/exists? (:store this) key)))
              ;; immutable value already held (reconnect / overlap) — skip the
              ;; re-store so its write-hook doesn't re-publish (echo).
              (log/trace! {:id ::apply-handshake-skip-immutable :data {:key key}})
              (do
                (log/trace! {:id ::apply-handshake-item
                             :msg "Applying handshake item"
                             :data {:key key}})
                (<! (k/assoc (:store this) key value))
                ;; Invoke callback if provided
                (when-let [on-key-update (get-in this [:opts :on-key-update])]
                  (on-key-update key value :handshake))))
            (put! ch {:ok true})
            (catch #?(:clj Exception :cljs js/Error) e
              (log/error! {:id ::apply-handshake-error
                           :msg "Error applying handshake item"
                           :data {:key key :error e}})
              (put! ch {:error e})))
          (close! ch))
        ;; Server shouldn't receive handshake items
        (do
          (put! ch {:ok true})
          (close! ch)))
      ch))

  (-apply-publish [this {:keys [key value operation meta] :as payload}]
    ;; Apply publish to local store (both client and server can receive)
    (let [ch (chan 1)]
      (go
        (try
          (if (and (:immutable? meta)
                   (not= operation :dissoc)
                   (<! (k/exists? (:store this) key)))
            ;; immutable + already present ⇒ skip. No k/assoc ⇒ no write-hook ⇒ no
            ;; re-publish: the bidirectional echo terminates in ONE propagation wave
            ;; (content-addressed values are identical across peers, so "present"
            ;; means "identical"). Mutable cells (roots) never reach here — they ride
            ;; the convergent δ path, not the node push.
            (log/trace! {:id ::apply-publish-skip-immutable :data {:key key}})
            (do
              (case operation
                :dissoc
                (<! (k/dissoc (:store this) key))

                ;; Default: assoc
                (<! (k/assoc (:store this) key value)))

              ;; Invoke callback if provided
              (when-let [on-key-update (get-in this [:opts :on-key-update])]
                (on-key-update key value (or operation :assoc)))))

          (put! ch {:ok true})
          (catch #?(:clj Exception :cljs js/Error) e
            (log/error! {:id ::apply-publish-error
                         :msg "Error applying publish"
                         :data {:key key :error e}})
            (put! ch {:error e})))
        (close! ch))
      ch)))

;; =============================================================================
;; Strategy Constructors
;; =============================================================================

(defn store-sync-strategy
  "Create a StoreSyncStrategy for client-side use.

   Parameters:
   - store: Local konserve store to sync into
   - opts: Options map
     - :on-key-update (fn [key value operation]) - Called after each update
       operation is :handshake, :assoc, or :dissoc"
  [store opts]
  (->StoreSyncStrategy store opts :client))

(defn server-store-strategy
  "Create a StoreSyncStrategy for server-side use.

   Parameters:
   - store: Server konserve store (source of truth)
   - opts: Options map
     - :filter-fn (fn [key value] -> bool) - Filter which keys to sync
     - :walk-fn (fn [store opts] -> channel) - Custom key discovery
     - :key-sort-fn (fn [key] -> comparable) - Sort keys for sync order"
  [store opts]
  (->StoreSyncStrategy store opts :server))

;; =============================================================================
;; Convenience: Write Hook Integration
;; =============================================================================

(defn- make-write-hook
  "Create a write-hook that publishes changes to pubsub."
  [peer topic filter-fn key-sort-fn]
  (fn [event]
    (when-let [api-op (:api-op event)]
      (let [{:keys [key value kvs]} event
            subscribers (pubsub/get-subscribers peer topic)]
        (log/debug! {:id ::write-hook-event
                     :msg "Write hook triggered"
                     :data {:api-op api-op
                            :key key
                            :topic topic
                            :subscriber-count (count subscribers)}})
        (case api-op
          ;; Single key write operations
          (:assoc :assoc-in :update :update-in :bassoc)
          (when (filter-fn key value)
            (log/debug! {:id ::write-hook-publish
                         :msg "Publishing single key"
                         :data {:key key :topic topic :subscribers (count subscribers)}})
            ;; forward the value's metadata (:immutable? marks content-addressed,
            ;; write-once values) so a receiver can skip re-storing one it already
            ;; has — terminating the bidirectional write-hook echo.
            (pubsub/publish! peer topic (cond-> {:key key :value value :operation :assoc}
                                          (:meta event) (assoc :meta (:meta event)))))

          ;; Delete
          :dissoc
          (when (filter-fn key nil)
            (log/debug! {:id ::write-hook-publish
                         :msg "Publishing dissoc"
                         :data {:key key :topic topic :subscribers (count subscribers)}})
            (pubsub/publish! peer topic {:key key :operation :dissoc}))

          ;; Multi-key operation - sort keys to ensure proper ordering
          ;; (e.g., index nodes before :db for Datahike)
          :multi-assoc
          (let [sorted-kvs (cond->> kvs
                             key-sort-fn (sort-by (fn [[k _]] (key-sort-fn k))))]
            (log/debug! {:id ::write-hook-multi-assoc
                         :msg "Publishing multi-assoc"
                         :data {:key-count (count sorted-kvs)
                                :topic topic
                                :subscribers (count subscribers)
                                :keys (mapv first sorted-kvs)}})
            ;; per-key meta is a pure-data map {key -> meta} (e.g. mark nodes immutable but
            ;; the batch's mutable branch-head pointer not); look it up per key.
            (let [m (:meta event)]
              (doseq [[k v] sorted-kvs]
                (when (filter-fn k v)
                  (let [km (get m k)]
                    (pubsub/publish! peer topic (cond-> {:key k :value v :operation :assoc}
                                                  km (assoc :meta km))))))))

          ;; Unknown - ignore
          (log/warn! {:id ::write-hook-unknown-op
                      :msg "Unknown api-op in write hook"
                      :data {:api-op api-op}}))))))

(defn register-store!
  "Register a konserve store as a pubsub topic (server-side convenience).

   This:
   1. Creates a server StoreSyncStrategy
   2. Registers the topic with pubsub
   3. Sets up write-hooks to auto-publish changes

   Parameters:
   - peer: The kabel peer atom
   - topic: Topic identifier (any EDN value)
   - store: The konserve store to sync
   - opts: Options map
     - :filter-fn (fn [key value] -> bool) - Filter which keys to sync
     - :walk-fn (fn [store opts] -> channel) - Custom key discovery
     - :key-sort-fn (fn [key] -> comparable) - Sort keys for sync order
     - :always-send-fn (fn [key] -> bool) - Keys re-sent on EVERY handshake, even
       when the subscriber is current. For the mutable pointer cells a subscriber
       gates on; combine with :key-sort-fn so they sort last. Default: never.
     - :batch-size - Items per batch during handshake (default 20)
     - :item-timeout-ms - Timeout waiting for next item (default 10000 for walk-fn)

   Returns the topic."
  [peer topic store opts]
  (log/info! {:id ::register-store
              :msg "Registering store for pubsub"
              :data {:topic topic
                     :store-type (type store)}})
  (let [filter-fn (or (:filter-fn opts) (constantly true))
        key-sort-fn (:key-sort-fn opts)
        strategy (server-store-strategy store opts)
        hook-id (keyword (str "pubsub-" (if (keyword? topic) (name topic) (str topic))
                              "-" (random-uuid)))
        ;; When walk-fn is provided, use longer timeout since tree traversal takes time
        item-timeout-ms (or (:item-timeout-ms opts)
                            (if (:walk-fn opts) 10000 100))
        ;; Check if store supports write-hooks
        hooks-atom #?(:clj (try
                             (require 'konserve.protocols)
                             ((resolve 'konserve.protocols/-get-write-hooks) store)
                             (catch Exception _ nil))
                      :cljs (try
                              (konserve.protocols/-get-write-hooks store)
                              (catch :default _ nil)))]

    (log/debug! {:id ::register-store-hooks-check
                 :msg "Checking write-hooks support"
                 :data {:topic topic
                        :hooks-supported? (some? hooks-atom)
                        :existing-hook-count (when hooks-atom (count @hooks-atom))}})

    ;; Register topic with pubsub
    (pubsub/register-topic! peer topic
                            {:strategy strategy
                             :batch-size (:batch-size opts 20)
                             :item-timeout-ms item-timeout-ms})

    ;; Set up write hook for auto-publishing
    ;; Pass key-sort-fn to ensure multi-assoc keys are published in correct order
    (k/add-write-hook! store hook-id (make-write-hook peer topic filter-fn key-sort-fn))

    (log/debug! {:id ::register-store-hook-added
                 :msg "Write hook added"
                 :data {:topic topic
                        :hook-id hook-id
                        :hook-count-after (when hooks-atom (count @hooks-atom))}})

    ;; Store hook-id for later removal
    (swap! peer assoc-in [:pubsub :topics topic :write-hook-id] hook-id)
    (swap! peer assoc-in [:pubsub :topics topic :store] store)

    topic))

(defn unregister-store!
  "Unregister a store from pubsub (server-side).

   Removes write-hooks and unregisters the topic."
  [peer topic]
  (log/info! {:id ::unregister-store
              :msg "Unregistering store from pubsub"
              :data {:topic topic}})
  (when-let [topic-data (get-in @peer [:pubsub :topics topic])]
    ;; Remove write hook
    (when-let [hook-id (:write-hook-id topic-data)]
      (when-let [store (:store topic-data)]
        (k/remove-write-hook! store hook-id)))
    ;; Unregister topic
    (pubsub/unregister-topic! peer topic)))
