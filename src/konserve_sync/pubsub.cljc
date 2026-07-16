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
            [hasch.base64 :as base64]
            [kabel.pubsub :as pubsub]
            [kabel.pubsub.protocol :as proto]
            [konserve-sync.log :as log]
            #?(:clj [clojure.java.io :as io]))
  #?(:clj (:import [java.io ByteArrayOutputStream InputStream])))

;; =============================================================================
;; Store Sync Strategy
;; =============================================================================

(defrecord StoreSyncStrategy
           [store        ; The konserve store (local on client, source on server)
            opts         ; {:filter-fn, :walk-fn, :key-sort-fn, :on-key-update}
            role])       ; :server or :client

#?(:clj
   (defn- binary-bytes [{:keys [input-stream blob] :as binary}]
     (cond
       (bytes? binary) binary
       (bytes? input-stream) input-stream
       (instance? InputStream input-stream)
       (let [out (ByteArrayOutputStream.)]
         (io/copy input-stream out)
         (.toByteArray out))
       (bytes? blob) blob
       :else (throw (ex-info "Unsupported JVM binary representation"
                             {:value-type (type binary)})))))

#?(:cljs
   (defn- concat-binary-chunks [chunks]
     (let [size (reduce + (map #(.-length %) chunks))
           out (js/Uint8Array. size)]
       (loop [offset 0 remaining (seq chunks)]
         (if-let [chunk (first remaining)]
           (do (.set out chunk offset)
               (recur (+ offset (.-length chunk)) (next remaining)))
           out)))))

#?(:cljs
   (defn- binary-channel [{:keys [input-stream blob] :as binary}]
     (let [out (chan 1)
           ^js value (or input-stream blob binary)]
       (cond
         (instance? js/Uint8Array value)
         (put! out value)

         (and value (fn? (.-arrayBuffer value)))
         (-> (.arrayBuffer value)
             (.then #(put! out (js/Uint8Array. %)))
             (.catch #(put! out %)))

         (and value (fn? (.-on value)))
         (let [chunks (atom [])]
           (.on value "data" #(swap! chunks conj %))
           (.once value "end" #(put! out (concat-binary-chunks @chunks)))
           (.once value "error" #(put! out %)))

         :else
         (put! out (ex-info "Unsupported CLJS binary representation"
                            {:value-type (type value)})))
       out)))

(defn- read-binary
  "Materialize one Konserve binary object while its bget callback is valid.
  Geschichte keeps these objects bounded (4 MiB by default); transport-level
  framing for arbitrary monolithic values is a separate protocol extension."
  [store key]
  (k/bget store key
          (fn [binary]
            #?(:clj (go (binary-bytes binary))
               :cljs (binary-channel binary)))
          {:sync? false :streaming? true}))

(defn- encode-binary [value]
  (base64/encode value))

(defn- decode-binary [value]
  #?(:clj (base64/decode value)
     :cljs (js/Uint8Array. (base64/decode value))))

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

   `:always-send-mutable?` (default false) — send every walked key that is NOT marked
   `:immutable?` in its stored metadata, regardless of timestamps.

   Why: the timestamp filter asks \"does the subscriber hold the CURRENT version?\" but
   `:last-write` only records WHEN each side wrote its own copy — on its own wall clock.
   Those are different questions, and comparing them across two machines is not sound.

   For an IMMUTABLE (content-addressed, write-once) value it does not matter: the key
   determines the value, so mere PRESENCE settles it (`nil? client-timestamp` ⇒ send),
   and the timestamp comparison is redundant.

   For a MUTABLE cell — same key, new value on every write — it is the wrong tool
   entirely. A subscriber that rewrites its local copy stamps its own `now`, which is
   LATER than the writer's commit, so the server concludes \"already current\" and skips
   the cell. It also skips it whenever the subscriber's clock merely runs ahead, leaving
   the subscriber on a stale value indefinitely.

   With this flag, mutable cells are always re-sent. Combined with a `:walk-fn` that
   emits them LAST, a subscriber is guaranteed to receive the pointer AFTER every value
   it references — on every handshake, not just when a clock comparison happens to say
   so. The cost is bounded: one small value per mutable cell per handshake, while the
   bulk (content-addressed nodes) still dedups."
  [store client-timestamps {:keys [filter-fn walk-fn key-sort-fn always-send-mutable?]
                            :or {filter-fn (constantly true)}}]
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
                                                         :type (:type meta)
                                                         :immutable? (:immutable? meta)})
                                           result))))))
                          ;; Default: get all keys via k/keys
                          (<! (k/keys store)))

          ;; Filter to keys that need syncing
          keys-to-send (filter
                        (fn [{:keys [key last-write immutable?]}]
                          (let [client-timestamp (get client-timestamps key)]
                            (and (filter-fn key nil)
                                 (or ;; subscriber does not have it at all
                                  (nil? client-timestamp)
                                     ;; MUTABLE cell — a timestamp cannot tell us whether the
                                     ;; subscriber's copy is the current VERSION, so never
                                     ;; dedup it away. See the docstring.
                                  (and always-send-mutable? (not immutable?))
                                     ;; otherwise fall back to the timestamp comparison
                                  (pos? (compare last-write client-timestamp))))))
                        all-key-metas)

          ;; Sort if key-sort-fn provided
          sorted-keys (cond->> (map :key keys-to-send)
                        key-sort-fn (sort-by key-sort-fn))
          ;; keys whose stored metadata marks them immutable (content-addressed,
          ;; write-once) — the handshake item carries this so a reconnecting peer
          ;; that already holds the value skips re-storing (and re-publishing) it.
          immutable-keys (into #{} (comp (filter :immutable?) (map :key)) keys-to-send)
          binary-keys (into #{} (comp (filter #(= :binary (:type %))) (map :key))
                            keys-to-send)]

      (log/debug! {:id ::keys-to-sync
                   :msg "Computed keys to sync"
                   :data {:count (count sorted-keys)}})

      ;; Fetch values for each key
      (loop [remaining (seq sorted-keys)
             result []]
        (if-not remaining
          result
          (let [k (first remaining)
                binary? (contains? binary-keys k)
                v (<! (if binary? (read-binary store k) (k/get store k)))
                v (if binary? (encode-binary v) v)]
            (recur (next remaining)
                   (conj result (cond-> {:key k :value v}
                                  binary? (assoc :binary? true)
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

  (-apply-handshake-item [this {:keys [key value meta binary?]}]
    ;; Client applies handshake item to local store
    (let [ch (chan 1)]
      (if (= :client (:role this))
        (go
          (try
            (if (and (:immutable? meta) (<! (k/exists? (:store this) key)))
              ;; immutable value already held (reconnect / overlap) — skip the
              ;; re-store so its write-hook doesn't re-publish (echo).
              (log/trace! {:id ::apply-handshake-skip-immutable :data {:key key}})
              (let [stored-value (if binary? (decode-binary value) value)]
                (log/trace! {:id ::apply-handshake-item
                             :msg "Applying handshake item"
                             :data {:key key}})
                (<! (if binary?
                      (k/bassoc (:store this) key stored-value)
                      (k/assoc (:store this) key stored-value)))
                ;; Invoke callback if provided
                (when-let [on-key-update (get-in this [:opts :on-key-update])]
                  (on-key-update key stored-value :handshake))))
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

  (-apply-publish [this {:keys [key value operation meta binary?] :as payload}]
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
            (let [stored-value (if binary? (decode-binary value) value)]
              (case operation
                :dissoc
                (<! (k/dissoc (:store this) key))

                ;; Default: assoc/bassoc
                (<! (if binary?
                      (k/bassoc (:store this) key stored-value)
                      (k/assoc (:store this) key stored-value))))

              ;; Invoke callback if provided
              (when-let [on-key-update (get-in this [:opts :on-key-update])]
                (on-key-update key stored-value (or operation :assoc)))))

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
     - :always-send-mutable? (bool, default false) - Re-send every walked key NOT
       marked :immutable? in its stored metadata, regardless of timestamps. The
       timestamp filter asks whether you hold the current VERSION, but :last-write only
       says WHEN each side wrote its own copy, on its own wall clock. That settles
       nothing for a MUTABLE cell (same key, new value each write): a subscriber that
       rewrote its copy stamps a LATER time than the writer's commit, so the server
       skips the cell — and it also skips it whenever the subscriber's clock merely
       runs ahead, stranding it on a stale value. Immutable (content-addressed) values
       are unaffected: the key determines the value, so presence settles it. Pair with
       a :walk-fn that emits mutable cells LAST, and a subscriber always receives the
       pointer after every value it references.
     - :key-sort-fn (fn [key] -> comparable) - LEGACY escape hatch: impose a sync
       order on a source that carries none, so a mutable pointer lands after the
       values it references (sort it last). It is a heuristic on the SHAPE of the
       key, and silently wrong for any store whose keys don't fit the guess —
       prefer carrying real order, and leave this nil:
         * HANDSHAKE — have :walk-fn return an ORDERED seq with the mutable
           pointer cells last (walk order is preserved). konserve-sync's datahike
           walker does exactly that.
         * ONGOING publishes — hand konserve's multi-assoc an ORDERED batch (a seq
           of [k v] pairs); it is relayed verbatim, pointer last. Only a MAP batch,
           which has no order to carry, still falls back to this."
  [store opts]
  (->StoreSyncStrategy store opts :server))

;; =============================================================================
;; Convenience: Write Hook Integration
;; =============================================================================

(defn- make-write-hook
  "Create a write-hook that publishes changes to pubsub."
  [peer topic store filter-fn key-sort-fn]
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
          (:assoc :assoc-in :update :update-in)
          (when (filter-fn key value)
            (log/debug! {:id ::write-hook-publish
                         :msg "Publishing single key"
                         :data {:key key :topic topic :subscribers (count subscribers)}})
            ;; forward the value's metadata (:immutable? marks content-addressed,
            ;; write-once values) so a receiver can skip re-storing one it already
            ;; has — terminating the bidirectional write-hook echo.
            (pubsub/publish! peer topic (cond-> {:key key :value value :operation :assoc}
                                          (:meta event) (assoc :meta (:meta event)))))

          :bassoc
          (when (filter-fn key value)
            ;; Byte buffers (Geschichte chunks) remain valid after bassoc and can
            ;; be published immediately, preserving their write-before-head order.
            ;; Stateful inputs have been consumed by the store, so refetch them.
            #?(:clj
               (if (bytes? value)
                 (pubsub/publish! peer topic
                                  {:key key :value (encode-binary value)
                                   :operation :assoc :binary? true})
                 (go (let [stored (<! (read-binary store key))]
                       (pubsub/publish! peer topic
                                        {:key key :value (encode-binary stored) :operation :assoc
                                         :binary? true}))))
               :cljs
               (if (instance? js/Uint8Array value)
                 (pubsub/publish! peer topic
                                  {:key key :value (encode-binary value)
                                   :operation :assoc :binary? true})
                 (go (let [stored (<! (read-binary store key))]
                       (pubsub/publish! peer topic
                                        {:key key :value (encode-binary stored) :operation :assoc
                                         :binary? true}))))))

          ;; Delete
          :dissoc
          (when (filter-fn key nil)
            (log/debug! {:id ::write-hook-publish
                         :msg "Publishing dissoc"
                         :data {:key key :topic topic :subscribers (count subscribers)}})
            (pubsub/publish! peer topic {:key key :operation :dissoc}))

          ;; Multi-key batch. An ORDERED batch (a seq of [k v] pairs) already CARRIES its
          ;; apply order: konserve's multi-assoc contract makes sequence order the apply
          ;; order, and a writer puts the mutable pointer LAST (write-the-leaves-then-
          ;; flip-the-root). Relay it VERBATIM, so a subscriber applies the batch in the
          ;; order the writer committed it and the pointer lands only after everything it
          ;; references. That is a causal guarantee carried from the source.
          ;;
          ;; A MAP batch has no order, so there is nothing to carry: key-sort-fn imposes
          ;; one after the fact, by guessing from the shape of the key (e.g. "keywords are
          ;; roots, sort them last"). That is a heuristic — it is silently wrong for any
          ;; store whose keys don't fit the guess — and it is kept only for map batches and
          ;; legacy callers. Prefer an ordered batch.
          :multi-assoc
          (let [ordered-kvs (if (map? kvs)
                              (cond->> kvs
                                key-sort-fn (sort-by (fn [[k _]] (key-sort-fn k))))
                              kvs)]
            (log/debug! {:id ::write-hook-multi-assoc
                         :msg "Publishing multi-assoc"
                         :data {:key-count (count ordered-kvs)
                                :ordered? (not (map? kvs))
                                :topic topic
                                :subscribers (count subscribers)
                                :keys (mapv first ordered-kvs)}})
            ;; per-key meta is a pure-data map {key -> meta} (e.g. mark nodes immutable but
            ;; the batch's mutable branch-head pointer not); look it up per key.
            (let [m (:meta event)]
              (doseq [[k v] ordered-kvs]
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
     - :always-send-mutable? (bool, default false) - Re-send every walked key NOT
       marked :immutable? in its stored metadata, regardless of timestamps. The
       timestamp filter asks whether you hold the current VERSION, but :last-write only
       says WHEN each side wrote its own copy, on its own wall clock. That settles
       nothing for a MUTABLE cell (same key, new value each write): a subscriber that
       rewrote its copy stamps a LATER time than the writer's commit, so the server
       skips the cell — and it also skips it whenever the subscriber's clock merely
       runs ahead, stranding it on a stale value. Immutable (content-addressed) values
       are unaffected: the key determines the value, so presence settles it. Pair with
       a :walk-fn that emits mutable cells LAST, and a subscriber always receives the
       pointer after every value it references.
     - :key-sort-fn (fn [key] -> comparable) - LEGACY escape hatch: impose a sync
       order on a source that carries none, so a mutable pointer lands after the
       values it references (sort it last). It is a heuristic on the SHAPE of the
       key, and silently wrong for any store whose keys don't fit the guess —
       prefer carrying real order, and leave this nil:
         * HANDSHAKE — have :walk-fn return an ORDERED seq with the mutable
           pointer cells last (walk order is preserved). konserve-sync's datahike
           walker does exactly that.
         * ONGOING publishes — hand konserve's multi-assoc an ORDERED batch (a seq
           of [k v] pairs); it is relayed verbatim, pointer last. Only a MAP batch,
           which has no order to carry, still falls back to this.
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
    (k/add-write-hook! store hook-id
                       (make-write-hook peer topic store filter-fn key-sort-fn))

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
