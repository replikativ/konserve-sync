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
            [konserve-sync.log :as log])
  #?(:clj (:import [java.io ByteArrayOutputStream InputStream])))

;; =============================================================================
;; Store Sync Strategy
;; =============================================================================

(defrecord StoreSyncStrategy
           [store        ; The konserve store (local on client, source on server)
            opts         ; {:filter-fn, :walk-fn, :key-sort-fn, :on-key-update}
            role])       ; :server or :client

(def ^:private default-max-binary-bytes (* 4 1024 1024))

(defn- binary-too-large [size limit]
  (ex-info "Binary value exceeds the configured synchronization limit"
           {:type :konserve-sync/binary-too-large
            :size size
            :limit limit}))

#?(:clj
   (defn- checked-bytes [value limit]
     (when (> (alength ^bytes value) limit)
       (throw (binary-too-large (alength ^bytes value) limit)))
     value))

#?(:clj
   (defn- stream-bytes [^InputStream input limit]
     (let [output (ByteArrayOutputStream.)
           buffer (byte-array 8192)]
       (loop [total 0]
         (let [n (.read input buffer)]
           (if (neg? n)
             (.toByteArray output)
             (let [total (+ total n)]
               (when (> total limit)
                 (throw (binary-too-large total limit)))
               (.write output buffer 0 n)
               (recur total))))))))

#?(:clj
   (defn- binary-bytes [{:keys [input-stream blob] :as binary} limit]
     (cond
       (bytes? binary) (checked-bytes binary limit)
       (bytes? input-stream) (checked-bytes input-stream limit)
       (instance? InputStream input-stream)
       (stream-bytes input-stream limit)
       (bytes? blob) (checked-bytes blob limit)
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
   (defn- binary-channel [{:keys [input-stream blob] :as binary} limit]
     (let [out (chan 1)
           ^js value (or input-stream blob binary)]
       (cond
         (instance? js/Uint8Array value)
         (put! out (if (> (.-byteLength value) limit)
                     (binary-too-large (.-byteLength value) limit)
                     value))

         (and value (fn? (.-arrayBuffer value)))
         (if (and (number? (.-size value)) (> (.-size value) limit))
           (put! out (binary-too-large (.-size value) limit))
           (-> (.arrayBuffer value)
               (.then (fn [buffer]
                        (let [bytes (js/Uint8Array. buffer)]
                          (put! out
                                (if (> (.-byteLength bytes) limit)
                                  (binary-too-large (.-byteLength bytes) limit)
                                  bytes)))))
               (.catch #(put! out %))))

         (and value (fn? (.-on value)))
         (let [chunks (atom [])
               total (atom 0)
               failed? (atom false)]
           (.on value "data"
                (fn [chunk]
                  (when-not @failed?
                    (let [n (+ @total (.-length chunk))]
                      (if (> n limit)
                        (do
                          (reset! failed? true)
                          (put! out (binary-too-large n limit))
                          (when (fn? (.-destroy value))
                            (.destroy value)))
                        (do
                          (reset! total n)
                          (swap! chunks conj chunk)))))))
           (.once value "end"
                  #(when-not @failed?
                     (put! out (concat-binary-chunks @chunks))))
           (.once value "error"
                  (fn [error]
                    (when-not @failed?
                      (reset! failed? true)
                      (put! out error)))))

         :else
         (put! out (ex-info "Unsupported CLJS binary representation"
                            {:value-type (type value)})))
       out)))

(defn- read-binary
  "Materialize one Konserve binary object while its bget callback is valid.
  Geschichte keeps these objects bounded (4 MiB by default); transport-level
  framing for arbitrary monolithic values is a separate protocol extension."
  [store key limit]
  (k/bget store key
          (fn [binary]
            #?(:clj (go (binary-bytes binary limit))
               :cljs (binary-channel binary limit)))
          {:sync? false :streaming? true :raw? true}))

(defn- wire-binary [value limit]
  #?(:clj (checked-bytes value limit)
     :cljs (if (> (.-byteLength value) limit)
             (throw (binary-too-large (.-byteLength value) limit))
             value)))

(defn- encode-wire-binary [value encoding limit]
  (let [value (wire-binary value limit)]
    (case encoding
      :bytes value
      :base64 (base64/encode value)
      (throw (ex-info "Unsupported binary wire encoding"
                      {:type :konserve-sync/unsupported-binary-encoding
                       :encoding encoding})))))

(defn- decode-wire-binary [value encoding limit]
  (let [value (case encoding
                :bytes value
                :base64 #?(:clj (base64/decode value)
                           :cljs (js/Uint8Array. (base64/decode value)))
                (throw (ex-info "Unsupported binary wire encoding"
                                {:type :konserve-sync/unsupported-binary-encoding
                                 :encoding encoding})))]
    (wire-binary value limit)))

(defn- get-local-key-timestamps
  "Get {key -> last-write} map from a konserve store.
   When `:walk-fn` is supplied, inspect only keys reachable through that walk
   instead of enumerating the entire local store. This matters for durable
   client caches, which can retain large amounts of unreachable history.
   Returns a channel yielding the map."
  [store {:keys [walk-fn]}]
  (go
    (let [key-metas (if walk-fn
                      (let [walked-keys (<! (walk-fn store {:sync? false}))]
                        (loop [remaining (seq walked-keys)
                               result []]
                          (if-not remaining
                            result
                            (let [key (first remaining)
                                  meta (<! (k/get-meta store key))]
                              (recur (next remaining)
                                     (if meta
                                       (conj result (assoc meta :key key))
                                       result))))))
                      (<! (k/keys store)))]
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
  [store client-timestamps
   {:keys [filter-fn walk-fn key-sort-fn always-send-mutable? max-binary-bytes
           binary-wire-format]
    :or {filter-fn (constantly true)
         max-binary-bytes default-max-binary-bytes
         binary-wire-format :base64}}]
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
                v (<! (if binary?
                        (read-binary store k max-binary-bytes)
                        (k/get store k)))
                v (if binary?
                    (encode-wire-binary v binary-wire-format max-binary-bytes)
                    v)]
            (recur (next remaining)
                   (conj result (cond-> {:key k :value v}
                                  binary? (assoc :binary? true
                                                 :binary-encoding binary-wire-format)
                                  (immutable-keys k) (assoc :meta {:immutable? true}))))))))))

(extend-type StoreSyncStrategy
  proto/PSyncStrategy

  (-init-client-state [this]
    ;; Client sends {key -> timestamp} for differential sync
    (if (= :client (:role this))
      (do
        (log/debug! {:id ::init-client-state
                     :msg "Initializing client state for differential sync"})
        (get-local-key-timestamps (:store this) (:opts this)))
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

  (-apply-handshake-item
    [this {:keys [key value meta binary? binary-encoding]}]
    ;; Client applies handshake item to local store
    (let [ch (chan 1)]
      (if (= :client (:role this))
        (go
          (try
            (if (and (:immutable? meta) (<! (k/exists? (:store this) key)))
              ;; immutable value already held (reconnect / overlap) — skip the
              ;; re-store so its write-hook doesn't re-publish (echo).
              (log/trace! {:id ::apply-handshake-skip-immutable :data {:key key}})
              (let [stored-value (if binary?
                                   (decode-wire-binary
                                    value
                                    (or binary-encoding :base64)
                                    (get-in this [:opts :max-binary-bytes]
                                            default-max-binary-bytes))
                                   value)]
                (log/trace! {:id ::apply-handshake-item
                             :msg "Applying handshake item"
                             :data {:key key}})
                (<! (if binary?
                      (k/bassoc (:store this) key stored-value {:raw? true})
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

  (-apply-publish
    [this {:keys [key value operation meta binary? binary-encoding] :as payload}]
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
            (let [stored-value (if binary?
                                 (decode-wire-binary
                                  value
                                  (or binary-encoding :base64)
                                  (get-in this [:opts :max-binary-bytes]
                                          default-max-binary-bytes))
                                 value)]
              (case operation
                :dissoc
                (<! (k/dissoc (:store this) key))

                ;; Default: assoc/bassoc
                (<! (if binary?
                      (k/bassoc (:store this) key stored-value {:raw? true})
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
     - :walk-fn (fn [store opts] -> channel) - Limit the local timestamp
       inventory to reachable keys. Without it, every local key is enumerated.
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

(defn- publish-one!
  [peer topic payload]
  (go
    (let [result (<! (pubsub/publish! peer topic payload))]
      (if-let [error (:error result)]
        (throw error)
        result))))

(defn- publish-event!
  "Publish one completed store write. This function may materialize a consumed
  binary input, so callers must serialize invocations to preserve commit order."
  [peer topic store filter-fn key-sort-fn max-binary-bytes binary-wire-format event]
  (go
    (let [{:keys [api-op key value kvs]} event
          subscribers (pubsub/get-subscribers peer topic)]
      (log/debug! {:id ::write-hook-event
                   :msg "Publishing completed store write"
                   :data {:api-op api-op
                          :key key
                          :topic topic
                          :subscriber-count (count subscribers)}})
      (case api-op
        (:assoc :assoc-in :update :update-in)
        (when (filter-fn key value)
          (<! (publish-one!
               peer topic
               (cond-> {:key key :value value :operation :assoc}
                 (:meta event) (assoc :meta (:meta event))))))

        :bassoc
        (when (filter-fn key value)
          (let [stored
                #?(:clj (if (bytes? value)
                          (wire-binary value max-binary-bytes)
                          (<! (read-binary store key max-binary-bytes)))
                   :cljs (if (instance? js/Uint8Array value)
                           (wire-binary value max-binary-bytes)
                           (<! (read-binary store key max-binary-bytes))))]
            (<! (publish-one! peer topic
                              {:key key
                               :value (encode-wire-binary stored binary-wire-format
                                                          max-binary-bytes)
                               :operation :assoc
                               :binary? true
                               :binary-encoding binary-wire-format}))))

        :dissoc
        (when (filter-fn key nil)
          (<! (publish-one! peer topic {:key key :operation :dissoc})))

        :multi-assoc
        (let [ordered-kvs (if (map? kvs)
                            (cond->> kvs
                              key-sort-fn (sort-by (fn [[k _]] (key-sort-fn k))))
                            kvs)
              metadata (:meta event)]
          (log/debug! {:id ::write-hook-multi-assoc
                       :msg "Publishing multi-assoc"
                       :data {:key-count (count ordered-kvs)
                              :ordered? (not (map? kvs))
                              :topic topic
                              :subscribers (count subscribers)
                              :keys (mapv first ordered-kvs)}})
          (loop [remaining (seq ordered-kvs)]
            (when-let [[k v] (first remaining)]
              (when (filter-fn k v)
                (let [km (get metadata k)]
                  (<! (publish-one!
                       peer topic
                       (cond-> {:key k :value v :operation :assoc}
                         km (assoc :meta km))))))
              (recur (next remaining)))))

        (log/warn! {:id ::write-hook-unknown-op
                    :msg "Unknown api-op in write hook"
                    :data {:api-op api-op}}))
      {:ok true})))

(defn- start-publisher!
  [peer topic store filter-fn key-sort-fn max-binary-bytes binary-wire-format
   publisher-buffer]
  (let [events (chan publisher-buffer)]
    (go-loop []
      (when-let [event (<! events)]
        (try
          (let [result (<! (publish-event! peer topic store filter-fn key-sort-fn
                                           max-binary-bytes binary-wire-format event))]
            (when (instance? #?(:clj Throwable :cljs js/Error) result)
              (throw result)))
          (catch #?(:clj Throwable :cljs :default) error
            (log/error! {:id ::write-hook-publish-error
                         :msg "Failed to publish completed store write"
                         :data {:topic topic
                                :api-op (:api-op event)
                                :key (:key event)
                                :error error}})))
        (recur)))
    events))

(defn- make-write-hook
  "Enqueue completed writes without retaining unbounded pending puts.

  A synchronous Konserve write hook cannot park until the network catches up.
  If its bounded lane fills, retire every direct subscriber so reconnect runs
  the differential snapshot again; silently dropping one live write would
  leave those replicas permanently incomplete."
  [peer topic events on-publisher-overflow]
  (fn [event]
    (when (:api-op event)
      (when-not (async/offer! events event)
        (swap! peer update-in [:pubsub :topics topic :publisher-overflows]
               (fnil inc 0))
        (let [subscribers (pubsub/get-subscribers peer topic)]
          (log/error! {:id ::write-hook-publisher-overflow
                       :msg "Ordered publisher full; retiring subscribers for snapshot recovery"
                       :data {:topic topic
                              :api-op (:api-op event)
                              :key (:key event)
                              :subscriber-count (count subscribers)}})
          (doseq [transport subscribers]
            (close! transport))
          (when on-publisher-overflow
            (try
              (on-publisher-overflow {:peer peer :topic topic :event event})
              (catch #?(:clj Throwable :cljs :default) error
                (log/error! {:id ::publisher-overflow-callback-error
                             :msg "Publisher overflow callback failed"
                             :data {:topic topic :error error}})))))))))

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
     - :max-binary-bytes - Largest binary value materialized into one pubsub
       message (default 4 MiB). Larger values require a future bulk/chunked
       transfer profile.
     - :binary-wire-format - :base64 (legacy EDN carrier compatibility,
       default) or :bytes (native CBOR byte strings; the standards profile).
     - :publisher-buffer - completed writes retained by the ordered publisher
       (default 256). Overflow is bounded and retires direct subscribers so
       their next subscription performs a differential snapshot.
     - :on-publisher-overflow - callback receiving `{:peer :topic :event}`.
       Overlay transports, which do not own direct subscriber channels, use it
       to force a new application state-sync attempt.

   Returns the topic."
  [peer topic store opts]
  (log/info! {:id ::register-store
              :msg "Registering store for pubsub"
              :data {:topic topic
                     :store-type (type store)}})
  (let [filter-fn (or (:filter-fn opts) (constantly true))
        key-sort-fn (:key-sort-fn opts)
        max-binary-bytes (:max-binary-bytes opts default-max-binary-bytes)
        binary-wire-format (:binary-wire-format opts :base64)
        publisher-buffer (:publisher-buffer opts 256)
        on-publisher-overflow (:on-publisher-overflow opts)
        _ (when-not (and (int? publisher-buffer) (pos? publisher-buffer))
            (throw (ex-info "Publisher buffer must be a positive integer"
                            {:type :konserve-sync/invalid-publisher-buffer
                             :publisher-buffer publisher-buffer})))
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
                              (catch :default _ nil)))
        publisher-events (start-publisher! peer topic store filter-fn key-sort-fn
                                           max-binary-bytes binary-wire-format
                                           publisher-buffer)]

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

    (k/add-write-hook! store hook-id
                       (make-write-hook peer topic publisher-events
                                        on-publisher-overflow))

    (log/debug! {:id ::register-store-hook-added
                 :msg "Write hook added"
                 :data {:topic topic
                        :hook-id hook-id
                        :hook-count-after (when hooks-atom (count @hooks-atom))}})

    ;; Store hook-id for later removal
    (swap! peer assoc-in [:pubsub :topics topic :write-hook-id] hook-id)
    (swap! peer assoc-in [:pubsub :topics topic :store] store)
    (swap! peer assoc-in [:pubsub :topics topic :publisher-events] publisher-events)

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
    (when-let [events (:publisher-events topic-data)]
      (close! events))
    ;; Unregister topic
    (pubsub/unregister-topic! peer topic)))
