(ns konserve-sync.pubsub-test
  "Integration tests for konserve-sync using kabel.pubsub."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [konserve-sync.pubsub :as ks-pubsub]
            [konserve-sync.transport.kabel-pubsub :as kp]
            [kabel.pubsub :as pubsub]
            [kabel.pubsub.protocol :as proto]
            [kabel.peer :as peer]
            [kabel.http-kit :as http-kit]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [superv.async :refer [<?? S]]
            [clojure.core.async :refer [<!! >! go chan put! close! timeout]]))

;; =============================================================================
;; Test Infrastructure
;; =============================================================================

(def ^:dynamic *server-peer* nil)
(def ^:dynamic *client-peer* nil)
(def ^:dynamic *server-store* nil)
(def ^:dynamic *client-store* nil)
(def ^:dynamic *server-url* nil)

(defn unique-port []
  (+ 47500 (rand-int 100)))

(defn- stored-bytes [store key]
  (<?? S (k/bget store key
                 (fn [{:keys [input-stream]}]
                   (go input-stream)))))

(defmacro with-store-peers
  "Execute body with server and client peers set up with konserve stores."
  [& body]
  `(let [port# (unique-port)
         url# (str "ws://localhost:" port#)
         sid# (java.util.UUID/randomUUID)
         cid# (java.util.UUID/randomUUID)]
     (binding [*server-url* url#
               *server-store* (<?? S (new-mem-store))
               *client-store* (<?? S (new-mem-store))]
       (let [handler# (http-kit/create-http-kit-handler! S url# sid#)]
         ;; Server peer with pubsub middleware
         (binding [*server-peer* (peer/server-peer S handler# sid#
                                                   (kp/server-middleware)
                                                   identity)]
           ;; Start server
           (<?? S (peer/start *server-peer*))
           (try
             ;; Client peer with pubsub middleware
             (binding [*client-peer* (peer/client-peer S cid#
                                                       (kp/client-middleware)
                                                       identity)]
               ;; Connect client to server
               (<?? S (peer/connect S *client-peer* url#))
               ;; Wait for connection
               (<?? S (timeout 200))
               (try
                 ~@body
                 (finally
                   nil)))
             (finally
               (<?? S (peer/stop *server-peer*)))))))))

;; =============================================================================
;; Unit Tests for StoreSyncStrategy
;; =============================================================================

(deftest store-sync-strategy-protocol-test
  (testing "Client strategy init-client-state returns key timestamps"
    (let [store (<?? S (new-mem-store))]
      ;; Add some data
      (<?? S (k/assoc store :foo "bar"))
      (<?? S (k/assoc store :baz 123))

      (let [strategy (ks-pubsub/store-sync-strategy store {})
            client-state (<!! (proto/-init-client-state strategy))]
        (is (map? client-state))
        (is (contains? client-state :foo))
        (is (contains? client-state :baz))
        (is (inst? (get client-state :foo))))))

  (testing "Server strategy handshake-items yields keys client doesn't have"
    (let [store (<?? S (new-mem-store))]
      ;; Add some data to server store
      (<?? S (k/assoc store :a 1))
      (<?? S (k/assoc store :b 2))
      (<?? S (k/assoc store :c 3))

      (let [strategy (ks-pubsub/server-store-strategy store {})
            ;; Client has :a with old timestamp, doesn't have :b, :c
            client-state {:a #inst "2020-01-01T00:00:00.000-00:00"}
            items-ch (proto/-handshake-items strategy client-state)
            items (atom [])]
        ;; Collect all items
        (loop []
          (when-let [item (<!! items-ch)]
            (swap! items conj item)
            (recur)))

        ;; Should have all 3 items (:a is stale, :b and :c are new)
        (is (= 3 (count @items)))
        (is (= #{:a :b :c} (set (map :key @items))))
        ;; Verify values
        (is (= 1 (:value (first (filter #(= :a (:key %)) @items)))))
        (is (= 2 (:value (first (filter #(= :b (:key %)) @items)))))
        (is (= 3 (:value (first (filter #(= :c (:key %)) @items))))))))

  (testing "Client strategy apply-handshake-item writes to store"
    (let [store (<?? S (new-mem-store))
          strategy (ks-pubsub/store-sync-strategy store {})]

      ;; Apply handshake item
      (let [result (<!! (proto/-apply-handshake-item strategy {:key :test :value "value"}))]
        (is (:ok result)))

      ;; Verify written
      (is (= "value" (<?? S (k/get store :test))))))

  (testing "Client strategy apply-publish writes to store"
    (let [store (<?? S (new-mem-store))
          strategy (ks-pubsub/store-sync-strategy store {})]

      ;; Apply publish (assoc)
      (let [result (<!! (proto/-apply-publish strategy {:key :pub-key :value 42 :operation :assoc}))]
        (is (:ok result)))

      (is (= 42 (<?? S (k/get store :pub-key))))

      ;; Apply publish (dissoc)
      (let [result (<!! (proto/-apply-publish strategy {:key :pub-key :operation :dissoc}))]
        (is (:ok result)))

      (is (nil? (<?? S (k/get store :pub-key))))))

  (testing "Strategy filter-fn filters keys during handshake"
    (let [store (<?? S (new-mem-store))]
      ;; Add some data
      (<?? S (k/assoc store :public-1 "pub"))
      (<?? S (k/assoc store :private-1 "priv"))
      (<?? S (k/assoc store :public-2 "pub2"))

      (let [strategy (ks-pubsub/server-store-strategy store
                                                      {:filter-fn (fn [k _]
                                                                    (clojure.string/starts-with?
                                                                     (name k) "public"))})
            items-ch (proto/-handshake-items strategy {})
            items (atom [])]
        (loop []
          (when-let [item (<!! items-ch)]
            (swap! items conj item)
            (recur)))

        ;; Should only have public keys
        (is (= 2 (count @items)))
        (is (= #{:public-1 :public-2} (set (map :key @items))))))))

;; =============================================================================
;; Integration Tests
;; =============================================================================

(deftest basic-store-sync-integration-test
  (testing "Basic store sync via pubsub"
    (with-store-peers
      ;; Populate server store
      (<?? S (k/assoc *server-store* :key1 "value1"))
      (<?? S (k/assoc *server-store* :key2 {:nested "data"}))
      (<?? S (k/assoc *server-store* :key3 [1 2 3]))

      ;; Register store as pubsub topic on server
      (kp/register-store! *server-peer* :test-store *server-store* {})

      ;; Subscribe from client
      (<?? S (kp/subscribe-store! *client-peer* :test-store *client-store* {}))

      ;; Wait for sync
      (<?? S (timeout 1000))

      ;; Verify client has all data
      (is (= "value1" (<?? S (k/get *client-store* :key1))))
      (is (= {:nested "data"} (<?? S (k/get *client-store* :key2))))
      (is (= [1 2 3] (<?? S (k/get *client-store* :key3)))))))

(deftest binary-store-sync-integration-test
  (testing "binary values use bassoc during handshake and incremental sync"
    (with-store-peers
      (let [initial (byte-array (map unchecked-byte (range 64)))
            incremental (byte-array (map unchecked-byte (range 127 -1 -1)))]
        (<?? S (k/bassoc *server-store* :initial-blob initial))
        (kp/register-store! *server-peer* :binary-store *server-store* {})
        (<?? S (kp/subscribe-store! *client-peer* :binary-store *client-store* {}))
        (<?? S (timeout 800))

        (is (= (seq initial) (seq (stored-bytes *client-store* :initial-blob))))
        (is (= :binary (:type (<?? S (k/get-meta *client-store* :initial-blob)))))

        (<?? S (k/bassoc *server-store* :new-blob incremental))
        (<?? S (timeout 500))
        (is (= (seq incremental) (seq (stored-bytes *client-store* :new-blob))))
        (is (= :binary (:type (<?? S (k/get-meta *client-store* :new-blob)))))))))

(deftest incremental-store-sync-test
  (testing "Incremental updates via pubsub after initial sync"
    (with-store-peers
      ;; Initial server data
      (<?? S (k/assoc *server-store* :initial "data"))

      ;; Register store
      (kp/register-store! *server-peer* :inc-store *server-store* {})

      ;; Create client strategy with callback tracking
      (let [updates (atom [])]
        ;; Subscribe with callback
        (<?? S (kp/subscribe-store! *client-peer* :inc-store *client-store*
                                    {:on-key-update (fn [k v op]
                                                      (swap! updates conj {:key k :value v :op op}))}))
        (<?? S (timeout 800))

        ;; Verify initial sync
        (is (= "data" (<?? S (k/get *client-store* :initial))))

        ;; Make incremental changes on server (write-hook should auto-publish)
        (<?? S (k/assoc *server-store* :new-key "new-value"))
        (<?? S (timeout 300))

        ;; Verify incremental sync
        (is (= "new-value" (<?? S (k/get *client-store* :new-key))))

        ;; Update existing key
        (<?? S (k/assoc *server-store* :initial "updated"))
        (<?? S (timeout 300))

        (is (= "updated" (<?? S (k/get *client-store* :initial))))

        ;; Verify callbacks were invoked
        (is (pos? (count @updates)))))))

(deftest differential-sync-test
  (testing "Differential sync only sends stale/new keys"
    (with-store-peers
      ;; Add data to both stores - client has some existing data
      (<?? S (k/assoc *server-store* :same-key "same-value"))
      (<?? S (k/assoc *client-store* :same-key "same-value"))

      ;; Server has additional keys
      (<?? S (k/assoc *server-store* :server-only "exclusive"))

      ;; Give a moment for timestamps to be different
      (<?? S (timeout 100))

      ;; Update same-key on server (making it stale on client)
      (<?? S (k/assoc *server-store* :same-key "updated-value"))

      ;; Register store
      (kp/register-store! *server-peer* :diff-store *server-store* {})

      ;; Subscribe - should do differential sync
      (<?? S (kp/subscribe-store! *client-peer* :diff-store *client-store* {}))
      (<?? S (timeout 800))

      ;; Client should have updated value for same-key
      (is (= "updated-value" (<?? S (k/get *client-store* :same-key))))

      ;; Client should have server-only key
      (is (= "exclusive" (<?? S (k/get *client-store* :server-only)))))))

(deftest ordered-multi-assoc-relay-test
  (testing "an ordered multi-assoc batch is relayed in the writer's order — no key-sort-fn"
    (with-store-peers
      ;; NOTE: no :key-sort-fn. The order must come from the BATCH, carried from the
      ;; writer, not reconstructed by guessing at key shape.
      (kp/register-store! *server-peer* :ord-store *server-store* {})

      (let [applied (atom [])]
        (<?? S (kp/subscribe-store! *client-peer* :ord-store *client-store*
                                    {:on-key-update (fn [k _v _op] (swap! applied conj k))}))
        (<?? S (timeout 500))
        (reset! applied [])                       ;; ignore whatever the handshake applied

        ;; write-the-leaves-then-flip-the-root: the mutable pointer goes LAST
        (<?? S (k/multi-assoc *server-store*
                              [[:node-a 1] [:node-b 2] [:root {:refs [:node-a :node-b]}]]))
        (<?? S (timeout 800))

        (is (= [:node-a :node-b :root] @applied)
            "subscriber applied the batch in the writer's order, pointer last")
        ;; and the pointer's referents are present by the time it lands
        (is (= 1 (<?? S (k/get *client-store* :node-a))))
        (is (= 2 (<?? S (k/get *client-store* :node-b))))
        (is (= {:refs [:node-a :node-b]} (<?? S (k/get *client-store* :root))))))))

(deftest always-send-mutable-test
  (testing "mutable cells are re-sent even when the subscriber's copy looks newer"
    (with-store-peers
      ;; Server writes an IMMUTABLE node and a MUTABLE pointer at it.
      (<?? S (k/assoc *server-store* :node-1 {:v 1} {:immutable? true} {:sync? false}))
      (<?? S (k/assoc *server-store* :head {:points-at :node-1}))
      (<?? S (timeout 50))

      ;; The subscriber already holds both — and wrote its copies LATER than the server
      ;; wrote its own (exactly what happens when a peer pre-fills its cache from the
      ;; backing store before subscribing, or simply has a fast clock). The timestamp
      ;; filter therefore reads "already current" for BOTH keys.
      (<?? S (k/assoc *client-store* :node-1 {:v 1} {:immutable? true} {:sync? false}))
      (<?? S (k/assoc *client-store* :head {:points-at :node-1}))

      ;; …meanwhile the server has moved the pointer on.
      (<?? S (k/assoc *server-store* :head {:points-at :node-1 :moved true}))
      ;; (deliberately NOT waiting — the subscriber's local write may still be the later
      ;; wall-clock time, which is the whole point: time cannot answer "which version?")

      (kp/register-store! *server-peer* :mut-store *server-store*
                          {:always-send-mutable? true})

      (let [applied (atom [])]
        (<?? S (kp/subscribe-store! *client-peer* :mut-store *client-store*
                                    {:on-key-update (fn [k _v _op] (swap! applied conj k))}))
        (<?? S (timeout 800))

        (is (some #{:head} @applied)
            "the MUTABLE cell is re-sent regardless of the timestamp comparison")
        (is (not (some #{:node-1} @applied))
            "the IMMUTABLE value the subscriber already holds is still deduped away")
        (is (= {:points-at :node-1 :moved true} (<?? S (k/get *client-store* :head)))
            "so the subscriber ends on the server's CURRENT pointer, not its stale one")))))

(deftest filter-fn-integration-test
  (testing "Filter function excludes keys from sync"
    (with-store-peers
      ;; Server has public and private keys
      (<?? S (k/assoc *server-store* :public-data "visible"))
      (<?? S (k/assoc *server-store* :_private "hidden"))
      (<?? S (k/assoc *server-store* :public-other "also-visible"))

      ;; Register with filter
      (kp/register-store! *server-peer* :filtered-store *server-store*
                          {:filter-fn (fn [k _]
                                        (not (clojure.string/starts-with?
                                              (name k) "_")))})

      (<?? S (kp/subscribe-store! *client-peer* :filtered-store *client-store* {}))
      (<?? S (timeout 800))

      ;; Public keys synced
      (is (= "visible" (<?? S (k/get *client-store* :public-data))))
      (is (= "also-visible" (<?? S (k/get *client-store* :public-other))))

      ;; Private key NOT synced
      (is (nil? (<?? S (k/get *client-store* :_private))))

      ;; Incremental publish of private key should also be filtered
      (<?? S (k/assoc *server-store* :_secret "should-not-sync"))
      (<?? S (timeout 300))

      (is (nil? (<?? S (k/get *client-store* :_secret)))))))

(deftest multiple-store-sync-test
  (testing "Multiple stores can be synced as separate topics"
    (with-store-peers
      (let [server-store-2 (<?? S (new-mem-store))
            client-store-2 (<?? S (new-mem-store))]

        ;; Populate stores
        (<?? S (k/assoc *server-store* :store1-key "store1"))
        (<?? S (k/assoc server-store-2 :store2-key "store2"))

        ;; Register both stores
        (kp/register-store! *server-peer* :store-1 *server-store* {})
        (kp/register-store! *server-peer* :store-2 server-store-2 {})

        ;; Subscribe to both
        (<?? S (kp/subscribe-store! *client-peer* :store-1 *client-store* {}))
        (<?? S (kp/subscribe-store! *client-peer* :store-2 client-store-2 {}))
        (<?? S (timeout 1000))

        ;; Verify each store synced to correct client store
        (is (= "store1" (<?? S (k/get *client-store* :store1-key))))
        (is (nil? (<?? S (k/get *client-store* :store2-key))))

        (is (= "store2" (<?? S (k/get client-store-2 :store2-key))))
        (is (nil? (<?? S (k/get client-store-2 :store1-key))))))))

(deftest dissoc-sync-test
  (testing "Key deletions are synced"
    (with-store-peers
      ;; Initial data
      (<?? S (k/assoc *server-store* :to-delete "exists"))
      (<?? S (k/assoc *server-store* :to-keep "stays"))

      ;; Register store
      (kp/register-store! *server-peer* :dissoc-store *server-store* {})

      (<?? S (kp/subscribe-store! *client-peer* :dissoc-store *client-store* {}))
      (<?? S (timeout 800))

      ;; Both keys synced
      (is (= "exists" (<?? S (k/get *client-store* :to-delete))))
      (is (= "stays" (<?? S (k/get *client-store* :to-keep))))

      ;; Delete on server
      (<?? S (k/dissoc *server-store* :to-delete))
      (<?? S (timeout 300))

      ;; Deletion synced to client
      (is (nil? (<?? S (k/get *client-store* :to-delete))))
      (is (= "stays" (<?? S (k/get *client-store* :to-keep)))))))

(deftest unsubscribe-test
  (testing "Unsubscribe stops receiving updates"
    (with-store-peers
      ;; Initial data
      (<?? S (k/assoc *server-store* :key1 "initial"))

      ;; Register and subscribe
      (kp/register-store! *server-peer* :unsub-store *server-store* {})
      (<?? S (kp/subscribe-store! *client-peer* :unsub-store *client-store* {}))
      (<?? S (timeout 800))

      ;; Verify initial sync
      (is (= "initial" (<?? S (k/get *client-store* :key1))))

      ;; Unsubscribe
      (<?? S (kp/unsubscribe-store! *client-peer* :unsub-store))
      (<?? S (timeout 200))

      ;; Update on server
      (<?? S (k/assoc *server-store* :key1 "updated"))
      (<?? S (k/assoc *server-store* :key2 "new"))
      (<?? S (timeout 300))

      ;; Client should NOT have received updates
      (is (= "initial" (<?? S (k/get *client-store* :key1))))
      (is (nil? (<?? S (k/get *client-store* :key2)))))))

(deftest sequential-publish-order-test
  (testing "successive single-key publishes are relayed in call order"
    (with-store-peers
      (kp/register-store! *server-peer* :probe-store *server-store* {})
      (let [applied (atom [])]
        (<?? S (kp/subscribe-store! *client-peer* :probe-store *client-store*
                                    {:on-key-update (fn [k _v _op] (swap! applied conj k))}))
        (<?? S (timeout 500))
        (reset! applied [])
        (<?? S (k/assoc *server-store* :s1 1))
        (<?? S (k/assoc *server-store* :s2 2))
        (<?? S (k/assoc *server-store* :s3 3))
        (<?? S (timeout 800))
        (is (= [:s1 :s2 :s3] @applied)
            "successive assocs arrive in call order")))))
