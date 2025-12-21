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
