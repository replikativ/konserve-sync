(ns konserve-sync.sync-test
  "Integration tests for konserve-sync using channel transport."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :refer [go <! >! <!! timeout alts!] :as async]
            [superv.async :refer [S]]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve-sync.core :as sync]
            [konserve-sync.protocol :as proto]
            [konserve-sync.transport.protocol :as tp]
            [konserve-sync.transport.channels :as ch]))

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(defn async-test
  "Run async test body and wait for result."
  [f]
  (<!! (go (f))))

;; =============================================================================
;; Basic Context Tests
;; =============================================================================

(deftest test-make-context
  (testing "creates context with defaults"
    (let [ctx (sync/make-context S)]
      (is (some? ctx))
      (is (= 20 (get-in ctx [:opts :batch-size])))))

  (testing "creates context with custom options"
    (let [ctx (sync/make-context S {:batch-size 50})]
      (is (= 50 (get-in ctx [:opts :batch-size]))))))

;; =============================================================================
;; Store Registration Tests
;; =============================================================================

(deftest test-register-store
  (testing "registers store and returns store-id"
    (async-test
      #(let [ctx (sync/make-context S)
             store (<!! (new-mem-store))
             store-config {:scope #uuid "12345678-1234-1234-1234-123456789012"
                           :backend :memory}
             store-id (sync/register-store! ctx store store-config {})]
         (is (= (proto/store-id store-config) store-id))
         (is (some? (sync/get-store ctx store-id)))))))

(deftest test-unregister-store
  (testing "unregisters store"
    (async-test
      #(let [ctx (sync/make-context S)
             store (<!! (new-mem-store))
             store-config {:scope #uuid "12345678-1234-1234-1234-123456789012"
                           :backend :memory}
             store-id (sync/register-store! ctx store store-config {})]
         (sync/unregister-store! ctx store-id)
         (is (nil? (sync/get-store ctx store-id)))))))

;; =============================================================================
;; End-to-End Sync Tests
;; =============================================================================

(deftest test-basic-subscription-flow
  (testing "client subscribes and receives initial sync"
    (<!!
      (go
        (let [;; Create stores
              server-store (<! (new-mem-store))
              client-store (<! (new-mem-store))

              ;; Pre-populate server store
              _ (<! (k/assoc server-store :key1 "value1"))
              _ (<! (k/assoc server-store :key2 "value2"))

              ;; Create contexts
              server-ctx (sync/make-context S {:batch-size 10})
              client-ctx (sync/make-context S)

              ;; Store config - same config used by server and client
              store-config {:scope #uuid "12345678-1234-1234-1234-123456789012"
                            :backend :memory}
              store-id (sync/register-store! server-ctx server-store store-config {})

              ;; Create channel transports
              [server-transport client-transport] (ch/channel-pair S)

              ;; Set up server to handle subscriptions
              _ (tp/on-message! server-transport
                                (fn [msg]
                                  (when (proto/subscribe-msg? msg)
                                    (sync/serve-subscription! server-ctx server-transport msg))))

              ;; Subscribe client using same store-id (computed from same config)
              errors (atom [])
              result (<! (sync/subscribe! client-ctx client-transport store-id client-store
                                          {:on-error #(swap! errors conj %)}))]

          ;; Wait for sync to complete
          (<! (timeout 500))

          ;; Verify data synced
          (is (= "value1" (<! (k/get client-store :key1))))
          (is (= "value2" (<! (k/get client-store :key2))))
          (is (empty? @errors))

          ;; Cleanup
          (tp/close! server-transport)
          (tp/close! client-transport))))))

(deftest test-incremental-updates
  (testing "client receives updates after initial sync"
    (<!!
      (go
        (let [;; Create stores
              server-store (<! (new-mem-store))
              client-store (<! (new-mem-store))

              ;; Create contexts
              server-ctx (sync/make-context S {:batch-size 10})
              client-ctx (sync/make-context S)

              ;; Store config - same config used by server and client
              store-config {:scope #uuid "12345678-1234-1234-1234-123456789012"
                            :backend :memory}
              store-id (sync/register-store! server-ctx server-store store-config {})

              ;; Create channel transports
              [server-transport client-transport] (ch/channel-pair S)

              ;; Set up server to handle subscriptions
              _ (tp/on-message! server-transport
                                (fn [msg]
                                  (when (proto/subscribe-msg? msg)
                                    (sync/serve-subscription! server-ctx server-transport msg))))

              ;; Subscribe client (empty store)
              _ (<! (sync/subscribe! client-ctx client-transport store-id client-store
                                     {:on-error (fn [_] nil)}))]

          ;; Wait for sync
          (<! (timeout 200))

          ;; Now write to server (should propagate to client)
          (<! (k/assoc server-store :new-key "new-value"))

          ;; Wait for propagation
          (<! (timeout 200))

          ;; Verify update arrived
          (is (= "new-value" (<! (k/get client-store :new-key))))

          ;; Cleanup
          (tp/close! server-transport)
          (tp/close! client-transport))))))

(deftest test-callback-on-update
  (testing "registered callback is called on key update"
    (<!!
      (go
        (let [;; Create stores
              server-store (<! (new-mem-store))
              client-store (<! (new-mem-store))

              ;; Create contexts
              server-ctx (sync/make-context S)
              client-ctx (sync/make-context S)

              ;; Store config - same config used by server and client
              store-config {:scope #uuid "12345678-1234-1234-1234-123456789012"
                            :backend :memory}
              store-id (sync/register-store! server-ctx server-store store-config {})

              ;; Create channel transports
              [server-transport client-transport] (ch/channel-pair S)

              ;; Set up server
              _ (tp/on-message! server-transport
                                (fn [msg]
                                  (when (proto/subscribe-msg? msg)
                                    (sync/serve-subscription! server-ctx server-transport msg))))

              ;; Subscribe client
              _ (<! (sync/subscribe! client-ctx client-transport store-id client-store
                                     {:on-error (fn [_] nil)}))

              ;; Register callback
              callback-received (atom nil)
              _ (sync/register-callback! client-ctx store-id :watched-key
                                         #(reset! callback-received %))]

          ;; Wait for sync
          (<! (timeout 200))

          ;; Write to watched key
          (<! (k/assoc server-store :watched-key "watched-value"))

          ;; Wait for propagation
          (<! (timeout 200))

          ;; Verify callback was called
          (is (some? @callback-received))
          (is (= "watched-value" (:value @callback-received)))
          (is (= :watched-key (:key @callback-received)))

          ;; Cleanup
          (tp/close! server-transport)
          (tp/close! client-transport))))))

(deftest test-filter-function
  (testing "filter-fn excludes keys from sync"
    (<!!
      (go
        (let [;; Create stores
              server-store (<! (new-mem-store))
              client-store (<! (new-mem-store))

              ;; Pre-populate server
              _ (<! (k/assoc server-store :public "public-data"))
              _ (<! (k/assoc server-store :private "private-data"))

              ;; Create contexts
              server-ctx (sync/make-context S)
              client-ctx (sync/make-context S)

              ;; Store config with filter - same config used by server and client
              store-config {:scope #uuid "12345678-1234-1234-1234-123456789012"
                            :backend :memory}
              store-id (sync/register-store! server-ctx server-store store-config
                                             {:filter-fn (fn [k _] (not= k :private))})

              ;; Create channel transports
              [server-transport client-transport] (ch/channel-pair S)

              ;; Set up server
              _ (tp/on-message! server-transport
                                (fn [msg]
                                  (when (proto/subscribe-msg? msg)
                                    (sync/serve-subscription! server-ctx server-transport msg))))

              ;; Subscribe client
              _ (<! (sync/subscribe! client-ctx client-transport store-id client-store
                                     {:on-error (fn [_] nil)}))]

          ;; Wait for sync
          (<! (timeout 500))

          ;; Verify only public data synced
          (is (= "public-data" (<! (k/get client-store :public))))
          (is (nil? (<! (k/get client-store :private))))

          ;; Cleanup
          (tp/close! server-transport)
          (tp/close! client-transport))))))

