(ns konserve-sync.transport.kabel-test
  "End-to-end tests for konserve-sync using kabel transport."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [go <! >! <!! timeout chan]]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve-sync.core :as sync]
            [konserve-sync.transport.kabel :as kabel-sync]
            [konserve-sync.transport.protocol :as tp]
            [kabel.peer :as peer]
            [kabel.http-kit :as http-kit]
            [superv.async :refer [<?? S go-try <? >? put?]]))

;; =============================================================================
;; End-to-End Kabel Test
;; =============================================================================

(deftest test-kabel-sync-end-to-end
  (testing "Server and client sync via kabel transport"
    (<!!
      (go
        (let [;; IDs
              server-id #uuid "fd0278e4-081c-4925-abb9-ff4210be271b"
              client-id #uuid "898dcf36-e07a-4338-92fd-f818d573444a"
              store-id #uuid "12345678-1234-1234-1234-123456789012"
              url "ws://localhost:47293"

              ;; Create stores
              server-store (<! (new-mem-store))
              client-store (<! (new-mem-store))

              ;; Pre-populate server store
              _ (<! (k/assoc server-store :key1 "value1"))
              _ (<! (k/assoc server-store :key2 "value2"))

              ;; Create sync contexts
              server-ctx (sync/make-context S {:batch-size 10})
              client-ctx (sync/make-context S)

              ;; Store config
              store-config {:sync/id store-id}
              _ (sync/register-store! server-ctx server-store store-config {})

              ;; Create server with sync middleware
              handler (http-kit/create-http-kit-handler! S url server-id)
              sync-server (kabel-sync/kabel-sync-server S server-ctx nil)

              ;; Server middleware: sync-server-middleware handles sync messages
              server-peer (peer/server-peer S handler server-id
                                            (kabel-sync/sync-server-middleware sync-server)
                                            identity)

              ;; Client atoms for tracking state
              client-transport-atom (atom nil)
              client-complete? (atom false)
              errors (atom [])]

          ;; Start server
          (<?? S (peer/start server-peer))

          ;; Allow server to start
          (<! (timeout 100))

          ;; Create client with sync middleware
          (let [client-peer (peer/client-peer S client-id
                                              (kabel-sync/sync-client-middleware
                                                client-ctx client-transport-atom)
                                              identity)]
            ;; Connect client
            (<?? S (peer/connect S client-peer url))

            ;; Wait for connection
            (<! (timeout 200))

            ;; Get transport and subscribe
            (when-let [transport @client-transport-atom]
              (let [result (<! (sync/subscribe! client-ctx transport store-id client-store
                                                {:on-error #(swap! errors conj %)
                                                 :on-complete #(reset! client-complete? true)}))]
                (when (:ok result)
                  ;; Wait for sync
                  (<! (timeout 500))

                  ;; Verify initial sync
                  (is (= "value1" (<! (k/get client-store :key1))))
                  (is (= "value2" (<! (k/get client-store :key2))))

                  ;; Test incremental update
                  (<! (k/assoc server-store :key3 "value3"))

                  ;; Wait for propagation
                  (<! (timeout 300))

                  ;; Verify incremental update
                  (is (= "value3" (<! (k/get client-store :key3))))
                  (is (empty? @errors))))))

          ;; Cleanup
          (<?? S (peer/stop server-peer)))))))

(deftest test-kabel-multiple-clients
  (testing "Multiple clients receive updates via kabel"
    (<!!
      (go
        (let [;; IDs
              server-id #uuid "fd0278e4-081c-4925-abb9-ff4210be271c"
              client-1-id #uuid "898dcf36-e07a-4338-92fd-f818d573444b"
              client-2-id #uuid "898dcf36-e07a-4338-92fd-f818d573444c"
              store-id #uuid "12345678-1234-1234-1234-123456789013"
              url "ws://localhost:47294"

              ;; Create stores
              server-store (<! (new-mem-store))
              client-1-store (<! (new-mem-store))
              client-2-store (<! (new-mem-store))

              ;; Create sync contexts
              server-ctx (sync/make-context S {:batch-size 10})
              client-1-ctx (sync/make-context S)
              client-2-ctx (sync/make-context S)

              ;; Register server store
              store-config {:sync/id store-id}
              _ (sync/register-store! server-ctx server-store store-config {})

              ;; Create server
              handler (http-kit/create-http-kit-handler! S url server-id)
              sync-server (kabel-sync/kabel-sync-server S server-ctx nil)
              server-peer (peer/server-peer S handler server-id
                                            (kabel-sync/sync-server-middleware sync-server)
                                            identity)

              ;; Transport atoms
              transport-1-atom (atom nil)
              transport-2-atom (atom nil)]

          ;; Start server
          (<?? S (peer/start server-peer))
          (<! (timeout 100))

          ;; Create and connect client 1
          (let [client-1-peer (peer/client-peer S client-1-id
                                                (kabel-sync/sync-client-middleware
                                                  client-1-ctx transport-1-atom)
                                                identity)
                ;; Create and connect client 2
                client-2-peer (peer/client-peer S client-2-id
                                                (kabel-sync/sync-client-middleware
                                                  client-2-ctx transport-2-atom)
                                                identity)]

            (<?? S (peer/connect S client-1-peer url))
            (<?? S (peer/connect S client-2-peer url))
            (<! (timeout 200))

            ;; Subscribe both clients
            (when (and @transport-1-atom @transport-2-atom)
              (<! (sync/subscribe! client-1-ctx @transport-1-atom store-id client-1-store
                                   {:on-error (fn [_] nil)}))
              (<! (sync/subscribe! client-2-ctx @transport-2-atom store-id client-2-store
                                   {:on-error (fn [_] nil)}))

              ;; Wait for subscriptions
              (<! (timeout 300))

              ;; Write to server
              (<! (k/assoc server-store :broadcast-key "broadcast-value"))

              ;; Wait for propagation
              (<! (timeout 300))

              ;; Both clients should have received the update
              (is (= "broadcast-value" (<! (k/get client-1-store :broadcast-key))))
              (is (= "broadcast-value" (<! (k/get client-2-store :broadcast-key))))))

          ;; Cleanup
          (<?? S (peer/stop server-peer)))))))
