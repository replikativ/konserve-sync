(ns konserve-sync.transport.channels-test
  (:require #?(:clj [clojure.test :refer [deftest is testing use-fixtures]]
               :cljs [cljs.test :refer-macros [deftest is testing use-fixtures async]])
            [clojure.core.async :refer [go <! >! timeout alts!] :as async]
            [superv.async :refer [S]]
            [konserve-sync.transport.protocol :as tp]
            [konserve-sync.transport.channels :as ch]))

;; =============================================================================
;; Helper for async tests
;; =============================================================================

#?(:clj
   (defmacro async-test [& body]
     `(let [result# (async/<!! (go ~@body))]
        result#))
   :cljs
   (defmacro async-test [& body]
     `(cljs.test/async done#
        (go
          ~@body
          (done#)))))

;; =============================================================================
;; Channel Transport Tests
;; =============================================================================

(deftest test-channel-pair-creation
  (testing "creates connected pair of transports"
    (let [[t-a t-b] (ch/channel-pair S)]
      (is (some? t-a))
      (is (some? t-b))
      (tp/close! t-a)
      (tp/close! t-b))))

#?(:clj
   (deftest test-channel-transport-send-receive
     (testing "messages sent on A arrive at B"
       (async-test
         (let [[t-a t-b] (ch/channel-pair S)
               received (atom nil)]
           ;; Set up handler on B
           (tp/on-message! t-b #(reset! received %))
           ;; Send from A
           (<! (tp/send! t-a {:msg "hello"}))
           ;; Wait a bit for message to arrive
           (<! (timeout 50))
           (is (= {:msg "hello"} @received))
           (tp/close! t-a)
           (tp/close! t-b))))))

#?(:clj
   (deftest test-channel-transport-bidirectional
     (testing "messages flow in both directions"
       (async-test
         (let [[t-a t-b] (ch/channel-pair S)
               received-a (atom nil)
               received-b (atom nil)]
           ;; Set up handlers
           (tp/on-message! t-a #(reset! received-a %))
           (tp/on-message! t-b #(reset! received-b %))
           ;; Send in both directions
           (<! (tp/send! t-a {:from "a"}))
           (<! (tp/send! t-b {:from "b"}))
           (<! (timeout 50))
           (is (= {:from "b"} @received-a))
           (is (= {:from "a"} @received-b))
           (tp/close! t-a)
           (tp/close! t-b))))))

#?(:clj
   (deftest test-channel-transport-multiple-handlers
     (testing "multiple handlers receive same message"
       (async-test
         (let [[t-a t-b] (ch/channel-pair S)
               received-1 (atom nil)
               received-2 (atom nil)]
           ;; Register multiple handlers on B
           (tp/on-message! t-b #(reset! received-1 %))
           (tp/on-message! t-b #(reset! received-2 %))
           ;; Send from A
           (<! (tp/send! t-a {:msg "multi"}))
           (<! (timeout 50))
           (is (= {:msg "multi"} @received-1))
           (is (= {:msg "multi"} @received-2))
           (tp/close! t-a)
           (tp/close! t-b))))))

#?(:clj
   (deftest test-channel-transport-unregister-handler
     (testing "unregistered handler stops receiving"
       (async-test
         (let [[t-a t-b] (ch/channel-pair S)
               received (atom nil)
               unregister (tp/on-message! t-b #(reset! received %))]
           ;; First message should arrive
           (<! (tp/send! t-a {:msg "first"}))
           (<! (timeout 50))
           (is (= {:msg "first"} @received))
           ;; Unregister and send another
           (unregister)
           (reset! received nil)
           (<! (tp/send! t-a {:msg "second"}))
           (<! (timeout 50))
           (is (nil? @received))
           (tp/close! t-a)
           (tp/close! t-b))))))

#?(:clj
   (deftest test-channel-transport-close
     (testing "closed transport returns error on send"
       (async-test
         (let [[t-a t-b] (ch/channel-pair S)]
           (tp/close! t-a)
           (<! (timeout 50))
           (let [result (<! (tp/send! t-a {:msg "test"}))]
             (is (:error result)))
           (tp/close! t-b))))))

;; =============================================================================
;; Channel Server Tests
;; =============================================================================

#?(:clj
   (deftest test-channel-server-creation
     (testing "creates server"
       (let [server (ch/channel-server S)]
         (is (some? server))
         (ch/close-server! server)))))

#?(:clj
   (deftest test-channel-server-connect
     (testing "client can connect to server"
       (let [server (ch/channel-server S)
             server-transport (atom nil)]
         ;; Register connection handler
         (tp/on-connection! server #(reset! server-transport %))
         ;; Connect client
         (let [client-transport (ch/connect-to-server server)]
           (is (some? client-transport))
           (is (some? @server-transport))
           (ch/close-server! server))))))

#?(:clj
   (deftest test-channel-server-message-exchange
     (testing "client and server can exchange messages"
       (async-test
         (let [server (ch/channel-server S)
               server-transport (atom nil)
               client-received (atom nil)
               server-received (atom nil)]
           ;; Register connection handler
           (tp/on-connection! server
                              (fn [t]
                                (reset! server-transport t)
                                (tp/on-message! t #(reset! server-received %))))
           ;; Connect client
           (let [client-transport (ch/connect-to-server server)]
             (tp/on-message! client-transport #(reset! client-received %))
             ;; Wait for connection
             (<! (timeout 50))
             ;; Send from client to server
             (<! (tp/send! client-transport {:from "client"}))
             (<! (timeout 50))
             (is (= {:from "client"} @server-received))
             ;; Send from server to client
             (<! (tp/send! @server-transport {:from "server"}))
             (<! (timeout 50))
             (is (= {:from "server"} @client-received))
             (ch/close-server! server)))))))

#?(:clj
   (deftest test-channel-server-broadcast
     (testing "broadcast sends to subscribed clients"
       (async-test
         (let [server (ch/channel-server S)
               store-id #uuid "12345678-1234-1234-1234-123456789012"
               received-1 (atom nil)
               received-2 (atom nil)]
           ;; Register connection handler that adds subscription
           (tp/on-connection! server
                              (fn [t]
                                (ch/add-subscription! server t store-id)))
           ;; Connect two clients
           (let [client-1 (ch/connect-to-server server)
                 client-2 (ch/connect-to-server server)]
             (tp/on-message! client-1 #(reset! received-1 %))
             (tp/on-message! client-2 #(reset! received-2 %))
             ;; Wait for connections
             (<! (timeout 50))
             ;; Broadcast
             (<! (tp/broadcast! server store-id {:msg "broadcast"}))
             (<! (timeout 50))
             (is (= {:msg "broadcast"} @received-1))
             (is (= {:msg "broadcast"} @received-2))
             (ch/close-server! server)))))))

