(ns konserve-sync.overlay-test
  "konserve-sync over Netz's peer-to-peer overlay.

  Its `StoreSyncStrategy` already expresses both paths a replicated store needs —
  the live path (`-apply-publish`) and a differential state sync
  (`-init-client-state` → `-handshake-items` → `-apply-handshake-item`) — and
  neither was ever the transport's business. So this test calls exactly the
  same `register-store!` and `subscribe-store!` that the single-connection
  tests call, and only the middleware differs.

  What the overlay changes:

  - a **publish** is disseminated — multi-hop, signed at its origin, verified
    at every hop, deduplicated, and repairable — instead of being sent down one
    socket;
  - a **subscription** is topic interest, so a relay can carry one store's
    range without carrying the network;
  - the **handshake stays point-to-point**, because a bulk acknowledged
    backpressured transfer of a whole store is the last thing that should be
    broadcast. What changes is who you may handshake with."
  (:require [clojure.test :refer [deftest testing is]]
            [konserve-sync.transport.kabel-pubsub :as kp]
            [kabel.pubsub :as pubsub]
            [netz.pubsub :as pso]
            [netz.overlay.runtime :as rt]
            [netz.identity :as id]
            [kabel.peer :as peer]
            [kabel.http-kit :as http-kit]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [superv.async :refer [<?? S]]
            [clojure.core.async :refer [timeout]]))

(def ^:private port-counter (atom 47700))
(defn- unique-port [] (swap! port-counter inc))

(defn- wait-for [ms pred]
  (let [deadline (+ (System/currentTimeMillis) ms)]
    (loop []
      (cond (pred) true
            (> (System/currentTimeMillis) deadline) false
            :else (do (<?? S (timeout 25)) (recur))))))

(defn- with-overlay-peers
  "Two peers whose middleware carries BOTH pub/sub and the overlay, and whose
  pub/sub publishes are routed through the overlay.

  Calls `(f ctx)` with the stores, peers and runtimes."
  [f]
  (let [port (unique-port)
        url (str "ws://localhost:" port)
        server-kp (<?? S (id/generate-identity))
        client-kp (<?? S (id/generate-identity))
        server-id (id/peer-id (:genesis server-kp))
        client-id (id/peer-id (:genesis client-kp))
        server-store (<?? S (new-mem-store))
        client-store (<?? S (new-mem-store))
        [server-ov install-server!] (rt/deferred-middleware)
        ;; The only line that differs from the single-connection fixture:
        ;; `comp` instead of `kp/server-middleware` alone. `comp` applies
        ;; right-to-left, so the overlay sees the raw socket and passes what it
        ;; does not recognise through to pub/sub.
        server (peer/server-peer S (http-kit/create-http-kit-handler! S url server-id)
                                 server-id
                                 (comp (kp/server-middleware) server-ov)
                                 identity)
        server-run (<?? S (rt/start! S server {:identity server-kp
                                               :addresses [url]
                                               :topics #{}}))]
    (install-server! (:middleware server-run))
    (<?? S (peer/start server))
    (try
      (let [[client-ov install-client!] (rt/deferred-middleware)
            client (peer/client-peer S client-id
                                     (comp (kp/client-middleware) client-ov)
                                     identity)
            client-run (<?? S (rt/start! S client
                                         {:identity client-kp
                                          :addresses []
                                          :topics #{}
                                          :seeds [{:peer-id server-id
                                                   :addresses [url]
                                                   :group "seed"}]}))]
        (install-client! (:middleware client-run))
        (pso/install! S server server-run)
        (pso/install! S client client-run)
        (<?? S (peer/connect S client url))
        (is (wait-for 8000 #(contains? (rt/connections client) server-id))
            "the overlay never connected, so nothing below would prove anything")
        (f {:server server :client client
            :server-store server-store :client-store client-store
            :server-run server-run :client-run client-run
            :server-id server-id :client-id client-id}))
      (finally (<?? S (peer/stop server))))))

;; =============================================================================
;; The handshake: a whole store, point-to-point
;; =============================================================================

(deftest a-store-syncs-over-the-overlay
  (testing "the initial handshake transfers the store, unchanged"
    (with-overlay-peers
      (fn [{:keys [server client server-store client-store]}]
        (<?? S (k/assoc server-store :key1 "value1"))
        (<?? S (k/assoc server-store :key2 {:nested "data"}))
        (<?? S (k/assoc server-store :key3 [1 2 3]))

        ;; Exactly the calls the single-connection test makes.
        (kp/register-store! server :test-store server-store {})
        (<?? S (kp/subscribe-store! client :test-store client-store {}))

        (is (wait-for 8000 #(= "value1" (<?? S (k/get client-store :key1))))
            "the handshake never delivered the store")
        (is (= {:nested "data"} (<?? S (k/get client-store :key2))))
        (is (= [1 2 3] (<?? S (k/get client-store :key3))))))))

;; =============================================================================
;; The live path: a publish, disseminated
;; =============================================================================

(deftest incremental-writes-are-disseminated
  (testing "a write after the handshake reaches the subscriber as gossip"
    (with-overlay-peers
      (fn [{:keys [server client server-store client-store
                   server-run client-run]}]
        (<?? S (k/assoc server-store :initial "before"))
        (kp/register-store! server :test-store server-store {})
        (<?? S (kp/subscribe-store! client :test-store client-store {}))
        (is (wait-for 8000 #(= "before" (<?? S (k/get client-store :initial))))
            "the handshake never completed, so the increment proves nothing")

        ;; The write hook publishes. On this transport that means dissemination.
        (<?? S (k/assoc server-store :later "after"))
        (is (wait-for 8000 #(= "after" (<?? S (k/get client-store :later))))
            "the incremental write never arrived")

        ;; And prove it was the overlay that carried it rather than the
        ;; point-to-point channel the handshake used — otherwise this test
        ;; would pass just as well with no overlay at all.
        (let [sv (:dissemination (rt/overlay-state server-run))
              c (:dissemination (rt/overlay-state client-run))]
          (is (pos? (get-in sv [:stats :published] 0))
              "the server never published through the overlay")
          (is (pos? (get-in c [:stats :delivered] 0))
              "the client never received anything through the overlay"))))))

;; =============================================================================
;; A dissoc, and the strategy that was never touched
;; =============================================================================

(deftest deletions-propagate-too
  (testing "dissoc is an ordinary publish and needs nothing special"
    (with-overlay-peers
      (fn [{:keys [server client server-store client-store]}]
        (<?? S (k/assoc server-store :doomed "here"))
        (kp/register-store! server :test-store server-store {})
        (<?? S (kp/subscribe-store! client :test-store client-store {}))
        (is (wait-for 8000 #(= "here" (<?? S (k/get client-store :doomed)))))

        (<?? S (k/dissoc server-store :doomed))
        (is (wait-for 8000 #(nil? (<?? S (k/get client-store :doomed))))
            "the deletion never propagated")))))

(deftest public-unsubscribe-withdraws-overlay-interest
  (testing "the unchanged konserve-sync API dispatches through Netz"
    (with-overlay-peers
      (fn [{:keys [server client server-store client-store client-run]}]
        (<?? S (k/assoc server-store :key "initial"))
        (kp/register-store! server :test-store server-store {})
        (<?? S (kp/subscribe-store! client :test-store client-store {}))
        (is (wait-for 8000 #(= "initial" (<?? S (k/get client-store :key)))))

        (is (= {:ok true}
               (<?? S (kp/unsubscribe-store! client :test-store))))
        (is (wait-for 3000
                      #(not (contains? (get-in (rt/overlay-state client-run)
                                               [:dissemination :topics])
                                       :test-store)))
            "Netz retained topic interest after the public unsubscribe")

        (<?? S (k/assoc server-store :key "after"))
        (<?? S (timeout 300))
        (is (= "initial" (<?? S (k/get client-store :key))))))))
