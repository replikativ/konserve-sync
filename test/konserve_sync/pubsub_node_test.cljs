(ns konserve-sync.pubsub-node-test
  (:require [cljs.core.async :refer [<!]]
            [cljs.test :refer-macros [async deftest is]]
            [kabel.pubsub.protocol :as proto]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve-sync.pubsub :as pubsub])
  (:require-macros [cljs.core.async.macros :refer [go]]))

(defn- stored-bytes [store key]
  (k/bget store key
          (fn [{:keys [input-stream]}]
            (go input-stream))))

(deftest binary-handshake-roundtrip
  (async done
         (go
           (let [source (<! (new-mem-store))
                 target (<! (new-mem-store))
                 expected (js/Uint8Array. #js [0 1 127 128 255])
                 _ (<! (k/bassoc source :blob expected))
                 item (<! (proto/-handshake-items
                           (pubsub/server-store-strategy source {}) {}))
                 result (<! (proto/-apply-handshake-item
                             (pubsub/store-sync-strategy target {}) item))
                 actual (<! (stored-bytes target :blob))]
             (is (:binary? item))
             (is (= {:ok true} result))
             (is (= [0 1 127 128 255]
                    (vec (js/Array.from actual))))
             (done)))))
