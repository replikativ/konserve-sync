(ns konserve-sync.cbor-transport-node-test
  "konserve-sync's payloads through the CBOR middleware on ClojureScript.

  There are no websockets here. Node has no `WebSocket` without a package, so
  the JVM suite in `cbor_transport_test.clj` owns the socket-level story and
  this file drives the middleware directly over channels — which is the part
  that actually differs by platform.

  What it is really checking is the browser half of the datahike wire: that the
  values konserve-sync puts on it (handshake items, publishes, base64 binary,
  records) survive boring's ClojureScript reader and writer, and that the
  record path needs no registration on the sender."
  (:require [cljs.test :refer-macros [deftest is testing async]]
            [boring.core :as boring]
            [boring.data :as bdata]
            [clojure.core.async :refer [chan] :refer-macros [go]]
            [kabel.middleware.cbor :refer [cbor] :rename {cbor cbor-mw}]
            [kabel.pubsub.protocol :as proto]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve-sync.pubsub :as ks-pubsub]
            [superv.async :refer [S put?] :refer-macros [go-try <?]]))

(defrecord WirePoint [x y])

(defn- mw
  "A middleware instance over a bare channel pair. `:in`/`:out` are the wire
  side, `:tin`/`:tout` the application side."
  ([] (mw cbor-mw))
  ([make]
   (let [in (chan) out (chan)
         [_ _ [tin tout]] (make [S nil [in out]])]
     {:in in :out out :tin tin :tout tout})))

(defn- through
  "Serialize `v` on the out-branch and read it back on the in-branch of a fresh
  middleware instance, i.e. one full trip across the wire."
  ([v] (through v cbor-mw))
  ([v make]
   (go-try S
           (let [a (mw make)
                 b (mw make)]
             (put? S (:tout a) v)
             (let [frame (<? S (:out a))]
               (put? S (:in b) frame)
               [frame (<? S (:tin b))])))))

(deftest frames-are-tagged-cbor-with-byte-payloads
  (async done
         (go
           (let [[frame back] (<? S (through {:key :k :value [1 2 3]}))]
             (is (= :cbor (:kabel/serialization frame)))
             (is (instance? js/Uint8Array (:kabel/payload frame)))
             (is (= {:key :k :value [1 2 3]} back))
             (done)))))

(deftest konserve-sync-payload-shapes-survive
  (testing "the message shapes konserve-sync actually puts on the wire"
    (async done
           (go
             (doseq [v [{:key :a :value "s" :operation :assoc}
                        {:key :a :operation :dissoc}
                        {:key :cfg :value {:nested {:xs [1 2 3]} :set #{:x :y}}}
                        {:key :b :value "AAECAw==" :binary? true}
                        {:key :ts :value {:t (js/Date. 0)}}]]
               (let [[_ back] (<? S (through v))]
                 (is (= v back) (pr-str v))))
             (done)))))

(deftest records-cross-without-registration
  (testing "tag 27 carries the type name, so the SENDER needs nothing"
    (async done
           (go
             (let [[_ back] (<? S (through (->WirePoint 3 4)))]
               (is (bdata/unknown-record? back))
               (is (= 3 (:x back)))
               (is (= 4 (:y back)))
               (is (= "konserve_sync.cbor_transport_node_test.WirePoint"
                      (bdata/record-type back)))
               (done))))))

(deftest records-reconstruct-with-a-read-handler
  (testing "registering the constructor on the READER rebuilds the record.
            `register-record` takes the wire name as a string rather than
            reflecting on the class, because advanced compilation renames
            constructors — this is the portable form."
    (async done
           (go
             (let [reg (atom (boring/register-record
                              (boring/tag-registry)
                              "konserve_sync.cbor_transport_node_test.WirePoint"
                              map->WirePoint))
                   sender (mw)
                   reader (mw #(cbor-mw reg (atom {}) %))]
               (put? S (:tout sender) (->WirePoint 3 4))
               (put? S (:in reader) (<? S (:out sender)))
               (let [back (<? S (:tin reader))]
                 (is (= (->WirePoint 3 4) back))
                 (is (= WirePoint (type back))))
               (done))))))

(deftest handshake-items-survive-the-codec
  (testing "the real thing: an item produced by konserve-sync's server strategy,
            pushed through CBOR, applied by the client strategy, and compared
            byte for byte. This is what a browser peer does on subscribe."
    (async done
           (go
             (let [source (<? S (new-mem-store))
                   target (<? S (new-mem-store))
                   blob (js/Uint8Array. #js [0 1 127 128 255])]
               (<? S (k/bassoc source :blob blob))
               (let [item (<? S (proto/-handshake-items
                                 (ks-pubsub/server-store-strategy source {}) {}))
                     [_ item'] (<? S (through item))]
                 (is (:binary? item'))
                 (is (= (:value item) (:value item'))
                     "base64 payload identical after the CBOR round trip")
                 (<? S (proto/-apply-handshake-item
                        (ks-pubsub/store-sync-strategy target {}) item'))
                 (let [stored (<? S (k/bget target :blob
                                            (fn [{:keys [input-stream]}]
                                              (go input-stream))))]
                   (is (= [0 1 127 128 255] (vec (js/Array.from stored))))))
               (done))))))
