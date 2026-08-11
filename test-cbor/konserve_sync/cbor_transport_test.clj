(ns konserve-sync.cbor-transport-test
  "konserve-sync running over the CBOR transport, end to end.

  konserve-sync is serializer-agnostic by construction: `peer/server-peer` and
  `peer/client-peer` take the serialization middleware as their last argument,
  and konserve-sync never names one — its own docstrings pass `identity`. So
  nothing in `src/` changes here; these tests only swap that argument.

  `identity` is not 'no serialization', though. `kabel.binary/to-binary` falls
  back to `pr-str` + `edn/read-string` when no middleware set
  `:kabel/serialization`, which is why `a-record-poisons-the-default-transport`
  below is a *passing* test asserting a *failure*: the default transport cannot
  carry a defrecord at all. That is the baseline the CBOR tests are measured
  against, and it is worth having in the file rather than in a commit message.

  The last test is the full vertical — CBOR on the wire AND konserve's boring
  serializer (byte 3) in both stores, with the on-disk header byte asserted."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest testing is]]
            [boring.core :as boring]
            [boring.data :as bdata]
            [kabel.http-kit :as http-kit]
            [kabel.middleware.cbor :as cbor-mw]
            [kabel.peer :as peer]
            [konserve.core :as k]
            [konserve.filestore :refer [connect-fs-store delete-store]]
            [konserve.memory :refer [new-mem-store]]
            [konserve-sync.transport.kabel-pubsub :as kp]
            [clojure.core.async :refer [go timeout]]
            [superv.async :refer [<?? S]]))

(defrecord WirePoint [x y])

(def ^:dynamic *server-peer* nil)
(def ^:dynamic *client-peer* nil)

(defn- unique-port []
  ;; Same scheme as pubsub_test.clj, on a disjoint range so the two suites can
  ;; run concurrently without fighting over a socket.
  (+ 47700 (rand-int 200)))

(defn- stored-bytes [store key]
  (<?? S (k/bget store key (fn [{:keys [input-stream]}] (go input-stream)))))

(defn- with-peers*
  "Run `f` with a connected server/client peer pair.

  Adapted from `with-store-peers` in `konserve_sync.pubsub-test`, which
  hardcodes `identity` as the serialization middleware and so cannot be reused
  directly — that argument is exactly what varies here. It also takes the
  stores as arguments rather than creating memory stores itself, because the
  vertical test needs filestores.

  The two-middleware arity exists because a record's read handler belongs only
  on the side that reconstructs it; keeping the ends asymmetric is what proves
  the server needs no registration."
  ([serialization-mw f] (with-peers* serialization-mw serialization-mw f))
  ([server-mw client-mw f]
   (let [url (str "ws://localhost:" (unique-port))
         sid (java.util.UUID/randomUUID)
         cid (java.util.UUID/randomUUID)
         handler (http-kit/create-http-kit-handler! S url sid)
         server (peer/server-peer S handler sid (kp/server-middleware) server-mw)]
     (<?? S (peer/start server))
     (try
       (let [client (peer/client-peer S cid (kp/client-middleware) client-mw)]
         (<?? S (peer/connect S client url))
         (<?? S (timeout 200))
         (binding [*server-peer* server
                   *client-peer* client]
           (f)))
       (finally
         (<?? S (peer/stop server)))))))

(defmacro with-cbor-peers [& body]
  `(with-peers* cbor-mw/cbor (fn [] ~@body)))

(defmacro with-default-peers
  "The transport konserve-sync's own docs and tests use: `identity`, which
  means kabel's `pr-str` fallback."
  [& body]
  `(with-peers* identity (fn [] ~@body)))

;; ---------------------------------------------------------------------------
;; Core sync, unchanged behaviour on a new transport
;; ---------------------------------------------------------------------------

(deftest handshake-and-incremental-sync-over-cbor
  (testing "the existing sync semantics must be identical over CBOR — the
            transport is the only thing that changed"
    (let [server-store (<?? S (new-mem-store))
          client-store (<?? S (new-mem-store))]
      (with-cbor-peers
        (<?? S (k/assoc server-store :key1 "value1"))
        (<?? S (k/assoc server-store :key2 {:nested "data"}))
        (<?? S (k/assoc server-store :key3 [1 2 3]))
        (<?? S (k/assoc server-store :key4 #{:a :b}))
        (<?? S (k/assoc server-store :doomed "will be deleted"))

        (kp/register-store! *server-peer* :cbor-store server-store {})
        (<?? S (kp/subscribe-store! *client-peer* :cbor-store client-store {}))
        (<?? S (timeout 1000))

        (testing "handshake"
          (is (= "value1" (<?? S (k/get client-store :key1))))
          (is (= {:nested "data"} (<?? S (k/get client-store :key2))))
          (is (= [1 2 3] (<?? S (k/get client-store :key3))))
          (is (= #{:a :b} (<?? S (k/get client-store :key4)))
              "sets go through CBOR tag 258, not through a handler"))

        (testing "incremental assoc"
          (<?? S (k/assoc server-store :later {:added "after handshake"}))
          (<?? S (timeout 400))
          (is (= {:added "after handshake"} (<?? S (k/get client-store :later)))))

        (testing "incremental dissoc"
          (<?? S (k/dissoc server-store :doomed))
          (<?? S (timeout 400))
          (is (nil? (<?? S (k/get client-store :doomed)))))))))

(deftest binary-values-survive-the-cbor-transport
  (testing "bassoc/bget with byte equality, on the handshake path and the
            incremental path.

            Note what actually crosses the wire: konserve-sync base64-encodes
            binary values in `pubsub.cljc` so they can ride any serializer,
            which costs ~33%. boring could carry them as a CBOR byte string
            instead, but that is a src/ change and is deliberately not made
            here — this test pins that the existing encoding keeps working."
    (let [server-store (<?? S (new-mem-store))
          client-store (<?? S (new-mem-store))
          initial (byte-array (map unchecked-byte (range 64)))
          incremental (byte-array (map unchecked-byte (range 127 -1 -1)))]
      (with-cbor-peers
        (<?? S (k/bassoc server-store :initial-blob initial))
        (kp/register-store! *server-peer* :bin-store server-store {})
        (<?? S (kp/subscribe-store! *client-peer* :bin-store client-store {}))
        (<?? S (timeout 900))

        (is (= (seq initial) (seq (stored-bytes client-store :initial-blob))))
        (is (= :binary (:type (<?? S (k/get-meta client-store :initial-blob)))))

        (<?? S (k/bassoc server-store :new-blob incremental))
        (<?? S (timeout 500))
        (is (= (seq incremental) (seq (stored-bytes client-store :new-blob))))
        (is (= :binary (:type (<?? S (k/get-meta client-store :new-blob)))))))))

;; ---------------------------------------------------------------------------
;; Records — the thing the default transport cannot do
;; ---------------------------------------------------------------------------

(deftest a-record-poisons-the-default-transport
  (testing "with `identity`, kabel falls back to pr-str/edn for the WHOLE
            frame. A defrecord prints as #ns.Type{...}, for which edn has no
            reader, so `edn/read-string` throws inside the client's websocket
            read loop — and that kills the CONNECTION, not just the message.

            So the baseline is worse than 'records lose their type': a single
            record anywhere in a synced store silently stops the sync for every
            other key too. The three assertions below are before / during /
            after, in that order, because the interesting part is the third."
    (let [server-store (<?? S (new-mem-store))
          client-store (<?? S (new-mem-store))]
      (with-default-peers
        (<?? S (k/assoc server-store :before {:ordinary "data"}))
        (kp/register-store! *server-peer* :pr-store server-store {})
        (<?? S (kp/subscribe-store! *client-peer* :pr-store client-store {}))
        (<?? S (timeout 900))
        (is (= {:ordinary "data"} (<?? S (k/get client-store :before)))
            "control: plain data syncs fine over pr-str")

        (<?? S (k/assoc server-store :rec (->WirePoint 3 4)))
        (<?? S (timeout 600))
        (is (nil? (<?? S (k/get client-store :rec)))
            "the record's frame could not be decoded")

        (<?? S (k/assoc server-store :after {:ordinary "more"}))
        (<?? S (timeout 600))
        (is (nil? (<?? S (k/get client-store :after)))
            "and the connection is now dead — an unrelated later key is lost
             too. This is the collateral damage CBOR removes.")))))

(deftest records-cross-cbor-without-registration
  (testing "boring writes a record's type name natively via CBOR tag 27, so
            with no handler anywhere the value still arrives carrying its name
            and fields instead of being lost. That is what makes an
            unregistered record safe rather than silently destructive."
    (let [server-store (<?? S (new-mem-store))
          client-store (<?? S (new-mem-store))]
      (with-cbor-peers
        (<?? S (k/assoc server-store :rec (->WirePoint 3 4)))
        (kp/register-store! *server-peer* :rec-store server-store {})
        (<?? S (kp/subscribe-store! *client-peer* :rec-store client-store {}))
        (<?? S (timeout 900))
        (let [back (<?? S (k/get client-store :rec))]
          (is (some? back) "it arrived")
          (is (= 3 (:x back)))
          (is (= 4 (:y back)))
          (is (bdata/unknown-record? back)
              "it is an inert UnknownRecord, not a plain map that silently
               dropped its identity")
          ;; boring >= 0.1.11 spells a record's wire name
          ;; `my-ns.core/MyRecord` -- the last dot becomes the namespace
          ;; separator and underscores in the namespace part become hyphens.
          ;; It used to be the munged `my_ns.core.MyRecord`, which is what this
          ;; asserted while the dependency was pinned eleven versions back.
          (is (= "konserve-sync.cbor-transport-test/WirePoint"
                 (bdata/record-type back))
              "and it still knows what it was"))

        (testing "and the connection is still alive afterwards — the exact
                  contrast with a-record-poisons-the-default-transport"
          (<?? S (k/assoc server-store :after {:ordinary "more"}))
          (<?? S (timeout 600))
          (is (= {:ordinary "more"} (<?? S (k/get client-store :after)))))))))

(deftest records-reconstruct-with-a-read-handler
  (testing "registering the constructor on the CLIENT's registry is enough;
            the server needs nothing, because the type name is on the wire"
    (let [server-store (<?? S (new-mem-store))
          client-store (<?? S (new-mem-store))
          client-registry
          ;; REGISTERED UNDER BORING'S OWN SPELLING, because this registers
          ;; straight with boring rather than through kabel's incognito bridge
          ;; -- so nothing here translates the name, and the constructor is
          ;; simply never reached if it does not match what the writer wrote.
          (atom (boring/register-record
                 (boring/tag-registry)
                 "konserve-sync.cbor-transport-test/WirePoint"
                 map->WirePoint))
          client-mw #(cbor-mw/cbor client-registry (atom {}) %)]
      ;; asymmetric on purpose: plain boring on the server, a registry on the
      ;; client.
      (with-peers*
        cbor-mw/cbor client-mw
        (fn []
          (<?? S (k/assoc server-store :rec (->WirePoint 3 4)))
          (kp/register-store! *server-peer* :rec2-store server-store {})
          (<?? S (kp/subscribe-store! *client-peer* :rec2-store client-store {}))
          (<?? S (timeout 900))
          (let [back (<?? S (k/get client-store :rec))]
            (is (= (->WirePoint 3 4) back))
            (is (= WirePoint (type back)))))))))

;; ---------------------------------------------------------------------------
;; The vertical: boring on the wire and boring at rest, on both peers
;; ---------------------------------------------------------------------------

(defn- header-serializer-byte
  "The serializer id konserve wrote into a blob header under `dir`.

  Header layout is fixed by `konserve.impl.storage-layout/create-header`:
  byte 0 is the version, byte 1 the serializer id."
  [dir]
  (let [f (->> (file-seq (io/file dir))
               (filter #(.isFile ^java.io.File %))
               (filter #(re-find #"\.ksv$" (.getName ^java.io.File %)))
               first)]
    (when f
      (let [head (byte-array 4)]
        (with-open [in (io/input-stream f)]
          (.read in head))
        (aget head 1)))))

(deftest both-stores-on-the-boring-serializer
  (testing "end to end: CBOR on the wire, and konserve's boring
            serializer (byte 3) at rest on BOTH peers, with the on-disk header
            byte asserted rather than the serializer merely named. Memory
            stores cannot show this — `konserve.memory` implements
            `PAssocSerializers` as a no-op and keeps live objects, so it would
            pass with any serializer at all."
    (let [base (str (System/getProperty "java.io.tmpdir") "/konserve-sync-cbor")
          sdir (str base "-server")
          cdir (str base "-client")]
      (delete-store sdir)
      (delete-store cdir)
      (try
        (let [server-store (<?? S (connect-fs-store
                                   sdir :default-serializer :BoringSerializer))
              client-store (<?? S (connect-fs-store
                                   cdir :default-serializer :BoringSerializer))]
          (with-cbor-peers
            (<?? S (k/assoc server-store :cfg {:name "vertical" :n 7}))
            (<?? S (k/assoc server-store :xs (vec (range 100))))
            (kp/register-store! *server-peer* :fs-store server-store {})
            (<?? S (kp/subscribe-store! *client-peer* :fs-store client-store {}))
            (<?? S (timeout 1200))

            (is (= {:name "vertical" :n 7} (<?? S (k/get client-store :cfg))))
            (is (= (vec (range 100)) (<?? S (k/get client-store :xs))))

            (testing "and both sides really wrote byte 3"
              (is (= 3 (header-serializer-byte sdir)))
              (is (= 3 (header-serializer-byte cdir))))))
        (finally
          (delete-store sdir)
          (delete-store cdir))))))
