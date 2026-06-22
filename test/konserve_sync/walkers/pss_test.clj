(ns konserve-sync.walkers.pss-test
  "Tests for the parameterizable reachability walk. The walker is format-agnostic:
   `:addresses-fn` projects a stored node to its child addresses. Map-storing
   consumers use the default `:addresses`; object-storing consumers (yggdrasil's
   KonserveStorage returns PSS Leaf/Branch OBJECTS) MUST pass an object-aware
   projector, else the walk silently collapses to the root alone — the multi-node
   sync/GC regression this pins."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!!]]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve-sync.walkers.pss :as pss]))

(defn- build-store!
  "A two-level tree whose branch node carries its child addresses under
   `child-key` (`:addresses` for map nodes; a different key to SIMULATE an
   object node that does not answer `:addresses` as a keyword)."
  [child-key]
  (let [store (<!! (new-mem-store))
        leaf-a (random-uuid) leaf-b (random-uuid) root (random-uuid)]
    (<!! (k/assoc store leaf-a {:level 0 :keys [:a]} {:sync? false}))
    (<!! (k/assoc store leaf-b {:level 0 :keys [:b]} {:sync? false}))
    (<!! (k/assoc store root {:level 1 :keys [] child-key [leaf-a leaf-b]} {:sync? false}))
    (<!! (k/assoc store :crdt/roots {:main root} {:sync? false}))
    (<!! (k/assoc store :crdt/freed {} {:sync? false}))
    {:store store :root root :leaf-a leaf-a :leaf-b leaf-b}))

(deftest default-addresses-fn-reaches-map-nodes
  (testing "the default `:addresses` projector walks plain-map branch nodes"
    (let [{:keys [store root leaf-a leaf-b]} (build-store! :addresses)
          walk      (pss/make-pss-walk-fn :crdt/roots #{:crdt/roots :crdt/freed})
          reachable (<!! (walk store {}))]
      (is (contains? reachable root))
      (is (contains? reachable leaf-a) "child leaf reached")
      (is (contains? reachable leaf-b) "child leaf reached"))))

(deftest custom-addresses-fn-reaches-object-like-nodes
  (testing "a node whose children are NOT under `:addresses` (an OBJECT-like node):
            the default walker collapses to the root alone — the latent bug — while
            an injected `:addresses-fn` reaches the whole tree (the fix)"
    (let [{:keys [store root leaf-a leaf-b]} (build-store! :child-addrs)
          default-walk (pss/make-pss-walk-fn :crdt/roots #{:crdt/roots :crdt/freed})
          default-set  (<!! (default-walk store {}))]
      (is (contains? default-set root))
      (is (not (contains? default-set leaf-a)) "BUG shape: leaves unreachable via :addresses")
      (is (not (contains? default-set leaf-b)))
      (let [object-walk (pss/make-pss-walk-fn :crdt/roots #{:crdt/roots :crdt/freed} :child-addrs)
            object-set  (<!! (object-walk store {}))]
        (is (contains? object-set root))
        (is (contains? object-set leaf-a) "FIX: injected projector reaches the leaves")
        (is (contains? object-set leaf-b))))))

(deftest sync-opts-threads-addresses-fn
  (testing "make-pss-sync-opts forwards the custom addresses-fn into its walker"
    (let [{:keys [store leaf-a]} (build-store! :child-addrs)
          opts (pss/make-pss-sync-opts :crdt/roots #{:crdt/roots :crdt/freed} :child-addrs)
          reachable (<!! ((:walk-fn opts) store {}))]
      (is (= pss/keyword-last (:key-sort-fn opts)))
      (is (contains? reachable leaf-a) "the threaded projector reaches the leaves"))))
