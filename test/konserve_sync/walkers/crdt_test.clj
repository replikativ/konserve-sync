(ns konserve-sync.walkers.crdt-test
  "Tests for the durable-CRDT walker. The store is hand-built to the on-disk
   contract of `yggdrasil.convergent.durable` (content-addressed plain-map PSS
   nodes + a `:crdt/roots` cell with one root per branch), so the test is
   self-contained (no yggdrasil dependency) and pins the format the walker
   relies on. Real-yggdrasil coverage is the durable-gset sync test in
   yggdrasil itself."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!!]]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve-sync.walkers.crdt :as crdt]
            [konserve-sync.walkers.pss :as pss]))

(defn- build-crdt-store!
  "A memory store shaped like a durable G-Set with TWO branches sharing a leaf:
     :main root over leaf-shared + leaf-a
     :fork root over leaf-shared + leaf-b
   plus the :crdt/roots / :crdt/freed pointers and an orphan block."
  []
  (let [store (<!! (new-mem-store))
        leaf-shared (random-uuid) leaf-a (random-uuid) leaf-b (random-uuid)
        root-main (random-uuid) root-fork (random-uuid)]
    (<!! (k/assoc store leaf-shared {:level 0 :keys [:x :y]} {:sync? false}))
    (<!! (k/assoc store leaf-a {:level 0 :keys [:a]} {:sync? false}))
    (<!! (k/assoc store leaf-b {:level 0 :keys [:b]} {:sync? false}))
    (<!! (k/assoc store root-main {:level 1 :keys [] :addresses [leaf-shared leaf-a]} {:sync? false}))
    (<!! (k/assoc store root-fork {:level 1 :keys [] :addresses [leaf-shared leaf-b]} {:sync? false}))
    (<!! (k/assoc store :crdt.head/main {:root root-main} {:sync? false}))
    (<!! (k/assoc store :crdt.head/fork {:root root-fork} {:sync? false}))
    (<!! (k/assoc store :crdt/branches #{:main :fork} {:sync? false}))
    (<!! (k/assoc store (random-uuid) {:level 0 :keys [:orphan]} {:sync? false}))
    {:store store :root-main root-main :root-fork root-fork
     :leaf-shared leaf-shared :leaf-a leaf-a :leaf-b leaf-b}))

(deftest crdt-walk-reaches-all-branch-roots
  (testing "walker reaches every node from EVERY branch root + the pointers"
    (let [{:keys [store root-main root-fork leaf-shared leaf-a leaf-b]} (build-crdt-store!)
          reachable (<!! (crdt/crdt-walk-fn store {}))]
      (is (contains? reachable :crdt/branches))
      (is (contains? reachable :crdt.head/main))
      (is (contains? reachable :crdt.head/fork))
      (is (contains? reachable root-main))
      (is (contains? reachable root-fork))
      (is (contains? reachable leaf-shared) "the shared leaf is reached once")
      (is (contains? reachable leaf-a))
      (is (contains? reachable leaf-b)))))

(deftest crdt-walk-excludes-orphans
  (testing "an unreferenced block is pruned (reachability, not k/keys)"
    (let [{:keys [store]} (build-crdt-store!)
          reachable (<!! (crdt/crdt-walk-fn store {}))
          all-keys (set (<!! (k/keys store)))]
      (is (< (count reachable) (count all-keys))))))

(deftest crdt-sync-opts-bundles-walker-and-gate
  (testing "crdt-sync-opts = the crdt walker + keyword-last fetch-gate"
    (let [opts (crdt/crdt-sync-opts)]
      (is (fn? (:walk-fn opts)))
      (is (= pss/keyword-last (:key-sort-fn opts)))
      (is (= 1 (pss/keyword-last :crdt.head/main)) "mutable pointer sorts last")
      (is (= 0 (pss/keyword-last (random-uuid))) "content block sorts first"))))
