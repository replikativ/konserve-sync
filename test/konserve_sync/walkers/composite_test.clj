(ns konserve-sync.walkers.composite-test
  "Tests for the co-located composite walker. The store is hand-built to the
   on-disk contract of a yggdrasil CompositeSystem that hosts its durable-CRDT
   subs in ONE store: a composite index tree under `:composite/root`, two subs
   each under their own `[:crdt/roots id]` cell, and the `:composite/subs`
   manifest. Self-contained (no yggdrasil dep); pins the format the walker
   relies on. Real-yggdrasil coverage is the composite sync test in yggdrasil."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!!]]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve-sync.walkers.composite :as cmp]))

(defn- build-composite-store!
  "A memory store shaped like a co-located composite:
     :composite/root → index-root over index-leaf
     [:crdt/roots \"a\"] → {:main a-root over a-leaf}
     [:crdt/roots \"b\"] → {:main b-root over b-leaf (sharing index-leaf? no)}
   plus :composite/freed / :composite/subs and an orphan block."
  []
  (let [store (<!! (new-mem-store))
        index-leaf (random-uuid) index-root (random-uuid)
        a-leaf (random-uuid) a-root (random-uuid)
        b-leaf (random-uuid) b-root (random-uuid)]
    (<!! (k/assoc store index-leaf {:level 0 :keys [{:composite-snap-id "s1"}]} {:sync? false}))
    (<!! (k/assoc store index-root {:level 1 :keys [] :addresses [index-leaf]} {:sync? false}))
    (<!! (k/assoc store a-leaf {:level 0 :keys [:a1 :a2]} {:sync? false}))
    (<!! (k/assoc store a-root {:level 1 :keys [] :addresses [a-leaf]} {:sync? false}))
    (<!! (k/assoc store b-leaf {:level 0 :keys [:b1]} {:sync? false}))
    (<!! (k/assoc store b-root {:level 1 :keys [] :addresses [b-leaf]} {:sync? false}))
    (<!! (k/assoc store :composite/root index-root {:sync? false}))
    (<!! (k/assoc store :composite/freed {} {:sync? false}))
    (<!! (k/assoc store (keyword "crdt.head" "a::main") {:root a-root} {:sync? false}))
    (<!! (k/assoc store :crdt.branches/a #{:main} {:sync? false}))
    (<!! (k/assoc store (keyword "crdt.head" "b::main") {:root b-root} {:sync? false}))
    (<!! (k/assoc store :crdt.branches/b #{:main} {:sync? false}))
    (<!! (k/assoc store :composite/subs
                  [{:branches-key :crdt.branches/a :cell-ns "a"}
                   {:branches-key :crdt.branches/b :cell-ns "b"}]
                  {:sync? false}))
    (<!! (k/assoc store (random-uuid) {:level 0 :keys [:orphan]} {:sync? false}))
    {:store store :index-root index-root :index-leaf index-leaf
     :a-root a-root :a-leaf a-leaf :b-root b-root :b-leaf b-leaf}))

(deftest composite-walk-reaches-index-and-every-sub
  (testing "walker reaches the index tree + every co-located sub's tree + pointers"
    (let [{:keys [store index-root index-leaf a-root a-leaf b-root b-leaf]} (build-composite-store!)
          reachable (<!! (cmp/composite-walk-fn store {}))]
      (is (contains? reachable :composite/root))
      (is (contains? reachable :composite/freed))
      (is (contains? reachable :composite/subs))
      (is (contains? reachable :crdt.branches/a))
      (is (contains? reachable :crdt.branches/b))
      (is (contains? reachable (keyword "crdt.head" "a::main")))
      (is (contains? reachable (keyword "crdt.head" "b::main")))
      (is (contains? reachable index-root))
      (is (contains? reachable index-leaf) "composite index node reached")
      (is (contains? reachable a-root))
      (is (contains? reachable a-leaf) "sub a reached via its head cell")
      (is (contains? reachable b-root))
      (is (contains? reachable b-leaf) "sub b reached via its head cell"))))

(deftest composite-walk-excludes-orphans
  (testing "an unreferenced block is pruned (reachability, not k/keys)"
    (let [{:keys [store]} (build-composite-store!)
          reachable (<!! (cmp/composite-walk-fn store {}))
          all-keys (set (<!! (k/keys store)))]
      (is (< (count reachable) (count all-keys))))))

(deftest composite-root-is-the-lone-last-gate
  (testing "3-tier gate: content first, sub pointers next, :composite/root STRICTLY last"
    (is (= 0 (cmp/composite-key-last (random-uuid))) "content node ships first")
    (is (= 1 (cmp/composite-key-last :crdt.branches/a)) "a sub branches cell ships before the gate")
    (is (= 1 (cmp/composite-key-last :composite/freed)) "composite bookkeeping ships before the gate")
    (is (= 2 (cmp/composite-key-last :composite/root)) "the lone causal gate ships last")
    (is (every? #(< (cmp/composite-key-last %) (cmp/composite-key-last :composite/root))
                [(random-uuid) :crdt.branches/a (keyword "crdt.head" "b::main") :composite/freed :composite/subs])
        "every other key precedes :composite/root")))

(deftest composite-sync-opts-bundles-walker-and-gate
  (testing "composite-sync-opts = the composite walker + the :composite/root-last gate"
    (let [opts (cmp/composite-sync-opts)]
      (is (fn? (:walk-fn opts)))
      (is (= cmp/composite-key-last (:key-sort-fn opts))))))
