(ns konserve-sync.walkers.yggdrasil-registry-test
  "Tests for the yggdrasil registry walker.

   The store is hand-built to the exact on-disk contract of
   `yggdrasil.storage/KonserveStorage` (plain-map PSS nodes + `:registry/roots`),
   so the test is self-contained (no yggdrasil dependency) and pins the format
   the walker relies on. Real-yggdrasil coverage follows in the workspace-peer
   integration test."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!!]]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve-sync.walkers.yggdrasil-registry :as reg]))

(defn- entry [sys branch snap phys log]
  {:snapshot-id snap
   :system-id sys
   :branch-name branch
   :hlc {:physical phys :logical log}
   :content-hash nil
   :parent-ids #{}
   :metadata {}})

(defn- build-registry-store!
  "Construct a memory store shaped like a yggdrasil registry: a 2-level PSS
   (one branch root over two leaves) plus the `:registry/roots` pointer.
   Returns {:store s :root root :leaf1 a :leaf2 b :entries [...]}."
  []
  (let [store (<!! (new-mem-store))
        leaf1 (random-uuid)
        leaf2 (random-uuid)
        root (random-uuid)
        e-a (entry "kb-1" "main" "snap-a" 100 0)
        e-b (entry "msgs-1" "main" "snap-b" 110 0)
        ;; same [system branch] as e-b but newer — exercises latest-by
        e-b2 (entry "msgs-1" "main" "snap-b2" 120 0)
        e-c (entry "kb-1" "fork" "snap-c" 130 0)]
    (<!! (k/assoc store leaf1 {:level 0 :keys [e-a e-b]} {:sync? false}))
    (<!! (k/assoc store leaf2 {:level 0 :keys [e-b2 e-c]} {:sync? false}))
    (<!! (k/assoc store root {:level 1 :keys [] :addresses [leaf1 leaf2]} {:sync? false}))
    (<!! (k/assoc store :registry/roots {:tsbs root} {:sync? false}))
    (<!! (k/assoc store :registry/freed {} {:sync? false}))
    ;; an unreferenced orphan block — must NOT be walked
    (<!! (k/assoc store (random-uuid) {:level 0 :keys []} {:sync? false}))
    {:store store :root root :leaf1 leaf1 :leaf2 leaf2
     :entries [e-a e-b e-b2 e-c]}))

(deftest test-registry-walk-reaches-all-nodes
  (testing "walker reaches every PSS node from the root + the well-known pointers"
    (let [{:keys [store root leaf1 leaf2]} (build-registry-store!)
          reachable (<!! (reg/registry-walk-fn store {}))]
      (is (contains? reachable :registry/roots))
      (is (contains? reachable :registry/freed))
      (is (contains? reachable root))
      (is (contains? reachable leaf1))
      (is (contains? reachable leaf2)))))

(deftest test-registry-walk-excludes-orphans
  (testing "an unreferenced block is NOT in the walk set (reachability, not k/keys)"
    (let [{:keys [store]} (build-registry-store!)
          reachable (<!! (reg/registry-walk-fn store {}))
          all-keys (set (<!! (k/keys store)))]
      ;; the orphan block makes all-keys strictly larger than the reachable set
      (is (< (count reachable) (count all-keys))
          "reachability prunes the orphan that a naive key-dump would include"))))

(deftest test-read-registry-entries
  (testing "read-registry-entries pulls every entry out of the tree"
    (let [{:keys [store entries]} (build-registry-store!)
          read-entries (<!! (reg/read-registry-entries store))]
      (is (= (count entries) (count read-entries)))
      (is (= (set entries) (set read-entries))))))

(deftest test-latest-by-system-branch
  (testing "projection keeps the greatest-HLC entry per [system branch]"
    (let [{:keys [store]} (build-registry-store!)
          read-entries (<!! (reg/read-registry-entries store))
          target (reg/latest-by-system-branch read-entries)]
      (is (= #{["kb-1" "main"] ["msgs-1" "main"] ["kb-1" "fork"]}
             (set (keys target))))
      (testing "msgs-1/main resolves to the newer snap-b2, not snap-b"
        (is (= "snap-b2" (:snapshot-id (get target ["msgs-1" "main"])))))
      (is (= "snap-a" (:snapshot-id (get target ["kb-1" "main"]))))
      (is (= "snap-c" (:snapshot-id (get target ["kb-1" "fork"])))))))
