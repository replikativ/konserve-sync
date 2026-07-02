(ns konserve-sync.walkers.yggdrasil-registry-test
  "Tests for the registry subscriber-side projection. The registry is a 2P-Set
   (`:crdt.head/main {:adds :removals}`); its sync/walk is the generic crdt walker
   (see crdt-test). Here we only test the read-out: live = adds − removals,
   shaped as RegistryEntry maps, + latest-by-system-branch. The store is
   hand-built to the on-disk contract (no yggdrasil dependency)."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!!]]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve-sync.walkers.yggdrasil-registry :as reg]))

(defn- entry [sys branch snap phys log]
  {:snapshot-id snap :system-id sys :branch-name branch
   :hlc {:physical phys :logical log}
   :content-hash nil :parent-ids #{} :metadata {}})

(defn- build-registry-store!
  "A memory store shaped like a 2P-Set registry: an :adds tree (one branch over
   two leaves) and a :removals tree (one leaf tombstoning e-b), under
   :crdt.head/main {:adds :removals}."
  []
  (let [store (<!! (new-mem-store))
        e-a  (entry "kb-1" "main" "snap-a" 100 0)
        e-b  (entry "msgs-1" "main" "snap-b" 110 0)
        e-b2 (entry "msgs-1" "main" "snap-b2" 120 0)
        e-c  (entry "kb-1" "fork" "snap-c" 130 0)
        leaf1 (random-uuid) leaf2 (random-uuid) adds-root (random-uuid)
        rem-leaf (random-uuid)]
    (<!! (k/assoc store leaf1 {:level 0 :keys [e-a e-b]} {:sync? false}))
    (<!! (k/assoc store leaf2 {:level 0 :keys [e-b2 e-c]} {:sync? false}))
    (<!! (k/assoc store adds-root {:level 1 :keys [] :addresses [leaf1 leaf2]} {:sync? false}))
    ;; removals tombstones e-b (deregistered)
    (<!! (k/assoc store rem-leaf {:level 0 :keys [e-b]} {:sync? false}))
    (<!! (k/assoc store :crdt.head/main {:adds adds-root :removals rem-leaf} {:sync? false}))
    (<!! (k/assoc store :crdt/freed {} {:sync? false}))
    {:store store :live [e-a e-b2 e-c] :removed [e-b]}))

(deftest read-registry-entries-is-live-set
  (testing "read-registry-entries returns adds − removals"
    (let [{:keys [store live removed]} (build-registry-store!)
          read-entries (<!! (reg/read-registry-entries store))]
      (is (= (set live) (set read-entries)) "live = adds minus tombstoned")
      (is (not-any? (set read-entries) removed) "the deregistered entry is gone"))))

(deftest test-latest-by-system-branch
  (testing "projection keeps the greatest-HLC LIVE entry per [system branch]"
    (let [{:keys [store]} (build-registry-store!)
          live (<!! (reg/read-registry-entries store))
          target (reg/latest-by-system-branch live)]
      (is (= #{["kb-1" "main"] ["msgs-1" "main"] ["kb-1" "fork"]}
             (set (keys target))))
      ;; msgs-1/main: snap-b was deregistered, so the live head is snap-b2
      (is (= "snap-b2" (:snapshot-id (get target ["msgs-1" "main"]))))
      (is (= "snap-a" (:snapshot-id (get target ["kb-1" "main"]))))
      (is (= "snap-c" (:snapshot-id (get target ["kb-1" "fork"])))))))
