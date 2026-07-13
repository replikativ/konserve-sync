(ns konserve-sync.walkers.datahike-test
  "Tests for the Datahike walker function."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :refer [<!!]]
            [datahike.api :as d]
            [konserve.core :as k]
            [konserve-sync.walkers.datahike :as walker]
            [clojure.set]))

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(def test-schema
  [{:db/ident :name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :age
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}])

(def test-dir "/tmp/konserve-sync-walker-test")

(defn clean-test-dir []
  (let [dir (java.io.File. test-dir)]
    (when (.exists dir)
      (doseq [f (reverse (file-seq dir))]
        (.delete f)))))

(defn with-clean-dir [f]
  (clean-test-dir)
  (try
    (f)
    (finally
      (clean-test-dir))))

(use-fixtures :each with-clean-dir)

;; =============================================================================
;; Walker Tests
;; =============================================================================

(deftest test-datahike-walk-fn-basic
  (testing "walker discovers all BTSet addresses from datahike store"
    (let [cfg {:store {:backend :file
                       :id (java.util.UUID/randomUUID)
                       :path test-dir}
               :schema-flexibility :write
               :keep-history? false}
          _ (d/create-database cfg)
          conn (d/connect cfg)
          _ (d/transact conn {:tx-data test-schema})
          _ (d/transact conn {:tx-data [{:name "Alice" :age 30}
                                        {:name "Bob" :age 25}
                                        {:name "Charlie" :age 35}]})
          store (-> conn d/db :store)

          ;; Walk returns channel
          reachable-keys (set (<!! (walker/datahike-walk-fn store {})))]

      ;; Should include :db
      (is (contains? (set reachable-keys) :db))

      ;; Should find BTSet addresses (UUIDs)
      (let [uuid-keys (filter uuid? reachable-keys)]
        (is (pos? (count uuid-keys))
            "Should find BTSet node addresses"))

      ;; Total keys should be much smaller than k/keys
      (let [all-keys (<!! (k/keys store))]
        (is (<= (count reachable-keys) (count all-keys))
            "Reachable keys should be subset of all keys"))

      (d/release conn))))

(deftest test-datahike-walk-fn-with-history
  (testing "walker discovers temporal index addresses when keep-history? is true"
    (let [cfg {:store {:backend :file
                       :id (java.util.UUID/randomUUID)
                       :path test-dir}
               :schema-flexibility :write
               :keep-history? true}
          _ (d/create-database cfg)
          conn (d/connect cfg)
          _ (d/transact conn {:tx-data test-schema})
          _ (d/transact conn {:tx-data [{:name "Alice" :age 30}]})
          ;; Make a second transaction to populate temporal indices
          _ (d/transact conn {:tx-data [{:name "Bob" :age 25}]})
          store (-> conn d/db :store)

          reachable-keys (set (<!! (walker/datahike-walk-fn store {})))]

      ;; With history, we should have more keys due to temporal indices
      (is (contains? (set reachable-keys) :db))
      (is (pos? (count (filter uuid? reachable-keys))))

      (d/release conn))))

(deftest test-make-tiered-walk-fn
  (testing "make-tiered-walk-fn creates proper wrapper"
    (let [cfg {:store {:backend :file
                       :id (java.util.UUID/randomUUID)
                       :path test-dir}
               :schema-flexibility :write
               :keep-history? false}
          _ (d/create-database cfg)
          conn (d/connect cfg)
          _ (d/transact conn {:tx-data test-schema})
          _ (d/transact conn {:tx-data [{:name "Alice" :age 30}]})
          store (-> conn d/db :store)

          ;; Create the tiered walk function
          tiered-walk-fn (walker/make-tiered-walk-fn)

          ;; Call it with the expected signature
          reachable-keys (<!! (tiered-walk-fn store {:db "some-db-value"} {}))]

      (is (contains? (set reachable-keys) :db))
      (is (pos? (count (filter uuid? reachable-keys))))

      (d/release conn))))

;; =============================================================================
;; Fork / multi-branch reachability + sync-order (the "reflect a fork" keystone)
;; =============================================================================

(deftest test-walk-includes-fork-branch
  (testing "walker reaches a FORK branch's head + blocks (not just trunk :db),
            and content keys sort before mutable branch pointers"
    (let [cfg {:store {:backend :file :id (java.util.UUID/randomUUID) :path test-dir}
               :schema-flexibility :write
               :keep-history? true        ; branching needs history
               :branch-history? true}
          _ (d/create-database cfg)
          conn (d/connect cfg)
          _ (d/transact conn {:tx-data test-schema})
          _ (d/transact conn {:tx-data [{:name "Trunk" :age 1}]})
          _ (d/branch! conn :db :fork)
          fork-conn (d/connect (assoc cfg :branch :fork))
          _ (d/transact fork-conn {:tx-data [{:name "ForkOnly" :age 99}]})
          store (-> conn d/db :store)
          reachable (set (<!! (walker/datahike-walk-fn store {})))]

      (testing "the fork head pointer IS reached (the bug this fixes)"
        (is (contains? reachable :fork)
            "fork branch HEAD key must be walked so it propagates to subscribers")
        (is (contains? reachable :branches))
        (is (contains? (<!! (k/get store :branches)) :fork)))

      (testing "fork's content blocks are reachable (so branch-as-db works remotely)"
        (let [fork-db     (<!! (k/get store :fork))
              fork-blocks (<!! (#'walker/walk-stored-db-async store fork-db))]
          (is (seq fork-blocks))
          (is (clojure.set/subset? fork-blocks reachable)
              "every reachable block of the fork must be in the walk set")))

      (testing "key-sort invariant: content (uuid) keys precede mutable pointers (keywords)"
        (let [key-sort-fn   (fn [k] (if (keyword? k) 1 0))
              sorted        (sort-by key-sort-fn reachable)
              last-uuid-idx (->> sorted (keep-indexed (fn [i k] (when-not (keyword? k) i))) last)
              first-kw-idx  (->> sorted (keep-indexed (fn [i k] (when (keyword? k) i))) first)]
          (when (and last-uuid-idx first-kw-idx)
            (is (< last-uuid-idx first-kw-idx)
                "every content block sorts before every branch-pointer keyword")))))))

(deftest test-walk-covers-multi-level-tree
  ;; The tests above transact a handful of datoms — a single leaf per index, so
  ;; branch-node recursion is NEVER exercised. This one transacts enough to force
  ;; a multi-level BTSet (a root BRANCH with leaf children). Regression guard:
  ;; a walk that fails to descend into branches (e.g. reading a persistent-
  ;; sorted-set field the library later renamed and swallowing the error) ships
  ;; only the roots, so a subscriber's replica points at index roots whose child
  ;; nodes it never received — the db is visible but not backed by its indices.
  (testing "walk of a multi-level index is closed under child reachability"
    (let [cfg {:store {:backend :file :id (java.util.UUID/randomUUID) :path test-dir}
               :schema-flexibility :write
               :keep-history? false}
          _ (d/create-database cfg)
          conn (d/connect cfg)
          _ (d/transact conn {:tx-data test-schema})
          ;; >512 datoms per index forces at least one branch level
          _ (d/transact conn {:tx-data (mapv (fn [i] {:name (str "e" i) :age i})
                                             (range 1500))})
          store (-> conn d/db :store)
          reachable (set (<!! (walker/datahike-walk-fn store {})))]

      (is (> (count (filter uuid? reachable)) 8)
          "walk must descend past the index roots into leaf nodes")

      ;; Closure: every child address of every reachable BRANCH must itself be in
      ;; the walk set. Under the truncation bug the roots' children are absent,
      ;; so this fails; a correct recursive walk is closed. Uses the walker's own
      ;; version-resolved accessor so the check holds across pss layouts.
      (doseq [k reachable :when (uuid? k)]
        (let [node (<!! (k/get store k))]
          (doseq [addr (remove nil? (seq (#'walker/get-node-addresses node)))]
            (is (contains? reachable addr)
                (str "branch " k " child " addr " missing from walk")))))

      (d/release conn))))

(deftest test-walk-covers-fused-index-roots
  ;; Regression: with datahike's `:fuse-index-roots? true` the index ROOT node is
  ;; inlined into the db record and NEVER written as its own konserve object. The
  ;; walker fetched each root by address — `(k/get store root-addr)` → nil — so it
  ;; dead-ended AT the root: it emitted a root address that does not exist in the
  ;; store and discovered none of the subtree below it.
  ;;
  ;; Effect: a subscriber syncing a fused store received ~nothing (a handful of
  ;; keys, two of them dangling) and read-through'd to the backend forever. Silent
  ;; — the head still applied, queries still worked, they were just never local.
  ;; Datahike's own GC handles fusion by seeding the inlined root before marking;
  ;; this walker has to agree with it about what is reachable, or sync and GC
  ;; disagree.
  ;;
  ;; The multi-level test above does NOT catch this: it leaves fusion off.
  (testing "walk descends through an INLINED root and emits no dangling addresses"
    (let [cfg {:store {:backend :file :id (java.util.UUID/randomUUID) :path test-dir}
               :schema-flexibility :write
               :keep-history? false
               :fuse-index-roots? true}          ;; <- the whole point
          _ (d/create-database cfg)
          conn (d/connect cfg)
          _ (d/transact conn {:tx-data test-schema})
          _ (d/transact conn {:tx-data (mapv (fn [i] {:name (str "e" i) :age i})
                                             (range 1500))})
          store (-> conn d/db :store)
          reachable (set (<!! (walker/datahike-walk-fn store {})))
          node-keys (filter uuid? reachable)]

      (is (> (count node-keys) 8)
          "walk must descend past the INLINED root into the child nodes")

      ;; Every address the walk emits must exist. Under the bug the fused root's
      ;; address was emitted while no such object was ever written — a subscriber
      ;; asks for it and gets nothing.
      (doseq [k node-keys]
        (is (some? (<!! (k/get store k)))
            (str "walk emitted " k " but no such object exists in the store")))

      ;; Closure, as in the unfused case.
      (doseq [k node-keys]
        (let [node (<!! (k/get store k))]
          (doseq [addr (remove nil? (seq (#'walker/get-node-addresses node)))]
            (is (contains? reachable addr)
                (str "branch " k " child " addr " missing from walk")))))

      (d/release conn))))

(deftest test-walk-branch-scope
  (testing ":branches opt scopes which branches' HEADs + nodes are walked;
            :branches set is always emitted"
    (let [cfg {:store {:backend :file :id (java.util.UUID/randomUUID) :path test-dir}
               :schema-flexibility :write
               :keep-history? true
               :branch-history? true}
          _ (d/create-database cfg)
          conn (d/connect cfg)
          _ (d/transact conn {:tx-data test-schema})
          _ (d/transact conn {:tx-data [{:name "Trunk" :age 1}]})
          _ (d/branch! conn :db :fork)
          fork-conn (d/connect (assoc cfg :branch :fork))
          _ (d/transact fork-conn {:tx-data [{:name "ForkOnly" :age 99}]})
          store (-> conn d/db :store)
          fork-blocks  (<!! (#'walker/walk-stored-db-async store (<!! (k/get store :fork))))
          trunk-blocks (<!! (#'walker/walk-stored-db-async store (<!! (k/get store :db))))
          ;; the fork shares most nodes with trunk (CoW); only this delta is fork-only
          fork-only    (clojure.set/difference fork-blocks trunk-blocks)]

      (testing ":all (default) reaches the fork head + blocks"
        (let [all (set (<!! (walker/datahike-walk-fn store {:branches :all})))]
          (is (contains? all :fork))
          (is (clojure.set/subset? fork-blocks all))))

      (testing ":trunk reaches :db + :branches but NOT the fork head/fork-only nodes"
        (let [trunk (set (<!! (walker/datahike-walk-fn store {:branches :trunk})))]
          (is (contains? trunk :db) "trunk head present")
          (is (contains? trunk :branches) ":branches set always emitted (subscriber learns names)")
          (is (not (contains? trunk :fork)) "fork head NOT shipped under :trunk scope")
          (is (seq fork-only) "sanity: the fork has at least one node not shared with trunk")
          (is (empty? (clojure.set/intersection fork-only trunk))
              "no fork-ONLY nodes shipped under :trunk scope (shared CoW nodes are fine)")))

      (testing "an explicit branch keyword scopes to that branch (intersected with real branches)"
        (let [only-fork (set (<!! (walker/datahike-walk-fn store {:branches :fork})))]
          (is (contains? only-fork :fork))
          (is (clojure.set/subset? fork-blocks only-fork))
          (is (contains? only-fork :branches)))
        (let [bogus (set (<!! (walker/datahike-walk-fn store {:branches :does-not-exist})))]
          (is (= #{:branches} bogus) "unknown branch ⇒ only the :branches marker"))))))

(deftest test-walk-branch-subset
  (testing "a COLL of branches walks exactly that subset (more than one, fewer than all)"
    (let [cfg {:store {:backend :file :id (java.util.UUID/randomUUID) :path test-dir}
               :schema-flexibility :write
               :keep-history? true
               :branch-history? true}
          _ (d/create-database cfg)
          conn (d/connect cfg)
          _ (d/transact conn {:tx-data test-schema})
          _ (d/transact conn {:tx-data [{:name "Trunk" :age 1}]})
          _ (d/branch! conn :db :fork-a)
          _ (d/branch! conn :db :fork-b)
          ca (d/connect (assoc cfg :branch :fork-a))
          cb (d/connect (assoc cfg :branch :fork-b))
          _ (d/transact ca {:tx-data [{:name "OnlyA" :age 11}]})
          _ (d/transact cb {:tx-data [{:name "OnlyB" :age 22}]})
          store (-> conn d/db :store)
          trunk-blocks (<!! (#'walker/walk-stored-db-async store (<!! (k/get store :db))))
          a-only (clojure.set/difference
                  (<!! (#'walker/walk-stored-db-async store (<!! (k/get store :fork-a))))
                  trunk-blocks)
          b-only (clojure.set/difference
                  (<!! (#'walker/walk-stored-db-async store (<!! (k/get store :fork-b))))
                  trunk-blocks)
          ;; subset = trunk + fork-a, but NOT fork-b
          walked (set (<!! (walker/datahike-walk-fn store {:branches #{:db :fork-a}})))]
      (is (seq a-only)) (is (seq b-only))
      (is (contains? walked :db))      (is (contains? walked :fork-a))
      (is (not (contains? walked :fork-b)) "out-of-subset branch HEAD not shipped")
      (is (contains? walked :branches) "all three branch names still learnable")
      (is (= #{:db :fork-a :fork-b} (<!! (k/get store :branches))))
      (is (clojure.set/subset? a-only walked) "in-subset fork's own nodes shipped")
      (is (empty? (clojure.set/intersection b-only walked))
          "out-of-subset fork's own nodes NOT shipped"))))

(deftest test-walk-emits-pointer-cells-last
  (testing "walk order: index nodes first, MUTABLE pointer cells last"
    (let [cfg {:store {:backend :file
                       :id (java.util.UUID/randomUUID)
                       :path test-dir}
               :schema-flexibility :write
               :keep-history? false}
          _ (d/create-database cfg)
          conn (d/connect cfg)
          _ (d/transact conn {:tx-data test-schema})
          _ (d/transact conn {:tx-data (vec (for [i (range 500)]
                                              {:name (str "n" i) :age (mod i 60)}))})
          store (-> conn d/db :store)
          ks (<!! (walker/datahike-walk-fn store {}))]

      (is (vector? ks) "walk returns an ordered vector, not a set")
      (is (= (count ks) (count (distinct ks))) "and it is deduped")

      ;; the invariant the sync relies on: a subscriber applies keys in this order, so a
      ;; branch HEAD is only ever written after every node it references. Nothing
      ;; downstream needs to infer that from the shape of the key.
      (let [last-node    (->> ks (keep-indexed #(when (uuid? %2) %1)) (reduce max -1))
            first-cell   (->> ks (keep-indexed #(when (keyword? %2) %1)) (reduce min Long/MAX_VALUE))]
        (is (pos? (inc last-node)) "there are index nodes to order")
        (is (< last-node first-cell)
            "every index node precedes every mutable pointer cell"))

      ;; and the pointers themselves are all there, at the tail
      (let [tail (set (drop-while uuid? ks))]
        (is (contains? tail :branches))
        (is (contains? tail :db)))

      (d/release conn))))
