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
          reachable-keys (<!! (walker/datahike-walk-fn store {}))]

      ;; Should include :db
      (is (contains? reachable-keys :db))

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

          reachable-keys (<!! (walker/datahike-walk-fn store {}))]

      ;; With history, we should have more keys due to temporal indices
      (is (contains? reachable-keys :db))
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

      (is (contains? reachable-keys :db))
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
          reachable (<!! (walker/datahike-walk-fn store {}))]

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
        (let [all (<!! (walker/datahike-walk-fn store {:branches :all}))]
          (is (contains? all :fork))
          (is (clojure.set/subset? fork-blocks all))))

      (testing ":trunk reaches :db + :branches but NOT the fork head/fork-only nodes"
        (let [trunk (<!! (walker/datahike-walk-fn store {:branches :trunk}))]
          (is (contains? trunk :db) "trunk head present")
          (is (contains? trunk :branches) ":branches set always emitted (subscriber learns names)")
          (is (not (contains? trunk :fork)) "fork head NOT shipped under :trunk scope")
          (is (seq fork-only) "sanity: the fork has at least one node not shared with trunk")
          (is (empty? (clojure.set/intersection fork-only trunk))
              "no fork-ONLY nodes shipped under :trunk scope (shared CoW nodes are fine)")))

      (testing "an explicit branch keyword scopes to that branch (intersected with real branches)"
        (let [only-fork (<!! (walker/datahike-walk-fn store {:branches :fork}))]
          (is (contains? only-fork :fork))
          (is (clojure.set/subset? fork-blocks only-fork))
          (is (contains? only-fork :branches)))
        (let [bogus (<!! (walker/datahike-walk-fn store {:branches :does-not-exist}))]
          (is (= #{:branches} bogus) "unknown branch ⇒ only the :branches marker"))))))
