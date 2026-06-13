(ns konserve-sync.walkers.datahike
  "Cross-platform walker for Datahike stores.

   Discovers all BTSet node addresses reachable from the :db root.
   This enables reachability-based sync instead of syncing ALL keys,
   which is critical for performance with large Datahike stores.

   Usage:
   - Server: Pass `datahike-walk-fn` to `register-store!` via :walk-fn option
   - Client: Pass `datahike-walk-fn` to `perform-walk-sync` in tiered store

   Note: Requires datahike and persistent-sorted-set on classpath.
   These are optional dependencies of konserve-sync."
  (:require [konserve.core :as k]
            [org.replikativ.persistent-sorted-set.arrays :as arrays]
            #?@(:clj [[superv.async :refer [go-try- <?-]]]
                :cljs [[clojure.core.async :refer [<!]]]))
  #?(:cljs (:require-macros [clojure.core.async :refer [go]]
                            [superv.async :refer [go-try- <?-]]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set PersistentSortedSet])))

;; ============================================================================
;; BTSet Address Collection (Recursive)
;; ============================================================================

(defn- get-node-addresses
  "Get addresses array from a BTSet node (branch nodes only).
   Returns nil for leaf nodes."
  [node]
  #?(:clj (try
            ;; Branch nodes have _addresses field
            (when-let [addresses (.-_addresses node)]
              addresses)
            (catch Exception _ nil))
     :cljs (.-addresses node)))

(defn- walk-node-async
  "Recursively walk a BTSet node and collect child addresses.
   Fetches each node from the store to discover its children's addresses."
  [store node collected]
  (go-try-
   (when node
      ;; Branch nodes have addresses array pointing to children
     (when-let [addresses (get-node-addresses node)]
       (when (pos? (arrays/alength addresses))
         (loop [i 0]
           (when (< i (arrays/alength addresses))
             (when-let [addr (arrays/aget addresses i)]
               (swap! collected conj addr)
                ;; Recursively walk child node
               (let [child (<?- (k/get store addr))]
                 (<?- (walk-node-async store child collected))))
             (recur (inc i)))))))))

(defn- get-btset-address
  "Extract root address from a BTSet or deferred index format.
   Handles both actual PersistentSortedSet/BTSet instances AND
   deferred format maps {:deferred-type :persistent-sorted-set :address ...}
   returned by Fressian handlers."
  [btset]
  (cond
    ;; Deferred format from Fressian deserialization
    (and (map? btset) (= (:deferred-type btset) :persistent-sorted-set))
    (:address btset)

    ;; Actual PersistentSortedSet (CLJ) or BTSet (CLJS)
    #?(:clj (instance? PersistentSortedSet btset)
       :cljs true)
    #?(:clj (.-_address ^PersistentSortedSet btset)
       :cljs (.-address btset))

    :else nil))

(defn- collect-btset-addresses-async
  "Collect all addresses from a BTSet by walking the tree.
   Fetches nodes from the store to discover all nested addresses."
  [store btset]
  (go-try-
   (let [collected (atom #{})
         root-addr (get-btset-address btset)]
     (when root-addr
       (swap! collected conj root-addr)
        ;; Fetch root node and walk its children
       (let [root-node (<?- (k/get store root-addr))]
         (<?- (walk-node-async store root-node collected))))
     @collected)))

;; ============================================================================
;; Main Walker Function
;; ============================================================================

(defn- walk-stored-db-async
  "Collect all BTSet node addresses reachable from a single stored-db value.
   Returns a channel that delivers a set of konserve keys (BTSet addresses
   + the schema-meta key, if any). Used by `datahike-walk-fn` to traverse
   one branch at a time."
  [store stored-db]
  (go-try-
   (let [collected (atom #{})]
     (when stored-db
       ;; Main indices
       (doseq [idx-key [:eavt-key :aevt-key :avet-key]]
         (when-let [btset (get stored-db idx-key)]
           (let [addrs (<?- (collect-btset-addresses-async store btset))]
             (swap! collected into addrs))))
       ;; Temporal indices (when :keep-history?)
       (doseq [idx-key [:temporal-eavt-key :temporal-aevt-key :temporal-avet-key]]
         (when-let [btset (get stored-db idx-key)]
           (let [addrs (<?- (collect-btset-addresses-async store btset))]
             (swap! collected into addrs))))
       ;; Schema meta
       (when-let [schema-key (get stored-db :schema-meta-key)]
         (swap! collected conj schema-key)))
     @collected)))

(defn datahike-walk-fn
  "Walker function for konserve-sync that discovers all keys reachable
   from every branch in a Datahike store.

   Arguments:
   - store: The konserve store containing Datahike data
   - opts: Options map (unused, kept for API compatibility)

   Returns:
   - Channel yielding set of reachable keys

   The returned set includes:
   - `:branches` (the set of branch names)
   - Every branch HEAD key (e.g. `:db`, `:db-foo`, `:db-bar` …) read from
     `:branches` and falling back to `:db` if the set is missing.
   - All BTSet node addresses reachable from each branch's
     eavt/aevt/avet indices (live + temporal).
   - Every branch's `:schema-meta-key`.

   Earlier versions walked only the `:db` (trunk) root, which meant
   non-trunk branches' HEADs never propagated to subscribed peers.
   Clients could call `(d/branches conn)` (because the `:branches` set
   itself was published through the write-hook on subsequent server
   writes) but `(d/branch-as-db conn :db-foo)` returned nil because the
   actual HEAD entry at key `:db-foo` was missing locally. Walking
   every branch on initial sync closes that gap.

   Usage with register-store!:
     (sync/register-store! ctx store config {:walk-fn datahike-walk-fn})

   Usage with perform-walk-sync (client):
     (tiered/perform-walk-sync frontend backend [:db]
       (fn [store root-values opts]
         (datahike-walk-fn store opts))
       opts)"
  [store _opts]
  (go-try-
   (let [;; Read the branch set, falling back to {:db} if absent so a
         ;; fresh store still walks trunk before `:branches` has ever
         ;; been initialized.
         branches (or (<?- (k/get store :branches))
                      #{:db})
         collected (atom (conj (set branches) :branches))]
     (loop [bs (seq branches)]
       (when bs
         (let [branch-key (first bs)]
           (when-let [stored-db (<?- (k/get store branch-key))]
             (let [addrs (<?- (walk-stored-db-async store stored-db))]
               (swap! collected into addrs)))
           (recur (next bs)))))
     @collected)))

;; ============================================================================
;; Convenience wrapper for tiered store walk-sync
;; ============================================================================

(defn make-tiered-walk-fn
  "Create a walk function suitable for tiered/perform-walk-sync.

   The tiered walk-sync expects: (fn [backend-store root-values opts] -> channel)
   This wrapper adapts datahike-walk-fn to that signature.

   Usage:
     (tiered/perform-walk-sync frontend backend [:db]
       (walkers/make-tiered-walk-fn)
       {:sync? false})"
  []
  (fn [backend-store _root-values opts]
    ;; root-values already has :db, but we re-fetch to ensure we walk
    ;; the BTSet structure stored in backend
    (datahike-walk-fn backend-store opts)))
