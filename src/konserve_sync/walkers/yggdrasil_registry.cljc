(ns konserve-sync.walkers.yggdrasil-registry
  "Cross-platform walker for the yggdrasil snapshot-registry konserve store.

   The registry is a persistent-sorted-set (PSS) B-tree whose nodes are
   serialized by `yggdrasil.storage/KonserveStorage` as **plain maps**:

     {:level     <int>
      :keys      [<RegistryEntry-as-map> ...]   ; leaf + branch
      :addresses [<child-address> ...]}         ; branch nodes only

   and whose root address lives at the well-known key

     :registry/roots  →  {:tsbs <root-address>}

   (`:registry/freed` holds GC bookkeeping.)

   Because the on-disk node format is plain data — NOT a Java PSS object the
   way datahike's index blocks are — both walking the tree (for sync) and
   reading the entries out of it (for projection) are pure `k/get` + map
   traversal. That means this namespace needs **no yggdrasil dependency** and
   runs unchanged on a read-only ClojureScript peer: the single-writer server
   owns mutation (which is the only place the Java PSS class is required); a
   subscribing peer only ever reads.

   Usage:
   - Server: pass `registry-walk-fn` to `register-store!` via :walk-fn.
   - Client: pass `registry-walk-fn` to the client's walk-sync; project the
     control-plane target with `read-registry-entries` / `latest-by-system-branch`."
  (:require [konserve.core :as k]
            [konserve-sync.walkers.pss :as pss]
            #?@(:clj [[superv.async :refer [go-try- <?-]]]
                :cljs [[clojure.core.async :refer [<!]]]))
  #?(:cljs (:require-macros [clojure.core.async :refer [go]]
                            [superv.async :refer [go-try- <?-]])))

;; ============================================================================
;; Fetch-gate ordering + register bundle — the registry is one instance of the
;; generic content-addressed-PSS-store walker (konserve-sync.walkers.pss).
;; ============================================================================

(def keyword-last
  "Re-export of the generic PSS fetch-gate (content blocks first, mutable
   keyword pointers last)."
  pss/keyword-last)

(def registry-walk-fn
  "Walker for a yggdrasil registry store: every PSS node reachable from
   `:registry/roots` (a `{:tsbs <root>}` cell) plus the well-known pointers
   `:registry/roots` / `:registry/freed`. Orphan blocks are pruned."
  (pss/make-pss-walk-fn :registry/roots #{:registry/roots :registry/freed}))

(defn registry-sync-opts
  "Options bundle for `register-store!` / `subscribe-store!` on a yggdrasil
   registry store: the reachability walker + the fetch-gate ordering. Merge in
   `:on-key-update` / `:on-complete` as needed:

     (register-store! peer topic (:kv-store registry) (registry-sync-opts))"
  []
  {:walk-fn registry-walk-fn
   :key-sort-fn keyword-last})

;; ============================================================================
;; Entry read-out (for control-plane projection)
;; ============================================================================

(defn- collect-entries-async
  "Walk from `addr` accumulating every node's `:keys` (RegistryEntry maps)
   into the `acc` atom. `seen` guards shared blocks."
  [store addr acc seen]
  (go-try-
   (when (and addr (not (contains? @seen addr)))
     (swap! seen conj addr)
     (let [node (<?- (k/get store addr))]
       (when-let [ks (:keys node)]
         (swap! acc into ks))
       (when-let [addresses (:addresses node)]
         (loop [as (seq addresses)]
           (when as
             (<?- (collect-entries-async store (first as) acc seen))
             (recur (next as)))))))))

(defn read-registry-entries
  "Read every RegistryEntry (as a plain map) out of a (locally-synced)
   registry store by traversing the PSS B-tree.

   Returns a channel yielding a vector of entry maps. Each entry has the
   shape persisted by `yggdrasil.storage/entry->map`:
     {:snapshot-id :system-id :branch-name
      :hlc {:physical :logical} :content-hash :parent-ids :metadata}

   Pure read traversal — safe on a read-only ClojureScript peer."
  [store]
  (go-try-
   (let [acc (atom [])
         seen (atom #{})
         roots (<?- (k/get store :registry/roots))]
     (loop [rs (seq (vals roots))]
       (when rs
         (<?- (collect-entries-async store (first rs) acc seen))
         (recur (next rs))))
     @acc)))

;; ============================================================================
;; Projection helpers (pure)
;; ============================================================================

(defn- hlc->vec
  "Comparable [physical logical] vector for an entry's HLC (nil-safe)."
  [entry]
  (let [{:keys [physical logical]} (:hlc entry)]
    [(or physical 0) (or logical 0)]))

(defn latest-by-system-branch
  "Reduce a seq of registry entry maps to the control-plane target:
     {[system-id branch-name] → latest-entry}
   keeping, per [system branch], the entry with the greatest HLC. This is the
   flat-list equivalent of `yggdrasil.registry/as-of` at T = now and is what a
   subscribing peer projects to learn each system's pinned head."
  [entries]
  (persistent!
   (reduce
    (fn [acc entry]
      (let [kbranch [(:system-id entry) (:branch-name entry)]
            cur (get acc kbranch)]
        (if (or (nil? cur)
                (pos? (compare (hlc->vec entry) (hlc->vec cur))))
          (assoc! acc kbranch entry)
          acc)))
    (transient {})
    entries)))
