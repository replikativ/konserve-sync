(ns konserve-sync.walkers.yggdrasil-registry
  "Subscriber-side PROJECTION of a yggdrasil snapshot-registry konserve store.

   The registry is now just a durable conflict-free system — a content-addressed
   **2P-Set** of RegistryEntry — so it SYNCS through the generic
   `konserve-sync.walkers.crdt` walker (its store uses the `:crdt.head/main` cell
   like any durable CRDT). This namespace no longer defines a walker; it keeps
   only the registry-FLAVORED read-out a subscriber needs to interpret the
   synced 2P-Set as live RegistryEntry maps.

   On-disk shape (serialized by `yggdrasil.storage/KonserveStorage` as plain
   maps — no yggdrasil dependency, runs on a read-only cljs peer):

     :crdt.head/main → {:adds <node> :removals <node>}
     <node>      → {:level :keys [<RegistryEntry-as-map> …] :addresses […]}

   live(entry) ⇔ entry ∈ adds ∧ entry ∉ removals.

   Sync: register the store with `(konserve-sync.walkers.crdt/crdt-sync-opts)`.
   Project: `read-registry-entries` (live entries) → `latest-by-system-branch`."
  (:require [konserve.core :as k]
            #?@(:clj [[superv.async :refer [go-try- <?-]]]
                :cljs [[clojure.core.async :refer [<!]]]))
  #?(:cljs (:require-macros [clojure.core.async :refer [go]]
                            [superv.async :refer [go-try- <?-]])))

;; ============================================================================
;; Entry read-out (2P-Set: live = adds − removals)
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

(defn- collect-from-root [store root]
  (go-try-
   (let [acc (atom []) seen (atom #{})]
     (cond
       (nil? root) nil
       ;; a FUSED root node (inlined in the head cell) is a plain map — process its keys +
       ;; recurse its child ADDRESSES directly (its own address isn't in the store).
       (map? root) (do (when-let [ks (:keys root)] (swap! acc into ks))
                       (loop [as (seq (:addresses root))]
                         (when as
                           (<?- (collect-entries-async store (first as) acc seen))
                           (recur (next as)))))
       ;; a bare address → walk from it.
       :else (<?- (collect-entries-async store root acc seen)))
     @acc)))

(defn read-registry-entries
  "Read every LIVE RegistryEntry (as a plain map) out of a (locally-synced)
   registry store by traversing both halves of the 2P-Set and subtracting:
   live = adds − removals.

   Returns a channel yielding a vector of entry maps, each shaped as
   `yggdrasil.storage/entry->map`:
     {:snapshot-id :system-id :branch-name
      :hlc {:physical :logical} :content-hash :parent-ids :metadata}

   Pure read traversal — safe on a read-only ClojureScript peer."
  [store]
  (go-try-
   (let [head    (<?- (k/get store :crdt.head/main))
         adds    (<?- (collect-from-root store (:adds head)))
         removed (set (<?- (collect-from-root store (:removals head))))]
     (into [] (remove removed) adds))))

;; ============================================================================
;; Projection helpers (pure)
;; ============================================================================

(defn- hlc->vec
  "Comparable [physical logical] vector for an entry's HLC (nil-safe)."
  [entry]
  (let [{:keys [physical logical]} (:hlc entry)]
    [(or physical 0) (or logical 0)]))

(defn latest-by-system-branch
  "Reduce a seq of LIVE registry entry maps to the control-plane target:
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
