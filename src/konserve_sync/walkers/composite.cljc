(ns konserve-sync.walkers.composite
  "konserve-sync walker for a yggdrasil CompositeSystem store that hosts its
   durable-CRDT sub-systems in ONE store (the lone-causal-root, browser/local-
   first form). Such a store holds, all content-addressed plain-map PSS nodes:

     <uuid>            → composite index node OR sub-system node {:level :keys …}
     :composite/root   → the composite index ROOT address (a bare address)
     :composite/freed  → composite GC bookkeeping
     :composite/subs   → [{:roots-key :freed-key} …]  manifest of co-located subs
     [:crdt/roots id]  → {<branch> <root>}  per co-located sub
     [:crdt/freed id]  → {<address> <ts>}   per co-located sub

   The walk ships every node reachable from the composite index root AND from
   every sub's roots, plus the pointer cells. The fetch-gate publishes content
   nodes first, then the sub pointer cells + composite bookkeeping, and
   `:composite/root` STRICTLY LAST — so a subscriber's `on-key-update` for
   `:composite/root` means the entire composite (index + every sub) is already
   local: ONE causal gate, exactly like datahike's `:db` root.

   Plain-data nodes ⇒ no yggdrasil dependency; runs on a read-only cljs peer."
  (:require [konserve.core :as k]
            [konserve-sync.walkers.pss :as pss]
            #?@(:clj [[superv.async :refer [go-try- <?-]]]
                :cljs [[clojure.core.async :refer [<!]]]))
  #?(:cljs (:require-macros [clojure.core.async :refer [go]]
                            [superv.async :refer [go-try- <?-]])))

(def ^:private composite-root-key :composite/root)
(def ^:private composite-freed-key :composite/freed)
(def ^:private composite-subs-key :composite/subs)

(defn composite-key-last
  "`:key-sort-fn` for a composite store, a 3-tier gate:
     0  content-addressed (uuid) node blocks      — first
     1  sub pointer cells + composite bookkeeping  — next
     2  :composite/root                            — STRICTLY last (lone gate)
   So `:composite/root` becomes visible only after every sub root and every node
   it (transitively) references is already local."
  [k]
  (cond
    (= k composite-root-key) 2
    (or (keyword? k) (vector? k)) 1
    :else 0))

(defn composite-walk-fn
  "konserve-sync `:walk-fn` for a co-located composite store: every PSS node
   reachable from `:composite/root` (the index) and from each sub's roots-cell
   (read from the `:composite/subs` manifest), plus all pointer cells. Orphan
   blocks are pruned. `(fn [store opts] -> channel<set-of-reachable-keys>)`.

   `addresses-fn` (3-arity) projects a stored node to its child addresses —
   default `:addresses` (plain-map nodes); object-storing consumers (whose `k/get`
   returns PSS Leaf/Branch objects) pass e.g. yggdrasil's `node-child-addresses`."
  ([store opts] (composite-walk-fn store opts :addresses))
  ([store _opts addresses-fn]
   (go-try-
    (let [manifest (<?- (k/get store composite-subs-key))
          pointer-keys (into #{composite-root-key composite-freed-key composite-subs-key}
                             (mapcat (fn [s] [(:roots-key s) (:freed-key s)]))
                             manifest)
          collected (atom pointer-keys)]
      ;; the composite index tree (root is a bare address)
      (<?- (pss/walk-pss-node-async store (<?- (k/get store composite-root-key)) collected addresses-fn))
      ;; each co-located sub's tree(s)
      (loop [ss (seq manifest)]
        (when ss
          (let [roots (<?- (k/get store (:roots-key (first ss))))]
            (loop [rs (seq (vals roots))]
              (when rs
                (<?- (pss/walk-pss-node-async store (first rs) collected addresses-fn))
                (recur (next rs)))))
          (recur (next ss))))
      @collected))))

(defn composite-sync-opts
  "Options bundle for `register-store!` / `subscribe-store!` on a co-located
   composite store: the reachability walker + the `:composite/root`-last gate.
   `addresses-fn` (1-arity) overrides the node→child-addresses projection for
   object-storing consumers (default `:addresses`, plain-map nodes).

     (register-store! peer topic (:kv-store composite) (composite-sync-opts))"
  ([] (composite-sync-opts :addresses))
  ([addresses-fn]
   {:walk-fn (fn [store opts] (composite-walk-fn store opts addresses-fn))
    :key-sort-fn composite-key-last}))
