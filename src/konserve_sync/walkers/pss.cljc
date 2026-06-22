(ns konserve-sync.walkers.pss
  "Generic walker for a content-addressed persistent-sorted-set (PSS) konserve
   store — the substrate shared by the yggdrasil snapshot-registry AND any
   yggdrasil durable conflict-free system (durable G-Set / OR-Map …).

   Such a store holds PSS B-tree nodes under content-addressed UUID keys, plus a
   small set of mutable POINTER keys (always keywords), one of which is the
   **roots cell** `{_ -> root-address}` naming the live head(s):

     :registry/roots → {:tsbs <root>}      (registry)
     :crdt/roots     → {<branch> <root>}   (durable CRDT — one root per branch)

   The walker reaches every node by following child addresses from each root. HOW
   to read a node's child addresses is a parameter — `:addresses-fn` — because the
   on-disk node *format* is owned by the consumer's konserve serializer. It
   defaults to the keyword `:addresses` (consumers that store nodes as plain maps
   `{:level :keys :addresses}`). A consumer whose `k/get` returns PSS Leaf/Branch
   **objects** (e.g. `yggdrasil.storage`, via the canonical fressian codec) MUST
   pass an object-aware projector (`yggdrasil.storage/node-child-addresses`):
   keyword access yields nil on an object, which would silently collapse the walk
   to the root alone — breaking incremental sync / GC of any multi-node tree.
   Because the format detail is injected, this walker needs **no yggdrasil/PSS
   dependency** and runs unchanged on a read-only ClojureScript peer.

   `make-pss-walk-fn` builds the konserve-sync `:walk-fn` for such a store given
   its roots-key + its mutable pointer keys (+ optional `:addresses-fn`);
   `keyword-last` is the per-store fetch-gate (content blocks first, mutable
   keyword pointers last)."
  (:require [konserve.core :as k]
            #?@(:clj [[superv.async :refer [go-try- <?-]]]
                :cljs [[clojure.core.async :refer [<!]]]))
  #?(:cljs (:require-macros [clojure.core.async :refer [go]]
                            [superv.async :refer [go-try- <?-]])))

(defn keyword-last
  "`:key-sort-fn` for a PSS store: content-addressed (uuid) node blocks publish
   FIRST, the mutable pointer keys (keywords like `:crdt/roots`, `:crdt/freed`)
   LAST. So a subscriber's `on-key-update` for the roots pointer means \"the
   whole tree at that root is already local\" — the per-store fetch-gate."
  [k]
  (if (keyword? k) 1 0))

(defn walk-pss-node-async
  "Depth-first walk from `addr`, adding every reachable node address to the
   `collected` atom. Idempotent — guards against re-visiting a shared block.
   `addresses-fn` projects a stored node to its child addresses (nil/empty at a
   leaf)."
  [store addr collected addresses-fn]
  (go-try-
   (when (and addr (not (contains? @collected addr)))
     (swap! collected conj addr)
     (let [node (<?- (k/get store addr))]
       (when-let [addresses (addresses-fn node)]
         (loop [as (seq addresses)]
           (when as
             (<?- (walk-pss-node-async store (first as) collected addresses-fn))
             (recur (next as)))))))))

(defn make-pss-walk-fn
  "Build a konserve-sync `:walk-fn` for a content-addressed PSS store.

     roots-key     the keyword naming the roots cell ({_ -> root-address})
     pointer-keys  the mutable keyword pointers to include verbatim
                   (typically #{roots-key <freed-key>})
     addresses-fn  (optional) projects a stored node to its child addresses;
                   defaults to `:addresses` (plain-map nodes). Object-storing
                   consumers (yggdrasil) MUST pass an object-aware projector.

   Returns `(fn [store _opts] -> channel<set-of-reachable-keys>)`: every pointer
   key + the root address(es) read from `roots-key` + every PSS node reachable
   from each root. Unreferenced (orphan) blocks are pruned."
  ([roots-key pointer-keys] (make-pss-walk-fn roots-key pointer-keys :addresses))
  ([roots-key pointer-keys addresses-fn]
   (fn [store _opts]
     (go-try-
      (let [collected (atom (set pointer-keys))
            roots (<?- (k/get store roots-key))]
        (loop [rs (seq (vals roots))]
          (when rs
            (<?- (walk-pss-node-async store (first rs) collected addresses-fn))
            (recur (next rs))))
        @collected)))))

(defn make-pss-sync-opts
  "Options bundle for `register-store!` / `subscribe-store!` on a PSS store:
   the reachability walker + the fetch-gate ordering. Merge in `:on-key-update`
   / `:on-complete` as needed. `addresses-fn` (optional) projects a stored node
   to its child addresses — default `:addresses`; object-storing consumers
   (yggdrasil) MUST pass `yggdrasil.storage/node-child-addresses`."
  ([roots-key pointer-keys] (make-pss-sync-opts roots-key pointer-keys :addresses))
  ([roots-key pointer-keys addresses-fn]
   {:walk-fn (make-pss-walk-fn roots-key pointer-keys addresses-fn)
    :key-sort-fn keyword-last}))
