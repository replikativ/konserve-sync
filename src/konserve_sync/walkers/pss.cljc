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
  "Build a konserve-sync `:walk-fn` for a content-addressed PSS store on the per-branch
   head-cell layout.

     branches-key       the keyword naming the branch REGISTRY cell (a grow-set of names)
     head-key-fn        branch name → its head cell key (e.g. `default-head-key`)
     include-pointers?  (optional, default true) also ship `branches-key` + each head cell
                        (so a peer reconstructs the whole tree from the store); false =
                        nodes ONLY (the head rides the value via signal-sync)
     addresses-fn       (optional) node → child addresses; default `:addresses`. Object-
                        storing consumers (yggdrasil) MUST pass an object-aware projector.

   Each head cell is `{:root node}` (single-root) or `{:adds node :removals node}`
   (two-half); each root value is a fused NODE (walk its CHILDREN — its own address would
   `k-get` to nil) or a bare ADDRESS (walk from it). Returns `(fn [store _opts] ->
   channel<set-of-reachable-keys>)`: the pointer cells + every reachable PSS node; orphans
   pruned."
  ([branches-key head-key-fn] (make-pss-walk-fn branches-key head-key-fn true :addresses))
  ([branches-key head-key-fn include-pointers?] (make-pss-walk-fn branches-key head-key-fn include-pointers? :addresses))
  ([branches-key head-key-fn include-pointers? addresses-fn]
   (fn [store _opts]
     (go-try-
      (let [collected (atom (if include-pointers? #{branches-key} #{}))
            branches  (<?- (k/get store branches-key))]
        (loop [bs (seq branches)]
          (when bs
            (let [b    (first bs)
                  hk   (head-key-fn b)
                  head (<?- (k/get store hk))]
              (when include-pointers? (swap! collected conj hk))
              ;; a head cell is {:root node} (single-root) or {:adds node :removals node}
              ;; (two-half); walk each root value (fused node → its children; bare
              ;; address → from it). `:parents` etc. are metadata, not walked.
              (loop [rs (seq (keep #(get head %) [:root :adds :removals]))]
                (when rs
                  (let [root (first rs)]
                    (if (or (uuid? root) (string? root) (nil? root))
                      (when root (<?- (walk-pss-node-async store root collected addresses-fn)))
                      (loop [as (seq (addresses-fn root))]
                        (when as
                          (<?- (walk-pss-node-async store (first as) collected addresses-fn))
                          (recur (next as))))))
                  (recur (next rs)))))
            (recur (next bs))))
        @collected)))))

(defn default-head-key
  "The standalone head-cell key for a branch: `:crdt.head/<branch>` (round-trips a
   namespaced branch name via `(subs (str …) 1)`)."
  [branch]
  (keyword "crdt.head" (subs (str branch) 1)))

(defn co-located-head-key
  "The head-cell key for a co-located composite sub: `:crdt.head/<cell-ns>::<branch>`."
  [cell-ns branch]
  (keyword "crdt.head" (str cell-ns "::" (subs (str branch) 1))))

(defn walk-head-roots!
  "Walk every PSS root value in a head map (`{:root node}` single-root, or
   `{:adds node :removals node}` two-half) into `collected`: a fused NODE → its children
   (via `addresses-fn`); a bare ADDRESS → from it. Returns `@collected` (a channel op)."
  [store head collected addresses-fn]
  (go-try-
   (loop [rs (seq (keep #(get head %) [:root :adds :removals]))]
     (when rs
       (let [root (first rs)]
         (if (or (uuid? root) (string? root) (nil? root))
           (when root (<?- (walk-pss-node-async store root collected addresses-fn)))
           (loop [as (seq (addresses-fn root))]
             (when as
               (<?- (walk-pss-node-async store (first as) collected addresses-fn))
               (recur (next as))))))
       (recur (next rs))))
   @collected))

(defn make-pss-sync-opts
  "Options bundle for `register-store!` / `subscribe-store!` on a per-branch-head-cell
   PSS store: the reachability walker + the fetch-gate ordering. `head-key-fn` maps a
   branch name → its head cell key (default `default-head-key`). `include-pointers?`
   (default true) also ships the registry + head cells; pass false for nodes-ONLY.
   `addresses-fn` projects a node → child addresses — default `:addresses`; object
   stores (yggdrasil) MUST pass `yggdrasil.storage/node-child-addresses`."
  ([branches-key] (make-pss-sync-opts branches-key default-head-key true :addresses))
  ([branches-key head-key-fn include-pointers? addresses-fn]
   {:walk-fn (make-pss-walk-fn branches-key head-key-fn include-pointers? addresses-fn)
    :key-sort-fn keyword-last}))
