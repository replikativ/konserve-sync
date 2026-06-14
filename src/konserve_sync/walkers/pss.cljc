(ns konserve-sync.walkers.pss
  "Generic walker for a content-addressed persistent-sorted-set (PSS) konserve
   store — the substrate shared by the yggdrasil snapshot-registry AND any
   yggdrasil durable conflict-free system (durable G-Set / OR-Map …).

   Such a store holds PSS B-tree nodes serialized by
   `yggdrasil.storage/KonserveStorage` as **plain maps**:

     <content-addressed-uuid>  →  {:level :keys :addresses}

   plus a small set of mutable POINTER keys (always keywords), one of which is
   the **roots cell** `{_ -> root-address}` naming the live head(s):

     :registry/roots → {:tsbs <root>}      (registry)
     :crdt/roots     → {<branch> <root>}   (durable CRDT — one root per branch)

   `make-pss-walk-fn` builds the konserve-sync `:walk-fn` for such a store given
   its roots-key + its mutable pointer keys; `keyword-last` is the per-store
   fetch-gate (content blocks first, mutable keyword pointers last). Because the
   on-disk node format is plain data (NOT a Java PSS object), this needs **no
   yggdrasil dependency** and runs unchanged on a read-only ClojureScript peer."
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
   `collected` atom. Idempotent — guards against re-visiting a shared block."
  [store addr collected]
  (go-try-
   (when (and addr (not (contains? @collected addr)))
     (swap! collected conj addr)
     (let [node (<?- (k/get store addr))]
       (when-let [addresses (:addresses node)]
         (loop [as (seq addresses)]
           (when as
             (<?- (walk-pss-node-async store (first as) collected))
             (recur (next as)))))))))

(defn make-pss-walk-fn
  "Build a konserve-sync `:walk-fn` for a content-addressed PSS store.

     roots-key     the keyword naming the roots cell ({_ -> root-address})
     pointer-keys  the mutable keyword pointers to include verbatim
                   (typically #{roots-key <freed-key>})

   Returns `(fn [store _opts] -> channel<set-of-reachable-keys>)`: every pointer
   key + the root address(es) read from `roots-key` + every PSS node reachable
   from each root via `:addresses`. Unreferenced (orphan) blocks are pruned."
  [roots-key pointer-keys]
  (fn [store _opts]
    (go-try-
     (let [collected (atom (set pointer-keys))
           roots (<?- (k/get store roots-key))]
       (loop [rs (seq (vals roots))]
         (when rs
           (<?- (walk-pss-node-async store (first rs) collected))
           (recur (next rs))))
       @collected))))

(defn make-pss-sync-opts
  "Options bundle for `register-store!` / `subscribe-store!` on a PSS store:
   the reachability walker + the fetch-gate ordering. Merge in `:on-key-update`
   / `:on-complete` as needed."
  [roots-key pointer-keys]
  {:walk-fn (make-pss-walk-fn roots-key pointer-keys)
   :key-sort-fn keyword-last})
