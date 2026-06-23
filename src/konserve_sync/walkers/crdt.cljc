(ns konserve-sync.walkers.crdt
  "konserve-sync walker for a yggdrasil DURABLE conflict-free system (durable
   G-Set / OR-Map …) — `yggdrasil.convergent.durable`'s PSS+konserve store.

   Identical substrate to the registry (content-addressed plain-map PSS nodes),
   differing only in the roots-cell key: a durable CRDT keeps ONE root per
   branch under

     :crdt/roots  →  {<branch> <root-address>}
     :crdt/freed  →  {<address> <ts>}   (GC bookkeeping)

   So this is just the generic `konserve-sync.walkers.pss` walker bound to those
   keys. No yggdrasil dependency; runs on a read-only ClojureScript peer."
  (:require [konserve-sync.walkers.pss :as pss]))

(def crdt-walk-fn
  "Walker: every PSS node reachable from `:crdt/roots` (one root per branch)
   plus the `:crdt/roots` / `:crdt/freed` pointers."
  (pss/make-pss-walk-fn :crdt/roots #{:crdt/roots :crdt/freed}))

(defn crdt-sync-opts
  "Options bundle for `register-store!` / `subscribe-store!` on a durable CRDT
   store: the reachability walker + the fetch-gate ordering.

     (register-store! peer topic (:kv-store gset) (crdt-sync-opts))"
  []
  {:walk-fn crdt-walk-fn
   :key-sort-fn pss/keyword-last})
