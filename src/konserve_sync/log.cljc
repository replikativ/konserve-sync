(ns konserve-sync.log
  "Lightweight logging helpers for konserve-sync built on Trove.

  Use structured logging via `trace!`, `debug!`, etc.
  Trove's log! macro requires a compile-time map literal."
  (:require
   [taoensso.trove :as trove])
  #?(:cljs (:require-macros [konserve-sync.log])))

#?(:clj
   (defmacro log!
     "Generic wrapper if you want to pass :level explicitly."
     [opts]
     `(trove/log! ~opts)))

#?(:clj (defmacro trace!  [opts] `(trove/log! ~(assoc opts :level :trace))))
#?(:clj (defmacro debug!  [opts] `(trove/log! ~(assoc opts :level :debug))))
#?(:clj (defmacro info!   [opts] `(trove/log! ~(assoc opts :level :info))))
#?(:clj (defmacro warn!   [opts] `(trove/log! ~(assoc opts :level :warn))))
#?(:clj (defmacro error!  [opts] `(trove/log! ~(assoc opts :level :error))))
