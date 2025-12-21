(ns konserve-sync.transport.kabel-pubsub
  "Kabel transport adapter using kabel.pubsub for konserve-sync.

   This is the new integration layer that uses kabel.pubsub instead of
   the legacy custom sync middleware. It provides simplified server and
   client setup for store synchronization.

   ## Server-Side Usage

   ```clojure
   (require '[konserve-sync.transport.kabel-pubsub :as kp])
   (require '[kabel.peer :as peer])

   ;; Create server peer with pubsub middleware
   (def server-peer (peer/server-peer S handler server-id
                      (kp/server-middleware)
                      identity))

   ;; Start server
   (<! (peer/start server-peer))

   ;; Register stores for sync
   (kp/register-store! server-peer :my-store my-konserve-store {})
   ```

   ## Client-Side Usage

   ```clojure
   ;; Create client peer with pubsub middleware
   (def client-peer (peer/client-peer S client-id
                      (kp/client-middleware)
                      identity))

   ;; Connect to server
   (<! (peer/connect S client-peer server-url))

   ;; Subscribe to store
   (<! (kp/subscribe-store! client-peer :my-store local-store {}))
   ```"
  (:require #?(:clj [clojure.core.async :refer [go chan]]
               :cljs [clojure.core.async :refer [chan] :refer-macros [go]])
            #?(:clj [superv.async :refer [go-try <?]]
               :cljs [superv.async :refer [<?] :refer-macros [go-try]])
            [kabel.pubsub :as pubsub]
            [konserve-sync.pubsub :as ks-pubsub]
            [konserve-sync.log :as log]))

;; =============================================================================
;; Middleware Factories
;; =============================================================================

(defn server-middleware
  "Create kabel middleware for server-side store sync.

   This wraps kabel.pubsub middleware with default options.

   Parameters:
   - opts: Options map (passed to pubsub middleware)

   Returns a middleware function for use with peer/server-peer."
  ([]
   (server-middleware {}))
  ([opts]
   (pubsub/make-pubsub-peer-middleware opts)))

(defn client-middleware
  "Create kabel middleware for client-side store sync.

   This wraps kabel.pubsub middleware with default options.

   Parameters:
   - opts: Options map (passed to pubsub middleware)

   Returns a middleware function for use with peer/client-peer."
  ([]
   (client-middleware {}))
  ([opts]
   (pubsub/make-pubsub-peer-middleware opts)))

;; =============================================================================
;; Server-Side API
;; =============================================================================

(def register-store!
  "Register a konserve store for sync via pubsub (server-side).

   See konserve-sync.pubsub/register-store! for full documentation."
  ks-pubsub/register-store!)

(def unregister-store!
  "Unregister a store from pubsub (server-side).

   See konserve-sync.pubsub/unregister-store! for full documentation."
  ks-pubsub/unregister-store!)

;; =============================================================================
;; Client-Side API
;; =============================================================================

(defn subscribe-store!
  "Subscribe to a remote store and sync to a local store (client-side).

   Parameters:
   - peer: The kabel client peer atom
   - topic: The topic/store-id to subscribe to
   - local-store: Local konserve store to sync data into
   - opts: Options map
     - :on-key-update (fn [key value operation]) - Called after each update
     - :on-complete (fn []) - Called when initial sync completes

   Returns a channel that yields:
   - {:ok true} when subscription and initial sync complete
   - {:error ...} on failure"
  [peer topic local-store opts]
  (log/info! {:id ::subscribe-store
              :msg "Subscribing to store"
              :data {:topic topic}})
  (let [strategy (ks-pubsub/store-sync-strategy local-store opts)]
    (pubsub/subscribe! peer #{topic}
                       {:strategies {topic strategy}
                        :on-handshake-complete (when-let [on-complete (:on-complete opts)]
                                                 (fn [t]
                                                   (when (= t topic)
                                                     (on-complete))))})))

(defn unsubscribe-store!
  "Unsubscribe from a store (client-side).

   Parameters:
   - peer: The kabel client peer atom
   - topic: The topic/store-id to unsubscribe from

   Returns a channel that yields {:ok true}."
  [peer topic]
  (log/info! {:id ::unsubscribe-store
              :msg "Unsubscribing from store"
              :data {:topic topic}})
  (pubsub/unsubscribe! peer #{topic}))

;; =============================================================================
;; Utility Functions
;; =============================================================================

(defn get-subscribers
  "Get all transports subscribed to a store topic (server-side)."
  [peer topic]
  (pubsub/get-subscribers peer topic))

(defn topic-registered?
  "Check if a topic is registered (server-side)."
  [peer topic]
  (pubsub/topic-registered? peer topic))
