(ns konserve-sync.transport.kabel
  "Kabel transport adapter for konserve-sync.

   Integrates konserve-sync with kabel's peer/middleware system.
   This allows sync messages to flow through existing kabel connections.

   Uses superv.async for proper error handling - the supervisor is
   passed through from kabel's middleware chain.

   ## Usage

   Server-side:
   ```clojure
   (require '[konserve-sync.transport.kabel :as kabel-sync])
   (require '[superv.async :refer [S]])

   ;; Create sync context with supervisor
   (def sync-ctx (sync/make-context S))

   ;; Register stores
   (sync/register-store! sync-ctx my-store store-config {})

   ;; Create kabel peer with sync middleware
   (def server (peer/server-peer S handler server-id
                 (comp (kabel-sync/sync-middleware sync-ctx)
                       other-middleware)
                 serialization-middleware))
   ```

   Client-side:
   ```clojure
   ;; Create kabel peer with sync middleware
   (def client (peer/client-peer S client-id
                 (comp (kabel-sync/sync-middleware sync-ctx)
                       other-middleware)
                 serialization-middleware))

   ;; Connect and get transport
   (go
     (<! (peer/connect S client server-url))
     (let [transport (kabel-sync/connection-transport client)]
       (<! (sync/subscribe! sync-ctx transport store-id local-store opts))))
   ```"
  (:require #?(:clj [clojure.core.async :refer [chan put! close! go go-loop]]
               :cljs [clojure.core.async :refer [chan put! close!] :refer-macros [go go-loop]])
            #?(:clj [superv.async :refer [go-try go-loop-try <? >?]]
               :cljs [superv.async :refer [<? >?] :refer-macros [go-try go-loop-try]])
            [konserve-sync.protocol :as proto]
            [konserve-sync.sync :as sync]
            [konserve-sync.transport.protocol :as tp]
            [konserve-sync.log :as log]))

;; =============================================================================
;; Kabel Transport Wrapper
;; =============================================================================

(defrecord KabelTransport
  [S          ; Supervisor from superv.async
   out-ch     ; Channel to send messages to remote peer
   state      ; TransportState for handlers
   peer-id])  ; ID of the remote peer (for debugging)

(extend-type KabelTransport
  tp/PSyncTransport

  (send! [this msg]
    (let [{:keys [S out-ch state]} this]
      (go-try S
        (if (tp/closed? state)
          {:error (ex-info "Transport closed" {:msg msg})}
          (do
            (>? S out-ch msg)
            {:ok true})))))

  (on-message! [this handler]
    (tp/add-handler! (:state this) handler))

  (close! [this]
    (let [{:keys [S]} this]
      (tp/mark-closed! (:state this))
      (go-try S true))))

(defn- wrap-kabel-connection
  "Wrap a kabel connection as a PSyncTransport.

   Parameters:
   - S: Supervisor from superv.async
   - out-ch: The output channel for sending to the remote peer
   - peer-id: The remote peer's ID

   Returns a KabelTransport."
  [S out-ch peer-id]
  (->KabelTransport S out-ch (tp/make-transport-state) peer-id))

;; =============================================================================
;; Sync Middleware
;; =============================================================================

(defn sync-middleware
  "Create kabel middleware that handles sync protocol messages.

   The middleware:
   1. Intercepts :sync/* messages
   2. Routes subscription requests to the SyncContext
   3. Forwards other sync messages to registered handlers
   4. Passes non-sync messages through unchanged

   Parameters:
   - sync-ctx: SyncContext from konserve-sync.core/make-context

   Usage in kabel peer creation:
   ```clojure
   (peer/server-peer S handler server-id
     (comp (sync-middleware sync-ctx)
           other-middleware)
     serialization-middleware)
   ```"
  [sync-ctx]
  (fn [[S peer [in out]]]
    (let [;; Create transport for this connection
          transport (wrap-kabel-connection S out (:id peer))

          ;; Create filtered channels
          pass-in (chan)
          pass-out (chan)]

      ;; Route incoming messages with supervision
      (go-loop-try S [msg (<? S in)]
        (when msg
          (if (and (map? msg) (proto/valid-msg? msg))
            ;; Sync message - dispatch to handlers and/or context
            (do
              (tp/invoke-handlers! (:state transport) msg)

              ;; Server-side: handle subscription requests
              (when (proto/subscribe-msg? msg)
                (sync/serve-subscription! sync-ctx transport msg)))

            ;; Non-sync message - pass through
            (>? S pass-in msg))
          (recur (<? S in))))

      ;; Route outgoing messages with supervision
      (go-loop-try S [msg (<? S pass-out)]
        (when msg
          (>? S out msg)
          (recur (<? S pass-out))))

      ;; Return the filtered streams for downstream middleware
      [S peer [pass-in pass-out]])))

;; =============================================================================
;; Connection Management
;; =============================================================================

(defrecord KabelSyncServer
  [S            ; Supervisor from superv.async
   sync-ctx     ; SyncContext
   state        ; TransportState for connection handlers
   connections  ; atom: {peer-id KabelTransport}
   peer])       ; The kabel peer

(extend-type KabelSyncServer
  tp/PSyncServer

  (on-connection! [this handler]
    (tp/add-handler! (:state this) handler))

  (broadcast! [this store-id msg]
    (let [{:keys [S sync-ctx]} this]
      (go-try S
        (let [subscribers (sync/get-subscribers sync-ctx store-id)]
          ;; Use loop instead of doseq because <? doesn't work inside doseq
          (loop [remaining (seq subscribers)]
            (when remaining
              (<? S (tp/send! (first remaining) msg))
              (recur (next remaining))))
          {:ok true})))))

(defn kabel-sync-server
  "Create a KabelSyncServer for managing sync over kabel.

   Parameters:
   - S: Supervisor from superv.async
   - sync-ctx: SyncContext from konserve-sync.core/make-context
   - peer: The kabel server peer

   Returns a KabelSyncServer."
  [S sync-ctx peer]
  (->KabelSyncServer S
                     sync-ctx
                     (tp/make-transport-state)
                     (atom {})
                     peer))

;; =============================================================================
;; Client-Side Helpers
;; =============================================================================

(defn connection-transport
  "Get a PSyncTransport for a kabel client's connection.

   Call this after peer/connect to get a transport for sync/subscribe!

   Parameters:
   - S: Supervisor from superv.async
   - client-peer: The kabel client peer
   - out-ch: The output channel for this connection

   Note: This is a simplified implementation. In practice, you may need
   to track connections more carefully based on your kabel setup."
  [S client-peer out-ch]
  (wrap-kabel-connection S out-ch (:id client-peer)))

;; =============================================================================
;; Middleware Variants
;; =============================================================================

(defn sync-server-middleware
  "Server-specific sync middleware that tracks connections.

   Like sync-middleware but also:
   - Notifies connection handlers when peers connect
   - Tracks peer disconnects
   - Supports broadcast to subscribers

   Parameters:
   - sync-server: KabelSyncServer from kabel-sync-server

   Returns kabel middleware."
  [sync-server]
  (fn [[S peer [in out]]]
    (log/debug! {:id ::server-middleware-init
                 :msg "Server middleware initializing"
                 :data {:peer-id (:id peer)}})
    (let [peer-id (:id peer)
          ;; Create transport for this connection
          transport (wrap-kabel-connection S out peer-id)

          ;; Store connection
          _ (swap! (:connections sync-server) assoc peer-id transport)

          ;; Notify connection handlers
          _ (tp/invoke-handlers! (:state sync-server) transport)

          sync-ctx (:sync-ctx sync-server)
          _ (log/trace! {:id ::server-middleware-stores
                         :msg "Registered stores"
                         :data {:stores (keys (get @(:state sync-ctx) :stores))}})

          ;; Create filtered channels
          pass-in (chan)
          pass-out (chan)]

      ;; Route incoming messages with supervision
      (go-loop-try S [msg (<? S in)]
        (if msg
          (do
            (if (and (map? msg) (proto/valid-msg? msg))
              ;; Sync message - dispatch
              (do
                (log/trace! {:id ::server-incoming-msg
                             :msg "Incoming sync message"
                             :data {:type (:type msg)}})
                (tp/invoke-handlers! (:state transport) msg)
                (when (proto/subscribe-msg? msg)
                  (log/debug! {:id ::server-serve-subscription
                               :msg "Processing subscription request"})
                  (sync/serve-subscription! sync-ctx transport msg)))
              ;; Non-sync - pass through
              (>? S pass-in msg))
            (recur (<? S in)))

          ;; Connection closed (nil msg)
          (do
            (log/debug! {:id ::server-connection-closed
                         :msg "Connection closed, cleaning up"
                         :data {:peer-id peer-id}})
            ;; Clean up subscriptions
            (doseq [store-id (sync/get-store-ids sync-ctx)]
              (sync/remove-subscriber! sync-ctx store-id transport))
            ;; Remove from connections
            (swap! (:connections sync-server) dissoc peer-id)
            ;; Close transport
            (tp/close! transport))))

      ;; Route outgoing with supervision
      (go-loop-try S [msg (<? S pass-out)]
        (when msg
          (>? S out msg)
          (recur (<? S pass-out))))

      (log/debug! {:id ::server-middleware-ready
                   :msg "Server middleware ready"
                   :data {:peer-id peer-id}})
      [S peer [pass-in pass-out]])))

(defn sync-client-middleware
  "Client-specific sync middleware.

   Like sync-middleware but stores the connection transport
   for later use with subscribe!

   Parameters:
   - sync-ctx: SyncContext
   - transport-atom: Atom to store the transport in

   After connection, the transport will be available in @transport-atom.

   Returns kabel middleware."
  [sync-ctx transport-atom]
  (fn [[S peer [in out]]]
    (log/debug! {:id ::client-middleware-init
                 :msg "Client middleware initializing"
                 :data {:peer-id (:id peer)}})
    (let [;; Create and store transport
          transport (wrap-kabel-connection S out (:id peer))
          _ (reset! transport-atom transport)

          ;; Create filtered channels
          pass-in (chan)
          pass-out (chan)]

      ;; Route incoming with supervision
      (go-loop-try S [msg (<? S in)]
        (when msg
          (if (and (map? msg) (proto/valid-msg? msg))
            ;; Sync message - dispatch to handlers
            (do
              (log/trace! {:id ::client-incoming-msg
                           :msg "Incoming sync message"
                           :data {:type (:type msg)}})
              (tp/invoke-handlers! (:state transport) msg))
            ;; Non-sync - pass through
            (>? S pass-in msg))
          (recur (<? S in))))

      ;; Route outgoing with supervision
      (go-loop-try S [msg (<? S pass-out)]
        (when msg
          (>? S out msg)
          (recur (<? S pass-out))))

      (log/debug! {:id ::client-middleware-ready
                   :msg "Client middleware ready"
                   :data {:peer-id (:id peer)}})
      [S peer [pass-in pass-out]])))
