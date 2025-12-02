(ns konserve-sync.transport.channels
  "Channel-based transport for local testing and same-process communication.

   This transport uses core.async channels for message delivery.
   Useful for:
   - Unit testing without network
   - Same-JVM communication
   - Building blocks for custom transports

   Uses superv.async for proper error handling - errors are propagated
   through the supervisor hierarchy."
  (:require [clojure.core.async :refer [chan close!]]
            [superv.async :refer [go-try go-loop-try <? >?]]
            [konserve-sync.transport.protocol :as tp]))

;; =============================================================================
;; Channel Transport
;; =============================================================================

(defrecord ChannelTransport
  [S          ; Supervisor from superv.async
   in-ch      ; Channel for incoming messages
   out-ch     ; Channel for outgoing messages
   state])    ; TransportState record

(defn- start-message-loop!
  "Start loop that reads from in-ch and dispatches to handlers.

   Parameters:
   - S: Supervisor from superv.async
   - transport: The ChannelTransport"
  [S transport]
  (let [{:keys [in-ch state]} transport]
    (go-loop-try S [msg (<? S in-ch)]
      (when msg
        (when-not (tp/closed? state)
          (tp/invoke-handlers! state msg)
          (recur (<? S in-ch)))))))

(extend-type ChannelTransport
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
    (let [{:keys [S in-ch out-ch state]} this]
      (tp/mark-closed! state)
      (close! in-ch)
      (close! out-ch)
      (go-try S true))))

(defn channel-transport
  "Create a transport backed by core.async channels.

   Parameters:
   - S: Supervisor from superv.async
   - in-ch: Channel to read incoming messages from
   - out-ch: Channel to write outgoing messages to

   The transport starts a loop to dispatch incoming messages to handlers."
  [S in-ch out-ch]
  (let [transport (->ChannelTransport S in-ch out-ch (tp/make-transport-state))]
    (start-message-loop! S transport)
    transport))

;; =============================================================================
;; Channel Pair (for testing)
;; =============================================================================

(defn channel-pair
  "Create a connected pair of channel transports for testing.

   Messages sent on transport-a arrive at transport-b and vice versa.

   Parameters:
   - S: Supervisor from superv.async
   - opts: Options map
     - :buffer-size - buffer size for channels (default 100)

   Returns [transport-a transport-b]"
  ([S]
   (channel-pair S {}))
  ([S {:keys [buffer-size] :or {buffer-size 100}}]
   (let [;; Channels from A to B
         a->b (chan buffer-size)
         ;; Channels from B to A
         b->a (chan buffer-size)
         ;; A sends to a->b, receives from b->a
         transport-a (channel-transport S b->a a->b)
         ;; B sends to b->a, receives from a->b
         transport-b (channel-transport S a->b b->a)]
     [transport-a transport-b])))

;; =============================================================================
;; Channel Server (for testing multi-client scenarios)
;; =============================================================================

(defrecord ChannelServer
  [S                  ; Supervisor from superv.async
   state              ; TransportState for connection handlers
   connections        ; atom: map of conn-id -> {:transport ChannelTransport :subscriptions #{store-id}}
   next-conn-id])     ; atom: counter for connection IDs

(extend-type ChannelServer
  tp/PSyncServer

  (on-connection! [this handler]
    (tp/add-handler! (:state this) handler))

  (broadcast! [this store-id msg]
    (let [S (:S this)
          conns @(:connections this)
          ;; Filter connections subscribed to this store
          subscribed (filter (fn [[_conn-id {:keys [subscriptions]}]]
                               (contains? subscriptions store-id))
                             conns)]
      (go-try S
        ;; Send to all connections subscribed to this store using loop
        (loop [remaining (seq subscribed)]
          (when remaining
            (let [[_conn-id {:keys [transport]}] (first remaining)]
              (<? S (tp/send! transport msg))
              (recur (next remaining)))))
        {:ok true}))))

(defn channel-server
  "Create a channel-based server for testing multi-client scenarios.

   Parameters:
   - S: Supervisor from superv.async

   Returns a ChannelServer that can accept connections and broadcast updates."
  [S]
  (->ChannelServer S
                   (tp/make-transport-state)
                   (atom {})
                   (atom 0)))

(defn connect-to-server
  "Create a client connection to a channel server.

   Returns a transport for the client.
   The server's on-connection! handlers will be invoked with the server-side transport."
  [server]
  (let [S (:S server)
        conn-id (swap! (:next-conn-id server) inc)
        ;; Create channel pair
        [client-transport server-transport] (channel-pair S)
        ;; Store connection on server
        _ (swap! (:connections server) assoc conn-id
                 {:transport server-transport
                  :subscriptions #{}})
        ;; Notify connection handlers
        _ (tp/invoke-handlers! (:state server) server-transport)]
    client-transport))

(defn add-subscription!
  "Add a store subscription to a server connection.
   Used internally to track which connections receive which store updates."
  [server transport store-id]
  (let [conns @(:connections server)]
    ;; Find the connection with this transport and add subscription
    (doseq [[conn-id conn-data] conns
            :when (= transport (:transport conn-data))]
      (swap! (:connections server) update-in [conn-id :subscriptions] conj store-id))))

(defn close-server!
  "Close a channel server and all its connections."
  [server]
  (tp/mark-closed! (:state server))
  (doseq [[_conn-id {:keys [transport]}] @(:connections server)]
    (tp/close! transport))
  (reset! (:connections server) {}))
