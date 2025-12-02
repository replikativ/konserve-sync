(ns konserve-sync.transport.protocol
  "Transport abstraction protocols for konserve-sync.

   Transports handle the actual message delivery between peers.
   This allows konserve-sync to work with different network layers
   (kabel, plain channels, HTTP, etc.).")

(defprotocol PSyncTransport
  "Transport abstraction for sync message delivery.

   All methods are asynchronous:
   - send! returns a channel that closes when message is sent
   - on-message! registers a handler for incoming messages
   - close! cleans up resources"

  (send! [this msg]
    "Send a message to the remote peer.
     Returns a channel that yields true when sent, or an error.")

  (on-message! [this handler]
    "Register a handler for incoming messages.
     handler: (fn [msg]) - called for each incoming message.
     Returns a function to unregister the handler.")

  (close! [this]
    "Close the transport and release resources.
     Returns a channel that closes when done."))

(defprotocol PSyncServer
  "Server-side transport extensions for managing multiple connections.

   Used by servers to track subscribers and broadcast updates."

  (on-connection! [this handler]
    "Register a handler for new client connections.
     handler: (fn [transport]) - called with a new PSyncTransport for each connection.
     Returns a function to unregister the handler.")

  (broadcast! [this store-id msg]
    "Send a message to all clients subscribed to store-id.
     Returns a channel that closes when all sends complete."))

;; =============================================================================
;; Transport State
;; =============================================================================

(defrecord TransportState
  [handlers   ; atom: set of message handler fns
   closed?])  ; atom: boolean

(defn make-transport-state
  "Create initial transport state."
  []
  (->TransportState (atom #{})
                    (atom false)))

(defn add-handler!
  "Add a message handler to transport state.
   Returns a function to remove the handler."
  [state handler]
  (swap! (:handlers state) conj handler)
  (fn []
    (swap! (:handlers state) disj handler)))

(defn invoke-handlers!
  "Invoke all registered handlers with a message."
  [state msg]
  (doseq [handler @(:handlers state)]
    (try
      (handler msg)
      (catch #?(:clj Exception :cljs :default) _e
        ;; Handler errors shouldn't break the transport
        nil))))

(defn mark-closed!
  "Mark transport as closed."
  [state]
  (reset! (:closed? state) true))

(defn closed?
  "Check if transport is closed."
  [state]
  @(:closed? state))
