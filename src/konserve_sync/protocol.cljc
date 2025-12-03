(ns konserve-sync.protocol
  "Sync protocol message formats, store identification, and validation.

   This namespace contains pure functions for creating and validating
   sync protocol messages. No I/O or async operations."
  (:require [hasch.core :as hasch]))

;; =============================================================================
;; Message Types
;; =============================================================================

(def msg-types
  "Set of valid sync message types."
  #{:sync/subscribe       ; Client → Server: request subscription
    :sync/subscribe-ack   ; Server → Client: subscription confirmed
    :sync/subscribe-error ; Server → Client: subscription failed
    :sync/update          ; Server → Client: key-value update
    :sync/batch-complete  ; Server → Client: batch boundary
    :sync/batch-ack       ; Client → Server: batch acknowledged
    :sync/complete})      ; Server → Client: initial sync complete

(def operations
  "Set of valid sync operations."
  #{:key/assoc       ; Single key write (assoc, assoc-in, update, update-in, bassoc)
    :key/dissoc      ; Single key delete
    :key/multi-assoc ; Multiple keys write
    :key/write})     ; Initial sync write (same as :key/assoc, used during initial sync)

;; =============================================================================
;; Store Identification
;; =============================================================================

(def ^:private volatile-config-keys
  "Config keys to exclude when computing store-id.
   These are runtime-specific keys that should not affect store identity."
  #{:opts :serializers :cache :read-handlers :write-handlers})

(defn normalize-config
  "Normalize store config by removing volatile keys.
   Returns a stable map suitable for hashing."
  [store-config]
  (apply dissoc store-config volatile-config-keys))

(defn store-id
  "Compute store ID from config by hashing the normalized config.

   For distributed scenarios where client and server need to agree on the
   same store-id, ensure that the :scope key is set to the same value
   (typically a UUID) on both sides. The :scope represents the logical
   identity of the store across different network environments."
  [store-config]
  (hasch/uuid (normalize-config store-config)))

;; =============================================================================
;; Message Constructors
;; =============================================================================

(defn make-subscribe-msg
  "Create a subscription request message.

   - store-id: UUID of the store to subscribe to
   - local-key-timestamps: Map of {key last-write-timestamp} for keys the client has.
     The server will send keys that are either missing or have a newer timestamp."
  [store-id local-key-timestamps]
  {:type :sync/subscribe
   :store-id store-id
   :local-key-timestamps (or local-key-timestamps {})
   :id (random-uuid)})

(defn make-subscribe-ack-msg
  "Create a subscription acknowledgment message."
  [store-id request-id]
  {:type :sync/subscribe-ack
   :store-id store-id
   :id request-id})

(defn make-subscribe-error-msg
  "Create a subscription error message."
  [store-id request-id error]
  {:type :sync/subscribe-error
   :store-id store-id
   :error error
   :id request-id})

(defn make-update-msg
  "Create an update message from a write-hook event.

   - store-id: UUID of the store
   - api-op: The konserve API operation (:assoc-in, :update-in, :dissoc, etc.)
   - key: The top-level key
   - value: The value written (for assoc/update operations)
   - kvs: Map of key->value (for multi-assoc)"
  [store-id api-op key value kvs]
  (let [operation (case api-op
                    (:assoc :assoc-in :update :update-in :bassoc) :key/assoc
                    :dissoc :key/dissoc
                    :multi-assoc :key/multi-assoc
                    ;; Default to :key/assoc for unknown ops
                    :key/assoc)]
    (cond-> {:type :sync/update
             :store-id store-id
             :operation operation}
      ;; Single key operations
      (#{:key/assoc :key/dissoc} operation)
      (assoc :key key)

      ;; Value for assoc
      (= :key/assoc operation)
      (assoc :value value)

      ;; Multi-assoc
      (= :key/multi-assoc operation)
      (assoc :kvs kvs))))

(defn make-initial-sync-msg
  "Create an update message for initial sync (uses :key/write operation).
   Semantically same as :key/assoc but distinguishes initial sync from incremental."
  [store-id key value]
  {:type :sync/update
   :store-id store-id
   :operation :key/write
   :key key
   :value value})

(defn make-batch-complete-msg
  "Create a batch-complete message for flow control."
  [store-id batch-idx]
  {:type :sync/batch-complete
   :store-id store-id
   :batch-idx batch-idx})

(defn make-batch-ack-msg
  "Create a batch acknowledgment message."
  [store-id batch-idx]
  {:type :sync/batch-ack
   :store-id store-id
   :batch-idx batch-idx})

(defn make-complete-msg
  "Create a sync-complete message."
  [store-id]
  {:type :sync/complete
   :store-id store-id})

;; =============================================================================
;; Validation
;; =============================================================================

(defn valid-msg-type?
  "Check if a message type is valid."
  [type]
  (contains? msg-types type))

(defn valid-operation?
  "Check if a sync operation is valid."
  [operation]
  (contains? operations operation))

(defn valid-msg?
  "Validate a sync message structure.
   Returns true if valid, false otherwise."
  [msg]
  (and (map? msg)
       (valid-msg-type? (:type msg))
       (some? (:store-id msg))
       ;; Type-specific validation
       (case (:type msg)
         :sync/subscribe
         (and (map? (:local-key-timestamps msg))
              (some? (:id msg)))

         (:sync/subscribe-ack :sync/subscribe-error)
         (some? (:id msg))

         :sync/update
         (and (valid-operation? (:operation msg))
              (case (:operation msg)
                (:key/assoc :key/write :key/dissoc) (some? (:key msg))
                :key/multi-assoc (map? (:kvs msg))
                false))

         (:sync/batch-complete :sync/batch-ack)
         (number? (:batch-idx msg))

         :sync/complete
         true

         ;; Unknown type
         false)))

;; =============================================================================
;; Message Predicates
;; =============================================================================

(defn subscribe-msg?
  "Check if message is a subscription request."
  [msg]
  (= :sync/subscribe (:type msg)))

(defn update-msg?
  "Check if message is an update."
  [msg]
  (= :sync/update (:type msg)))

(defn batch-complete-msg?
  "Check if message is a batch-complete."
  [msg]
  (= :sync/batch-complete (:type msg)))

(defn complete-msg?
  "Check if message is a sync-complete."
  [msg]
  (= :sync/complete (:type msg)))

(defn control-msg?
  "Check if message is a control message (not data)."
  [msg]
  (contains? #{:sync/subscribe :sync/subscribe-ack :sync/subscribe-error
               :sync/batch-complete :sync/batch-ack :sync/complete}
             (:type msg)))
