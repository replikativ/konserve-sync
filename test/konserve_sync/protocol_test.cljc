(ns konserve-sync.protocol-test
  (:require #?(:clj [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer-macros [deftest is testing]])
            [konserve-sync.protocol :as proto]))

;; =============================================================================
;; Store ID Tests
;; =============================================================================

(deftest test-store-id-with-scope
  (testing "store-id produces same ID for configs with same :scope"
    (let [scope #uuid "12345678-1234-1234-1234-123456789012"
          config-1 {:scope scope :backend :file :path "/tmp/test"}
          config-2 {:scope scope :backend :file :path "/tmp/test"}]
      (is (= (proto/store-id config-1) (proto/store-id config-2))))))

(deftest test-store-id-from-config-hash
  (testing "store-id hashes normalized config"
    (let [config {:backend :file :path "/tmp/test"}
          id (proto/store-id config)]
      (is (uuid? id))
      ;; Same config should produce same ID
      (is (= id (proto/store-id config)))
      ;; Different config should produce different ID
      (is (not= id (proto/store-id {:backend :file :path "/tmp/other"}))))))

(deftest test-normalize-config
  (testing "normalize-config removes volatile keys"
    (let [config {:backend :file
                  :path "/tmp/test"
                  :opts {:some :opts}
                  :serializers {:custom :ser}
                  :cache {:some :cache}}
          normalized (proto/normalize-config config)]
      (is (= {:backend :file :path "/tmp/test"} normalized)))))

;; =============================================================================
;; Message Constructor Tests
;; =============================================================================

(deftest test-make-subscribe-msg
  (testing "creates valid subscribe message with key->timestamp map"
    (let [store-id #uuid "12345678-1234-1234-1234-123456789012"
          now #?(:clj (java.util.Date.) :cljs (js/Date.))
          local-key-timestamps {:key1 now :key2 now}
          msg (proto/make-subscribe-msg store-id local-key-timestamps)]
      (is (= :sync/subscribe (:type msg)))
      (is (= store-id (:store-id msg)))
      (is (= local-key-timestamps (:local-key-timestamps msg)))
      (is (uuid? (:id msg))))))

(deftest test-make-update-msg-assoc
  (testing "creates update message for assoc-in"
    (let [store-id #uuid "12345678-1234-1234-1234-123456789012"
          msg (proto/make-update-msg store-id :assoc-in :my-key "my-value" nil)]
      (is (= :sync/update (:type msg)))
      (is (= store-id (:store-id msg)))
      (is (= :key/assoc (:operation msg)))
      (is (= :my-key (:key msg)))
      (is (= "my-value" (:value msg))))))

(deftest test-make-update-msg-dissoc
  (testing "creates update message for dissoc"
    (let [store-id #uuid "12345678-1234-1234-1234-123456789012"
          msg (proto/make-update-msg store-id :dissoc :my-key nil nil)]
      (is (= :sync/update (:type msg)))
      (is (= :key/dissoc (:operation msg)))
      (is (= :my-key (:key msg)))
      (is (nil? (:value msg))))))

(deftest test-make-update-msg-multi-assoc
  (testing "creates update message for multi-assoc"
    (let [store-id #uuid "12345678-1234-1234-1234-123456789012"
          kvs {:k1 "v1" :k2 "v2"}
          msg (proto/make-update-msg store-id :multi-assoc nil nil kvs)]
      (is (= :sync/update (:type msg)))
      (is (= :key/multi-assoc (:operation msg)))
      (is (= kvs (:kvs msg))))))

(deftest test-make-batch-complete-msg
  (testing "creates batch-complete message"
    (let [store-id #uuid "12345678-1234-1234-1234-123456789012"
          msg (proto/make-batch-complete-msg store-id 5)]
      (is (= :sync/batch-complete (:type msg)))
      (is (= store-id (:store-id msg)))
      (is (= 5 (:batch-idx msg))))))

;; =============================================================================
;; Validation Tests
;; =============================================================================

(deftest test-valid-msg-type
  (testing "valid message types"
    (is (proto/valid-msg-type? :sync/subscribe))
    (is (proto/valid-msg-type? :sync/update))
    (is (proto/valid-msg-type? :sync/complete))
    (is (not (proto/valid-msg-type? :invalid)))))

(deftest test-valid-operation
  (testing "valid operations"
    (is (proto/valid-operation? :key/assoc))
    (is (proto/valid-operation? :key/dissoc))
    (is (proto/valid-operation? :key/multi-assoc))
    (is (proto/valid-operation? :key/write))
    (is (not (proto/valid-operation? :invalid)))))

(deftest test-valid-msg
  (testing "validates subscribe message"
    (let [now #?(:clj (java.util.Date.) :cljs (js/Date.))
          msg (proto/make-subscribe-msg #uuid "12345678-1234-1234-1234-123456789012" {:k now})]
      (is (proto/valid-msg? msg))))

  (testing "validates update message"
    (let [msg (proto/make-update-msg #uuid "12345678-1234-1234-1234-123456789012"
                                     :assoc-in :k "v" nil)]
      (is (proto/valid-msg? msg))))

  (testing "rejects invalid messages"
    (is (not (proto/valid-msg? {})))
    (is (not (proto/valid-msg? {:type :invalid})))
    (is (not (proto/valid-msg? {:type :sync/update :store-id nil})))))

;; =============================================================================
;; Predicate Tests
;; =============================================================================

(deftest test-message-predicates
  (testing "subscribe-msg?"
    (is (proto/subscribe-msg? {:type :sync/subscribe}))
    (is (not (proto/subscribe-msg? {:type :sync/update}))))

  (testing "update-msg?"
    (is (proto/update-msg? {:type :sync/update}))
    (is (not (proto/update-msg? {:type :sync/subscribe}))))

  (testing "control-msg?"
    (is (proto/control-msg? {:type :sync/subscribe}))
    (is (proto/control-msg? {:type :sync/complete}))
    (is (not (proto/control-msg? {:type :sync/update})))))

