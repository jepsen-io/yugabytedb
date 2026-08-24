(ns jepsen.yugabyte.jsql
  "Wrappers for the jepsen.sql tests."
  (:require [jepsen [client :as client]
                    [sql :as jsql]]
            [jepsen.yugabyte.ysql.client :refer [open error-fn]]))

(def dml-lock
  "YugaByteDB struggles if you give it concurrent DDL operations, so we have to
  make sure they come in one at a time."
  (Object.))

(defrecord ClientWrapper [client]
  client/Client
  (open! [this test node]
    (assoc this :client (client/open! client test node)))

  (setup! [this test]
    (locking dml-lock
      (client/setup! client test)))

  (invoke! [this test op]
    (client/invoke! client test op))

  (teardown! [this test]
    (client/teardown! client test))

  (close! [this test]
    (client/close! client test)))

(def workloads
  "Workloads from jepsen.sql"
  (-> (jepsen.sql/workloads
        {:open     open
         :error-fn error-fn})
      (update-keys (fn [k] (keyword "jsql" (name k))))
      (update-vals (fn [workload]
                     (fn workload-wrapper [opts]
                       (-> (workload opts)
                           (update :client ->ClientWrapper)))))))
