(ns jepsen.yugabyte.ysql.set
  "Adds elements to a set, stored in a single table, and reads them back."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.random :as random]
            [jepsen.yugabyte.ysql.client :as c]))

(def table-name "elements")

(def regular-index-name "idx_elements")

(defrecord InternalClient []
  c/YSQLYbClient

  (setup-cluster! [this test c]
    (c/execute! c [(str "CREATE TABLE IF NOT EXISTS " table-name
                        ; TODO: drop primary key?
                        " (val INT PRIMARY KEY)")])
    (c/execute! c [(str "CREATE INDEX " regular-index-name " ON "
                       table-name " (val)")]))

  (invoke-op! [this test op c]
    (c/with-txn test c
      (case (:f op)
        :add (do (c/insert! c table-name {:val (:value op)})
                 (assoc op :type :ok))

        :read (let [use-index? (zero? (random/long 2))
                    value (->> [(str (when use-index?
                                      (str "/*+ IndexOnlyScan(" table-name " " regular-index-name ") */ "))
                                    "SELECT val FROM " table-name)]
                               (c/execute! c)
                               (mapv :val))]
                (info table-name (if use-index? "IndexOnlyScan" "SeqScan"))
                (assoc op :type :ok, :value value)))))

  (teardown-cluster! [this test c]
    (c/drop-table c table-name)))

(c/defclient Client InternalClient)

;
; Index-governed set test
;

; NOTE: This doesn't work as intended yet as index isn't used for this query
; See https://github.com/YugaByte/yugabyte-db/issues/1554

(def index-name "elements_idx")

(def group-count
  "Number of distinct groups for indexing"
  8)

(def set-index-query
  [(str "SELECT val FROM " table-name " WHERE grp " (c/in (range group-count)))])

(defrecord InternalIndexClient []
  c/YSQLYbClient

  (setup-cluster! [this test c]
    (c/execute! c [(str "CREATE TABLE IF NOT EXISTS " table-name
                        " (id INT PRIMARY KEY, val INT, grp INT)")])
    (c/execute! c [(str "CREATE INDEX " index-name " ON " table-name
                       " (grp) INCLUDE (val)")])
    (c/assert-involves-index c set-index-query index-name))

  (invoke-op! [this test op c]
    (case (:f op)
      :add (do (c/insert! op c table-name {:id  (:value op)
                                           :val (:value op)
                                           :grp (random/long group-count)})
               (assoc op :type :ok))

      :read (let [value (->> set-index-query
                             (c/execute! op c)
                             (mapv :val))]
              (assoc op :type :ok, :value value))))

  (teardown-cluster! [this test c]
    (c/drop-index c index-name)
    (c/drop-table c table-name)))

(c/defclient IndexClient InternalIndexClient)
