(ns jepsen.yugabyte.ysql.single-key-acid
  "A collection of independent registers, each of which supports linearizable
  read, write, and compare-and-set."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.independent :as independent]
            [jepsen.random :as random]
            [jepsen.yugabyte.ysql.client :as c]))

(def table-name "single_key_acid")
(def index-name "idx_single_key_acid")

(defrecord InternalClient []
  c/YSQLYbClient

  (setup-cluster! [this test c]
    (c/execute! c [(str "CREATE TABLE IF NOT EXISTS " table-name
                        "(id INT PRIMARY KEY, val INT)")])
    (c/execute! c [(str "CREATE INDEX " index-name " ON " table-name
                        " (id, val)")]))

  (invoke-op! [this test op c]
    (let [[id val] (:value op)]
      (case (:f op)
        :write
        (do (c/execute!
               op c
               [(str "INSERT INTO " table-name " (id, val) VALUES (?, ?) "
                     "ON CONFLICT ON CONSTRAINT " table-name "_pkey "
                     "DO UPDATE SET val = ?")
                id val val])
             (assoc op :type :ok))

        :cas
        (let [[v v'] val
              res (if (= 0 v)
                    ; Every value is (logically) initially zero, so when we're
                    ; doing (cas 0 x), we actually want to upsert.
                    (c/execute!
                      op c
                      [(str "INSERT INTO " table-name
                            " (id, val) VALUES (?, ?) "
                            "ON CONFLICT ON CONSTRAINT " table-name "_pkey "
                            "DO UPDATE SET val = ? "
                            "WHERE " table-name ".val = ?")
                            id v' v' v])
                    ; Otherwise this is a straightforward conditional update
                    (c/execute!
                      op c
                      [(str "UPDATE " table-name
                       " SET val = ? WHERE id = ? AND " table-name ".val = ?")
                       v' id v]))
              applied (pos? (:next.jdbc/update-count (first res)))]
          (assoc op :type (if applied :ok :fail)))

        :read
        (let [use-index? (random/bool)
              ;_ (info table-name (if use-index? "IndexOnlyScan" "SeqScan")
              ;        "id=" id)
              value (if use-index?
                      (-> (c/execute! op c [(str "/*+ IndexOnlyScan(" table-name
                                             " " index-name
                                             ") */ SELECT val FROM " table-name
                                             " WHERE id = " id)])
                          first :val)
                      (c/select-single-value c table-name :val
                                             (str "id = " id)))]
          (assoc op :type :ok :value (independent/tuple id value))))))

  (teardown-cluster! [this test c]
    (c/drop-table c table-name)))

(c/defclient Client InternalClient)
