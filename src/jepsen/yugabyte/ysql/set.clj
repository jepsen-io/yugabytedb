(ns jepsen.yugabyte.ysql.set
  "Adds elements to a one of several independent sets, and reads them back. We
  identify each set by an integer key, and store all sets in a single table. We
  use an index to read all rows relating to a given group."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [independent :as independent]
                    [random :as random]]
            [jepsen.yugabyte.ysql.client :as c]))

(def table-name "elements")

(defrecord InternalClient [index?]
  c/YSQLYbClient

  (setup-cluster! [this test c]
    (c/execute! c [(str "CREATE TABLE IF NOT EXISTS " table-name
                        " (key INT NOT NULL,
                           element INT NOT NULL)")])
    (when index?
      (c/execute! c [(str "CREATE INDEX idx_key ON "
                          table-name " (key) INCLUDE (element)")])))

  (invoke-op! [this test op c]
    (let [[key element] (:value op)]
      (case (:f op)
        :add (do (c/insert! c table-name {:key key, :element element})
                 (assoc op :type :ok))

        :read (let [use-index? (and index? (random/bool))
                    value (->> [(str (when use-index?
                                       (str "/*+ IndexOnlyScan(" table-name " key_idx) */ "))
                                     "SELECT element FROM " table-name
                                     " WHERE key = ?") key]
                               (c/execute! c)
                               (mapv :element))]
                (info table-name (if use-index? "IndexOnlyScan" "SeqScan"))
                (assoc op :type :ok, :value (independent/tuple key value))))))

  (teardown-cluster! [this test c]
    (c/drop-table c table-name)))

(c/defclient Client InternalClient)
