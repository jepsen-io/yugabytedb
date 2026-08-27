(ns jepsen.yugabyte.ysql.rw
  "YSQL client for the read-write register workload.

  Registers are rows in `wr (k int primary key, k2 int, v int)` with a secondary
  index on k2 that INCLUDEs v. A transaction is a sequence of micro-ops [f k v]:
    :r  read  -> SELECT v WHERE k = k, OR (chosen at random) an index-only scan
                 SELECT v WHERE k2 = k, so both the base table and the secondary
                 index are exercised and must agree with committed writes.
    :w  write -> upsert (k2 mirrors k; writes are unique)
  Multi-op transactions run inside a JDBC transaction. Single-op transactions
  do not run in a transaction."
  (:require [jepsen.random :as random]
            [clojure.tools.logging :refer [info]]
            [jepsen.yugabyte.ysql.client :as c]))

(def table-name "wr")
(def index-name "idx_wr")

(defn read-register
  "Reads register k, coerced to a Long. Randomly reads via the primary key or
  via an index-only scan on the secondary index, so both paths are covered."
  [conn k]
  (let [use-index? (zero? (random/long 2))
        sql        (if use-index?
                     (str "/*+ IndexOnlyScan(" table-name " " index-name ") */ "
                          "select v from " table-name " where k2 = ?")
                     (str "select v from " table-name " where k = ?"))]
    (info table-name (if use-index? "IndexOnlyScan(k2)" "PrimaryScan(k)") "k=" k)
    (some-> conn (c/execute! [sql k]) first :v long)))

(defn write-register!
  "Upserts register k = v (k2 mirrors k). Returns v."
  [conn k v]
  (c/execute! conn [(str "insert into " table-name " (k, k2, v) values (?, ?, ?) "
                         "on conflict (k) do update set v = ?") k k v v])
  v)

(defn mop!
  "Executes a micro-op [f k v] on a connection, returning the completed op."
  [test conn [f k v]]
  (Thread/sleep (random/zipf (:mop-delay test)))
  [f k (case f
         :r (read-register conn k)
         :w (write-register! conn k v))])

(defrecord InternalClient []
  c/YSQLYbClient

  (setup-cluster! [this test c]
    (c/execute! c [(str "CREATE TABLE IF NOT EXISTS " table-name
                        "(k INT PRIMARY KEY, k2 INT, v INT)")])
    (c/execute! c [(str "CREATE INDEX " index-name " ON " table-name
                        " (k2) INCLUDE (v)")]))

  (invoke-op! [this test op c]
    (let [txn      (:value op)
          use-txn? (< 1 (count txn))
          txn'     (if use-txn?
                     (c/with-txn test c
                       (mapv (partial mop! test c) txn))
                     (mapv (partial mop! test c) txn))]
      (assoc op :type :ok, :value txn')))

  (teardown-cluster! [this test c]
    (c/drop-table c table-name)))

(c/defclient Client InternalClient)
