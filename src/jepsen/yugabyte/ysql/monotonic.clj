(ns jepsen.yugabyte.ysql.monotonic
  "YSQL client for the monotonic-reads workload (yugabyte.monotonic).

  A single row `monotonic (k int primary key, v bigint)` seeded at v=0.
    :inc  -> UPDATE monotonic SET v = v + 1 WHERE k = 0
    :read -> SELECT v FROM monotonic WHERE k = 0   (coerced to Long)"
  (:require [jepsen.yugabyte.ysql.client :as c]))

(def table-name "monotonic")

(defrecord InternalClient []
  c/YSQLYbClient

  (setup-cluster! [this test c]
    (c/execute! c [(str "CREATE TABLE IF NOT EXISTS " table-name
                        " (k INT PRIMARY KEY, v BIGINT)")])
    (c/execute! c [(str "insert into " table-name " (k, v) values (0, 0) "
                        "on conflict (k) do nothing")]))

  (invoke-op! [this test op c]
    (c/with-txn test c
      (case (:f op)
        :inc
        (do (c/execute! c [(str "update " table-name " set v = v + 1 where k = 0")])
            (assoc op :type :ok))

        :read
        (let [v (-> (c/execute! c [(str "select v from " table-name " where k = 0")])
                    first :v)]
          (assoc op :type :ok, :value (some-> v long))))))

  (teardown-cluster! [this test c]
    (c/drop-table c table-name)))

(c/defclient Client InternalClient)
