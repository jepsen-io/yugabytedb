(ns jepsen.yugabyte.ycql.bank
  "Bank workload for YugaByteDB. Note that YCQL does not support reads or
  conditional writes in transactions
  (https://docs.yugabyte.com/stable/api/ycql/dml_transaction/), so we can't
  prevent negative balances in this workload."
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen.yugabyte.ycql.client :as c]))

(def keyspace "jepsen")
(def table-name "accounts")

(c/defclient Client keyspace []
  (setup! [this test]
    (c/create-transactional-table
      conn table-name
      {:id          :int
       :balance     :bigint
       :primary-key [:id]})
    (info "Creating accounts")
    (c/with-retry
      (c/insert! conn table-name
                 {:id      (first (:accounts test))
                  :balance (:total-amount test)}
                 ; I'm not sure why they explicitly qualified the keyspace
                 ; *only* for the first account, but my read is that the
                 ; (use-keyspace!) call on client open should carry through
                 ; here.
                 ;:keyspace keyspace
                 )
      (doseq [a (rest (:accounts test))]
        (c/insert! conn table-name
                   {:id a, :balance 0}))))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (case (:f op)
        :read
        (->> ;(c/select conn table-name :keyspace keyspace)
             (c/select conn table-name)
             (map (juxt :id :balance))
             (into (sorted-map))
             (assoc op :type :ok, :value))

        ; The DML transactions page doesn't seem to talk about failure
        ; semantics at all. If a statement throws an error, I *assume* you're
        ; guaranteed that none of the other statements in that transaction take
        ; effect, right?
        :transfer
        (let [{:keys [from to amount]} (:value op)]
          (c/execute!
            conn
            (str "BEGIN TRANSACTION"

                 " UPDATE " table-name
                 " SET balance = balance - " amount " WHERE id = " from
                 " IF EXISTS ELSE ERROR;"

                 " UPDATE " table-name
                 " SET balance = balance + " amount " WHERE id = " to
                 " IF EXISTS ELSE ERROR;"

                 " END TRANSACTION;"))
          (assoc op :type :ok))

        :insert
        (let [{:keys [from to amount]} (:value op)]
          (c/execute!
            conn
            (str "BEGIN TRANSACTION "
                 "INSERT INTO " table-name
                 ; Weirdly,
                 ; https://docs.yugabyte.com/stable/api/ycql/dml_transaction/
                 ; says (twice!) that transactions may not have any IF
                 ; expressions in their INSERTs, UPDATEs, or DELETEs. So... why
                 ; does this not throw?
                 " (id, balance) VALUES (" to "," amount ") IF NOT EXISTS ELSE ERROR;"

                 "UPDATE " table-name
                 " SET balance = balance - " amount " WHERE id = " from ";"
                 "END TRANSACTION;"))
          (assoc op :type :ok)))))

  (teardown! [this test]))
