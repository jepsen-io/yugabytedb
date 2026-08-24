(ns jepsen.yugabyte.ysql.bank-improved
  (:require [version-clj.core :as v]
            [clojure.tools.logging :refer [info]]
            [jepsen [random :as rand]]
            [jepsen.yugabyte.ysql.client :as c]))

(def table-name "accounts")
(def table-index "idx_accounts")
(def enable-follower-reads true)
(def minimal-follower-read-version "2.8.0.0-b1")

;
; Single-table bank improved test
;

(defn- read-accounts-map
  "Read {id balance} accounts map from a unified bank table using force index flag"
  ([test op c]
   (if (and enable-follower-reads (v/newer-or-equal? (:version test) minimal-follower-read-version))
     (c/execute! c ["SET yb_read_from_followers = true"]))
   (->>
     [(str "/*+ IndexOnlyScan(" table-name " " table-index ") */ SELECT id, balance FROM " table-name)]
     (c/execute! op c)
     (map (juxt :id :balance))
     (into (sorted-map)))))

(defn get-balance
  "Gets the balance of the given account."
  [op conn account]
  (c/select-single-value op conn table-name :balance (str "id = " account)))

(defrecord InternalClient []
  c/YSQLYbClient

  (setup-cluster! [this test c]
    (c/execute! c [(str "CREATE TABLE IF NOT EXISTS " table-name
                        " (id INT PRIMARY KEY, balance BIGINT)")])
    (c/execute! c [(str "CREATE INDEX " table-index " ON " table-name
                        " (id, balance)")])
    (c/with-retry
      (info "Creating accounts")
      (c/insert! c table-name
                 {:id      (first (:accounts test))
                  :balance (:total-amount test)})
      (doseq [acct (rest (:accounts test))]
        (c/insert! c table-name
                   {:id      acct,
                    :balance 0}))))

  (invoke-op! [this test op c]
    (case (:f op)
      :read
      (c/with-txn test c
        (assoc op :type :ok, :value (read-accounts-map test op c)))

      :transfer
      (c/with-txn test c
        (let [{:keys [from to amount]} (:value op)
              b-from (get-balance op c from)
              b-to   (get-balance op c to)]
          (cond ; One account doesn't exist
                (or (nil? b-from) (nil? b-to))
                (assoc op :type :fail)

                ; Self-transfer
                (= from to)
                (do (c/update! op c table-name {:balance (- b-from amount)}
                               ["id = ?" from])
                    (Thread/sleep (rand/zipf 10))
                    (c/update! op c table-name {:balance b-from}
                               ["id = ?" from])
                    (assoc op :type :fail))

                ; Different accounts
                true
                (let [b-from' (- b-from amount)
                      b-to'   (+ b-to amount)]
                  (c/update! op c table-name {:balance b-from'} ["id = ?" from])
                  (c/update! op c table-name {:balance b-to'} ["id = ?" to])
                  (assoc op :type :ok)))))

      :delete
      (c/with-txn test c
        (let [{:keys [from to]} (:value op)
              b-from (get-balance op c from)
              b-to   (get-balance op c to)]
          (cond
            (or (nil? b-from) (nil? b-to))
            (assoc op :type :fail)

            :else
            (let [b-to' (+ b-to b-from)]
              (c/execute! op c [(str "DELETE FROM " table-name " WHERE id = ?")
                                from])
              (Thread/sleep (rand/zipf 10))
              (c/update! op c table-name {:balance b-to'} ["id = ?" to])
                (assoc op
                       :type :ok
                       :value {:from from, :to to, :amount b-from})))))

      :insert
      (c/with-txn test c
        (let [{:keys [from to amount]} (:value op)
              b-from (get-balance op c from)
              b-to   (get-balance op c to)]
          (cond
            (or (nil? b-from) (not (nil? b-to)))
            (assoc op :type :fail)

            :else
            (let [b-from' (- b-from amount)]
              (c/update! op c table-name {:balance b-from'} ["id = ?" from])
              (Thread/sleep (rand/zipf 10))
              (c/insert! op c table-name {:id to, :balance amount})
              (assoc op :type :ok)))))))

  (teardown-cluster! [this test c]
    (c/drop-table c table-name)))

(c/defclient Client InternalClient)
