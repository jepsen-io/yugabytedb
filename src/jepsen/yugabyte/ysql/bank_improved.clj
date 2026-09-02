(ns jepsen.yugabyte.ysql.bank-improved
  (:require [version-clj.core :as v]
            [clojure.tools.logging :refer [info]]
            [clj-commons.slingshot :refer [try+ throw+]]
            [jepsen [random :as rand]]
            [jepsen.yugabyte.util :refer :all]
            [jepsen.yugabyte.ysql.client :as c]))

(def table-name "accounts")
(def table-index "idx_accounts")
(def enable-follower-reads true)
(def minimal-follower-read-version "2.8.0.0-b1")

;
; Single-table bank improved test
;

(defn- read-accounts-map
  "Read {id balance} accounts map from a unified bank table using force index
  flag"
  ([test op c]
   (if (and enable-follower-reads
            (v/newer-or-equal? (:version test) minimal-follower-read-version))
     (c/execute! c ["SET yb_read_from_followers = true"]))
   (->> [(str "/*+ IndexOnlyScan(" table-name " " table-index
              ") */ SELECT id, balance FROM " table-name)]
        (c/execute! op c)
        (map (juxt :id :balance))
        (into (sorted-map)))))

(defn get-balance
  "Gets the balance of the given account."
  [op conn account]
  (c/select-single-value op conn table-name :balance (str "id = " account)))

(defn update-or-throw!
  "Executes an UPDATE statement and throws if it updates zero rows."
  [op conn sql]
  (try (let [r (c/execute! conn sql)]
         (when (zero? (:next.jdbc/update-count (first r)))
           (throw+ {:type :not-found
                    :definite? true}))
         r)))

(defn insert!
  "Inserts a new account, transferring amount from the given account"
  [test op c {:keys [from to amount]}]
  (c/with-txn test c
    (rand/branch
      ; Transfer with a pair of writes only
      (do (update-or-throw!
            op c [(str "UPDATE " table-name
                       " SET balance = balance - ? WHERE id = ?")
                  amount from])
          (mop-delay test)
          (c/execute!
            op c [(str "INSERT INTO " table-name
                       " (id, balance) VALUES (?, ?)")
                  to amount]))

      ; Transfer with separate reads
      (let [b-from (get-balance op c from)
            _      (mop-delay test)
            b-to   (get-balance op c to)]
        (cond
          (nil? b-from)
          (throw+ {:type :not-found, :definite? true})

          (not (nil? b-to))
          (throw+ {:type :already-exists, :definite? true})

          :else
          (let [b-from' (- b-from amount)]
            (c/update! op c table-name {:balance b-from'} ["id = ?" from])
            (mop-delay test)
            (c/insert! op c table-name {:id to, :balance amount})))))))

(defn transfer!
  "Transfers an amount from one account to another."
  [test op c {:keys [from to amount]}]
  (rand/branch
    ; Transfer with a pair of writes only
    (c/with-txn test c
      (update-or-throw!
        op c [(str "UPDATE " table-name
                   " SET balance = balance - ? WHERE id = ?")
              amount from])
      (mop-delay test)
      (update-or-throw!
        op c [(str "UPDATE " table-name
                   " SET balance = balance + ? WHERE id = ?")
              amount to]))

    ; Transfer by reading balances separately, then writing back
    (c/with-txn test c
      (let [b-from (get-balance op c from)
            _      (mop-delay test)
            b-to   (get-balance op c to)]
        (cond ; One account doesn't exist
              (or (nil? b-from) (nil? b-to))
              (throw+ {:type :not-found, :definite? true})

              ; Self-transfer
              (= from to)
              (do (c/update! op c table-name {:balance (- b-from amount)}
                             ["id = ?" from])
                  (mop-delay test)
                  (c/update! op c table-name {:balance b-from}
                             ["id = ?" from]))

              ; Different accounts
              true
              (let [b-from' (- b-from amount)
                    b-to'   (+ b-to amount)]
                (c/update! op c table-name {:balance b-from'} ["id = ?" from])
                (mop-delay test)
                (c/update! op c table-name {:balance b-to'} ["id = ?" to])))))))

(defn delete!
  "Deletes an account, transferring its entire balance to another. Returns a
  completion op."
  [test op c {:keys [from to]}]
  (c/with-txn test c
    (let [b-from (get-balance op c from)
          _      (mop-delay test)
          b-to   (get-balance op c to)]
      (cond
        (or (nil? b-from) (nil? b-to))
        (throw+ {:type :not-found, :definite? true})

        :else
        (let [b-to' (+ b-to b-from)]
          (c/execute! op c [(str "DELETE FROM " table-name " WHERE id = ?")
                            from])
          (mop-delay test)
          (c/update! op c table-name {:balance b-to'} ["id = ?" to])
          (assoc op
                 :type :ok
                 :value {:from from, :to to, :amount b-from}))))))

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

      :insert (do (insert! test op c (:value op))
                  (assoc op :type :ok))

      :transfer (do (transfer! test op c (:value op))
                    (assoc op :type :ok))

      :delete (delete! test op c (:value op))))

  (teardown-cluster! [this test c]
    (c/drop-table c table-name)))

(c/defclient Client InternalClient)
