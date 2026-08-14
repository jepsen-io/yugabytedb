(ns jepsen.yugabyte.ysql.append
  "Values are lists of integers. Each operation performs a transaction,
  comprised of micro-operations which are either reads of some value (returning
  the entire list) or appends (adding a single number to whatever the present
  value of the given list is). We detect cycles in these transactions using
  Jepsen's cycle-detection system."
  (:require [clojure.string :as str]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [info]]
            [jepsen.random :as random]
            [jepsen.yugabyte.db :as db]
            [jepsen.yugabyte.ysql.client :as c]))

(defn table-count
  "How many tables do we need for a test?"
  [test]
  (:table-count test 5))

(defn table-name
  "Takes an integer and constructs a table name."
  [i]
  (str "append" i))

(defn table-for
  "What table should we use for the given key?"
  [test k]
  (table-name (mod (hash k) (table-count test))))

(def keys-per-row 2)

(defn row-for
  "What row should we use for the given key?"
  [test k]
  (quot k keys-per-row))

(defn col-for
  "What column should we use for the given key?"
  [test k]
  (str "v" (mod k keys-per-row)))

(defn select-with-optional-lock
  [locking col table]
  (let [clause (if (= :pessimistic locking)
                 (random/nth ["" " for update" " for no key update" " for share" " for key share"])
                 "")]
    (str "select (" col ") from " table " where k = ?" clause)))

(defn geo-insert-column
  [test]
  (if (:geo-partition test)
    (str ", geo_partition")
    ""))

(defn insert-primary-geo
  [test conn table col row v geo-row]
  (c/execute! conn
              [(str "insert into " table
                    " (k, k2, " col (geo-insert-column test) ")"
                    " values (?, ?, ?, ?)") row row v geo-row]))

(defn insert-primary
  [conn table col row v]
  (c/execute! conn
              [(str "insert into " table
                    " (k, k2, " col ")"
                    " values (?, ?, ?)") row row v]))

(defn geo-row-update
  [test v]
  (if (:geo-partition test)
    ; aphyr, 2026-08-14: These arguments are at most of size 2. Why on earth
    ; are they using +' here? Some deep magic I don't understand? LLM nonsense?
    (str "and geo_partition = '" (+' (mod v 2) 1) "a'")
    ""))

(defn read-primary
  "Reads a key based on primary key"
  [locking conn table row col]
  (some-> conn
          (c/query [(select-with-optional-lock locking col table) row])
          first
          (get (keyword col))
          (str/split #",")
          (->> ; Append might generate a leading , if the row already exists
            (remove str/blank?)
            (mapv #(Long/parseLong %)))))

(defn append-primary!
  "Writes a key based on primary key."
  [test locking conn table row col v]
  (let [_ (if (= :pessimistic locking)
            (do
              ; Randomly evaluate SELECT FOR UPDATE with timeout in case of
              ; pessimistic locking
              (c/query conn [(select-with-optional-lock locking col table) row])
              (Thread/sleep (long (random/long 2000))))
            nil)
        r (c/execute! conn [(str "update " table
                                 " set " col " = CONCAT(" col ", ',', ?)"
                                 " where k = ? " (geo-row-update test row)) v row])]
    (when (= [0] r)
      ; No rows updated
      (if (:geo-partition test)
        (insert-primary-geo test conn table col row v (str (+' (mod row 2) 1) "a"))
        (insert-primary conn table col row v))) v))

(defn read-secondary
  "Reads a key based on a predicate over a secondary key, k2"
  [conn table row col]
  (some-> conn
          (c/query [(str "select (" col ") from " table
                         " where (k2 * 2) - ? = ?")
                    col col])
          first
          (get (keyword col))
          (str/split #",")
          (->> (mapv #(Long/parseLong %)))))

(defn read-via-index
  "Reads a key using secondary index on k2"
  [locking conn table row col]
  (let [clause (if (= :pessimistic locking)
                 (random/nth ["" " for update" " for no key update" " for share" " for key share"])
                 "")]
    (some-> conn
            (c/query [(str "select (" col ") from " table " where k2 = ?" clause) row])
            first
            (get (keyword col))
            (str/split #",")
            (->>
              (remove str/blank?)
              (mapv #(Long/parseLong %))))))

(defn append-secondary!
  "Writes a key based on a predicate over a secondary key, k2. Returns v."
  [conn table row col v]
  (let [r (c/execute! conn [(str "update " table
                                 " set " col " = CONCAT(" col ", ',', ?) "
                                 "where (k2 * 2) - ? = ?") v row row])]
    (when (= [0] r)
      ; No rows updated
      (c/execute! conn
                  [(str "insert into " table
                        "(k, k2, " col ") values (?, ?, ?)") row row v]))
    v))

(defn mop!
  "Executes a transactional micro-op of the form [f k v] on a connection, where
  f is either :r for read or :append for list append. Returns the completed
  micro-op."
  [locking conn test [f k v]]
  (let [table (table-for test k)
        row (row-for test k)
        col (col-for test k)]
    [f k (case f
           :r
           (let [use-index? (and (not (:geo-partition test))
                                 (random/bool))]
             (info table (if use-index? "IndexScan(k2)" "PrimaryScan(k)")
                   "row=" row)
             (if use-index?
               (read-via-index locking conn table row col)
               (read-primary locking conn table row col)))

           :append
           (append-primary! test locking conn table row col v))]))

(defn create-table-columns-clause
  "Takes a test and returns a vector of columns for use with create-table-ddl."
  [test]
  (if (:geo-partition test)
    [[:k :int]
     [:k2 :int]
     [:geo_partition :varchar]]
    [;[:k :int "unique"]
     [:k :int "PRIMARY KEY"]
     [:k2 :int]]))

(defn table-spec
  [test]
  (if (:geo-partition test)
    "PARTITION BY LIST (geo_partition)"
    ""))

(defn create-partitioning-table
  [c table postfix]
  (info (str "Create table partitions for " table "_" postfix))
  (c/execute! c (str "CREATE TABLE " table "_" postfix " "
                     "PARTITION OF " table " (k, k2, geo_partition"
                     ", PRIMARY KEY (k, geo_partition)) FOR VALUES IN ('"
                     postfix "') "
                     "TABLESPACE " db/tablespace-name "_" postfix)))

(defn resolve-locking
  "Resolves locking mode for a transaction. :mixed randomly picks :optimistic
  or :pessimistic."
  [test]
  (let [locking (:locking test)]
    (if (= :mixed locking)
      (random/nth [:optimistic :pessimistic])
      locking)))

(defrecord InternalClient [isolation]
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (->> (range (table-count test))
         (map table-name)
         (map (fn [table]
                (info "Creating table" table)
                (c/execute! c (j/create-table-ddl
                                table
                                (into
                                  (create-table-columns-clause test)
                                  ; Columns for n values packed in this row
                                  (map (fn [i] [(col-for test i) :text])
                                       (range keys-per-row)))
                                {:conditional? true
                                 :table-spec   (table-spec test)}))
                (if (:geo-partition test)
                  (do (create-partitioning-table c table "1a")
                      (create-partitioning-table c table "2a"))
                  (c/execute! c (str "CREATE INDEX idx_" table " ON "
                                     table " (k2)")))))
         dorun))

  (invoke-op! [this test op c conn-wrapper]
    (let [txn (:value op)
          use-txn? (< 1 (count txn))
          resolved-locking (resolve-locking test)
          txn' (if use-txn?
                 (j/with-db-transaction [c c {:isolation isolation}]
                                        (mapv (partial mop! resolved-locking c test) txn))
                 (mapv (partial mop! resolved-locking c test) txn))]
      (assoc op :type :ok, :value txn'))))

(c/defclient Client InternalClient)
