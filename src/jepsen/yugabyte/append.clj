(ns jepsen.yugabyte.append
  "Values are lists of integers. Each operation performs a transaction,
  comprised of micro-operations which are either reads of some value (returning
  the entire list) or appends (adding a single number to whatever the present
  value of the given list is). We detect cycles in these transactions using
  Jepsen's cycle-detection system."
  (:require [elle.core :as elle]
            [jepsen.tests.cycle.append :as append]))

(defn workload
  "A workload for a standard append test, with each list stored in a separate
  row."
  [opts]
  (append/test (assoc
                 (select-keys opts [:key-count
                                    :max-txn-length
                                    :max-writes-per-key])
                :consistency-models [(:expected-consistency-model opts)])))


(defn workload-table
  "A workload for a table-based append test, where each list is a table, and
  each element a row."
  [opts]
  (append/test (assoc
                 (select-keys opts [:key-count
                                    :max-txn-length
                                    :max-writes-per-key])
                 :consistency-models [(:expected-consistency-model opts)])))
