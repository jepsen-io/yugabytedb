(ns jepsen.yugabyte.append
  "Values are lists of integers. Each operation performs a transaction,
  comprised of micro-operations which are either reads of some value (returning
  the entire list) or appends (adding a single number to whatever the present
  value of the given list is). We detect cycles in these transactions using
  Jepsen's cycle-detection system."
  (:require [elle.core :as elle]
            [jepsen.tests.cycle.append :as append]))

(defn workload
  "A workload for a list-append test."
  [opts]
  (-> (assoc
        (select-keys opts [:key-count
                           :max-txn-length
                           :max-writes-per-key])
        :consistency-models [(:expected-consistency-model opts)])
      append/test))
