(ns jepsen.yugabyte.wr
  "Write-read register workload built on Elle's rw-register cycle checker.

  Complements the list-append workload: instead of appending to lists, each
  transaction is a mix of single-key reads and writes of unique values, and we
  look for cycles in the resulting dependency graph. rw-register catches
  anomalies (e.g. via read/write and anti-dependency edges over plain registers)
  that the append datatype cannot express."
  (:require [elle.core :as elle]
            [jepsen.tests.cycle.wr :as wr]))

(defn workload
  "Takes CLI options and constructs a workload: a partial test map."
  [opts]
  (wr/test {:key-count          10
            :max-txn-length     4
            :max-writes-per-key 256
            :consistency-models [(:expected-consistency-model opts)]}))
