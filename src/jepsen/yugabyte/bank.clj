(ns jepsen.yugabyte.bank
  "Simulates transfers between bank accounts"
  (:refer-clojure :exclude [test])
  (:require [jepsen.tests.bank :as bank]))

(defn workload
  [opts]
  (bank/test))

(defn workload-allow-neg
  [opts]
  (bank/test {:negative-balances? true}))
