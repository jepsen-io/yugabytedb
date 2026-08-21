(ns jepsen.yugabyte.single-key-acid
  "Given a single table of hash column primary key and one value column with
  (value of --concurrency divided by 2 and by # of nodes) independent rows,
  verify that concurrent reads, writes and read-modify-write (UPDATE IF)
  operations results in linearizable history.

  Here's the deal. Each group of 2N consequent worker threads allocated by
  --concurrency are assigned to a separate row key. Of these 2N workers, first
  N are performing writes/updates and the last N are reading current state.
  Worker groups (i.e. table rows) are completely independent. To illustrate
  this further, given --concurrency 20 and N = 5:

  - Workers  0 to  9 will be working with row #0
  - Workers 10 to 19 will be working with row #1
  - Workers 0 to 4 and 10 to 14 will be updating their respective rows
  - Workers 5 to 9 and 15 to 19 will be reading their respective rows"
  (:require [clojure [pprint :refer :all]]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent]
            [jepsen.random :as random]
            [knossos.model :as model]
            [jepsen.checker.timeline :as timeline]))

(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn w [_ _] {:type :invoke, :f :write, :value (random/long 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(random/long 5)
                                                 (random/long 5)]})

(defn workload
  [opts]
  (let [n (count (:nodes opts))]
    {:concurrency (* 8 n)
     :generator (independent/concurrent-generator
                  (* 2 n)
                  (range)
                  (fn [k]
                    (->> (gen/reserve n r
                                      (gen/mix [w cas cas]))
                         (gen/process-limit 10)
                         (gen/limit 8192))))
     :checker   (independent/checker
                  (checker/compose
                    {:timeline (timeline/html)
                     :linear   (checker/linearizable
                                 {:model (model/cas-register 0)})}))}))
