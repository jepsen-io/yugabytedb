(ns jepsen.yugabyte.set
  "Adds elements to sets and reads them back"
  (:require [jepsen.generator :as gen]
            [jepsen.checker :as checker]))

(defn adds
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :add, :value x}))
       (map gen/once)))

(defn reads
  []
  {:type :invoke, :f :read, :value nil})

(defn workload
  [opts]
  (let [c (or (:concurrency opts)
              (* 2 (count (:nodes opts))))]
    {:concurrency c
     :generator (gen/reserve (/ c 2) (adds)
                             reads)
     :checker   (checker/set-full)}))
