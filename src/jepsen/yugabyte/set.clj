(ns jepsen.yugabyte.set
  "Adds elements to sets and reads them back"
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [generator :as gen]
                    [independent :as independent]]))

(defn adds
  []
  (->> (range)
       (map (fn [x] {:f :add, :value x}))))

(defn reads
  []
  (gen/repeat {:f :read}))

(defn workload
  [opts]
  (let [c (* 2 (count (:nodes opts)))]
    {; Ideally four groups, to run our four timescales at once
     :concurrency (* 4 c)
     :generator
     ; This is not great--uisng mod k here means that some of the time the
     ; generator gets stuck doing all slow gens and no fast ones. I'm on a
     ; tight schedule though, sorry!
     (independent/concurrent-generator
       c
       (range)
       (fn [k]
         (let [gen (->> (gen/reserve (/ c 2) (adds) (reads))
                        ; This test is quadratic, so don't go *too* large
                        (gen/limit 16384)
                        ; We want to measure this over a variety of timescales:
                        ; 100 us, 1 ms, 10 ms, 100 ms.
                        (gen/stagger (/ (Math/pow 10 (mod k 4))
                                        10000)))]
           (info :gen
                 (with-out-str (binding [*print-length* 8]
                                 (pprint gen))))
           gen)))
     :checker (independent/checker
                (checker/set-full))}))
