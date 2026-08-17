(ns jepsen.yugabyte.types
  "Writes numeric boundary values into a bigint column and reads them back,
  checking to make sure that they round-tripped correctly."
  (:require [jepsen [checker :as checker]
                    [history :as h]
                    [generator :as gen]]))

(def special-values
  "A variety of interesting 64-bit integers."
  [0 1 -1
   2147483647            ; Integer/MAX_VALUE
   2147483648            ; Integer/MAX_VALUE + 1
   -2147483648           ; Integer/MIN_VALUE
   -2147483649           ; Integer/MIN_VALUE - 1
   4294967296            ; 2^32
   9223372036854775807   ; Long/MAX_VALUE
   -9223372036854775808]) ; Long/MIN_VALUE

(defn writes
  "For each special value with index `i`, writes that value to key `i` until
  successful."
  []
  (map-indexed (fn [i v]
                 (gen/until-ok {:f :write, :value [i v]}))
               special-values))

(defn reads
  []
  "On each thread, a single ok read."
  (gen/each-thread (gen/until-ok {:f :read})))

(defn checker
  []
  (reify checker/Checker
    (check [_ test history _]
      (let [errs (->> history
                      h/oks
                      (h/filter (h/has-f? :read))
                      (mapcat
                        (fn [{:keys [value] :as op}]
                          (filter (fn [[k v]]
                                    (let [expected (nth special-values k)]
                                      (when (not= expected v)
                                        {:key k
                                         :expected expected
                                         :actual v
                                         :op op})))
                                  value))))]
        {:valid? (empty? errs)
         :errs   (vec errs)}))))

(defn workload
  [opts]
  {:generator [(writes) (reads)]
   :checker   (checker)})
