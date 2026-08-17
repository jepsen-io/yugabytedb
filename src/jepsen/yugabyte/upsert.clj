(ns jepsen.yugabyte.upsert
  ; aphyr, 2028-08-17: Wait, the whole point of an upsert test is to upsert,
  ; but this says it uses ON CONFLICT DO NOTHING, which is *not* an upsert! Is
  ; this now an insert test? TODO: figure out what exactly happened here.
  ;
  ; I swear, the number of hours I waste trying to untangle other people's use
  ; of Claude. Did no one even look at this? It did all its writes to just 20
  ; hardcoded keys, so after the first handful of operations nothing should
  ; have ever succeeded.
  "Concurrent INSERT ... ON CONFLICT DO NOTHING against a small, contended key
  space. Each :upsert reports whether it actually inserted a row. Because the
  primary key makes at most one insert win per key (and DO NOTHING never
  updates), the invariants are:

    1. At most one upsert reports success per key (no duplicate winners / lost
       uniqueness).
    2. Every value read for a key equals the winning insert's value.

  Catches lost-upsert / duplicate-key bugs under contention. Valid at every
  isolation level (uniqueness must hold under read-committed and snapshot too)."
  (:require [jepsen.checker :as checker]
            [jepsen.generator :as gen]))

(defn upserts
  []
  (->> (for [k        (range)
             ; Try five upserts per key
             attempt (range 5)]
         (->> {:f :upsert, :value [k attempt]}))))

(defn reads
  []
  (gen/repeat {:f :read}))

(defn checker
  []
  (reify checker/Checker
    (check [_ test history _]
      (let [oks     (filter #(= :ok (:type %)) history)
            ; key -> set of values whose upsert reported success
            winners (reduce (fn [m op]
                              (if (= :upsert (:f op))
                                (let [[k v inserted?] (:value op)]
                                  (if inserted?
                                    (update m k (fnil conj #{}) v)
                                    m))
                                m))
                            {} oks)
            dup-winners (into {} (filter (fn [[_ vs]] (> (count vs) 1)) winners))
            read-errs   (for [op   oks
                              :when (= :read (:f op))
                              [k v] (:value op)
                              :let  [win (get winners k)]
                              :when (and win (not (contains? win v)))]
                          {:key k, :read v, :expected win})]
        {:valid?            (and (empty? dup-winners) (empty? read-errs))
         :duplicate-winners dup-winners
         :read-errors       (vec read-errs)}))))

(defn workload
  [opts]
  (let [c (or (:concurrency opts)
              (* 2 (count (:nodes opts))))]
    {:concurrency c
     :generator   (gen/reserve (quot c 2) (upserts) (reads))
     :checker     (checker)}))
