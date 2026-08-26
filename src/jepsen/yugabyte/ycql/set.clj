(ns jepsen.yugabyte.ycql.set
  (:require [jepsen [independent :as independent]
                    [random :as random]]
            [jepsen.yugabyte.ycql.client :as c]))

(def keyspace "jepsen")
(def table "elements")

; aphyr, 2026-08-26: This client uses counter updates. I'm not sure
; why--there's no duplicate detection in the checker. Maybe the intent was to
; add that later?
(c/defclient Client keyspace []
  (setup! [this test]
    (c/create-table conn table
                    {:key         :int
                     :element     :int
                     :count       :counter
                     :primary-key [:key :element]}))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (let [[key element] (:value op)]
        (case (:f op)
          :add (do (c/update-counter! conn table :count 1
                                      :where [[:= :key key]
                                              [:= :element element]])
                   (assoc op :type :ok))

          :read (->> (c/select conn table
                               :where [[:= :key key]])
                     (mapcat (fn [row]
                               (repeat (:count row) (:element row))))
                     sort
                     (independent/tuple key)
                     (assoc op :type :ok, :value))))))

    (teardown! [this test]))

(def group-count
  "Number of distinct groups for indexing"
  8)

(c/defclient IndexClient keyspace []
  (setup! [this test]
    (c/create-transactional-table conn table
                                  {:key         :int
                                   :element     :int
                                   :grp         :int
                                   :primary-key [:key :element]})
    (c/create-index conn
      "CREATE INDEX IF NOT EXISTS elements_by_group ON elements (key, grp) INCLUDE (element)"))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (let [[key element] (:value op)]
      (case (:f op)
        :add (do (c/insert! conn table
                            {:key key
                             :element element
                             :grp (random/long group-count)})
                 (assoc op :type :ok))

        :read (->> (c/select conn table
                             :columns [:element]
                             :where [[:= :key key]
                                     [:in :grp (range group-count)]])
                   (map :element)
                   sort
                   (independent/tuple key)
                   (assoc op :type :ok, :value))))))

  (teardown! [this test]))
