(ns jepsen.yugabyte.recovery
  "Yugabyte can take a long time to recover from problems, but we don't know
  how long exactly. Here we wrap a nemesis in one which can perform a health
  check using an SQL client, and offer a generator which checks that every node
  is healthy before ending."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.yugabyte.ysql.client :as c]
            [jepsen [generator :as gen]
                    [nemesis :as n]]))

(defrecord HealthCheck [nemesis]
  n/Nemesis
  (setup! [this test]
    (assoc this :nemesis (n/setup! nemesis test)))

  (invoke! [this test op]
    (if (identical? :health-check (:f op))
      (c/with-errors op
        (->> (:nodes test)
             (mapv (fn [node]
                     (future
                       (let [c (c/open test node)]
                         (try
                           (c/execute! c ["SELECT TRUE"])
                           :alive
                           (finally
                             (c/close-conn! c)))))))
             (mapv deref)
             (zipmap (:nodes test))
             (assoc op :value)))
      ; Pass to the regular nemesis
      (n/invoke! nemesis test op)))

  (teardown! [this test]
    (n/teardown! nemesis test)))

(defn nemesis
  "Wraps a nemesis in one that supports :f :health-check. This nemesis tries to
  connect to every node's SQL interface and see if it'll serve a simple query."
  [nemesis]
  (HealthCheck. nemesis))

(defn generator
  "A generator which waits up to (:final-recovery-time opts) seconds for
  YugaByte to come back online."
  [{:keys [final-recovery-time]}]
  (gen/time-limit
    final-recovery-time
    (gen/phases
      (gen/log "Waiting for recovery...")
      (gen/nemesis
        (gen/until (fn [op]
                     (info "Checking" op)
                     (map? (:value op)))
                   (gen/delay 1 (gen/repeat
                                  {:type  :info
                                   :f     :health-check}))))
      (gen/log "Recovery complete!"))))
