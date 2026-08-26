(ns jepsen.yugabyte.nemesis
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer :all]
            [jepsen.control :as c]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.util :as util :refer [meh timeout]]
            [jepsen.nemesis [combined :as nc]]
            [jepsen.yugabyte.db :as db]
            [jepsen.yugabyte.ysql.client :as ysql.client]))

(defrecord MasterNemesis []
  nemesis/Nemesis
  (setup! [this test] this)

  (invoke! [this test op]
    (let [db    (:db test)
          nodes (nc/db-nodes (assoc test :nodes (db/master-nodes test))
                             db
                             (:value op))
          res   (c/with-nodes test nodes
                  (case (:f op)
                    :start  (db/start-master! db test c/*host*)
                    :kill   (db/kill-master!  db)
                    :pause  (db/signal! "yb-master" :STOP)
                    :resume (db/signal! "yb-master" :CONT)))]
      (assoc op :value res)))

  (teardown! [this test])

  nemesis/Reflection
  (fs [this]
    #{:kill :start :pause :resume}))

(defrecord TServerNemesis []
  nemesis/Nemesis
  (setup! [this test] this)

  (invoke! [this test op]
    (let [db (:db test)
          nodes (nc/db-nodes test db (:value op))
          res (c/with-nodes test nodes
                (case (:f op)
                  :start (db/start-tserver! db test c/*host*)
                  :kill  (db/kill-tserver!  db)
                  :pause (db/signal! "yb-tserver" :STOP)
                  :resume (db/signal! "yb-tserver" :CONT)))]
      (assoc op :value res)))

  (teardown! [this test])

  nemesis/Reflection
  (fs [this]
    #{:kill :start :pause :resume}))

(defn role-package
  "Given a role like \"master\" and CLI opts, constructs a combined nemesis
  package for :kill-master, :resume-master, etc."
  [role opts]
  (let [faults (:faults opts)
        faults (cond-> faults
                 (faults (keyword (str "kill-" role)))  (conj :kill)
                 (faults (keyword (str "pause-" role))) (conj :pause))
        pkg (nc/db-package (assoc opts :faults faults))
        pkg (assoc pkg :nemesis (case role
                                  "master"   (MasterNemesis.)
                                  "tserver" (TServerNemesis.)))]
    (nc/f-map #(keyword (str (name %) "-" role)) pkg)))

(defn package
  "Takes CLI opts and constructs a nemesis and generator for the test."
  [opts]
  (let [opts (update opts :faults set)
        packages (concat
                   ; Standard packages
                   (nc/nemesis-packages opts)
                   ; Custom packages
                   [(role-package "master" opts)
                    (role-package "tserver" opts)])]
    (nc/compose-packages packages)))
