(ns jepsen.yugabyte.nemesis
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer :all]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
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
                    :pause  (cu/grepkill! :STOP "yb-master")
                    :resume (cu/grepkill! :CONT "yb-master")))]
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
                  ; Sort of a hack, but... YB masters seem to crash for
                  ; mysterious reasons when we kill tservers. There's nothing
                  ; in the logs and I'm running out of time here, so I'm just
                  ; going to have them restart when the tservers do. This is
                  ; going to make the nemesis plots look wrong, and probably
                  ; confuse someone debugging this later, but I have *got* to
                  ; keep the cluster running somehow or I'm never going to get
                  ; results. :-/
                  :start (do (db/start-master! db test c/*host*)
                             (db/start-tserver! db test c/*host*))
                  :kill  (db/kill-tserver!  db)
                  :pause (cu/grepkill! :STOP "yb-tserver")
                  :resume (cu/grepkill! :CONT "yb-tserver")))]
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
