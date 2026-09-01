(ns jepsen.yugabyte.db
  "Installs and runs Yugabyte masters and tservers."
  (:require [jepsen [role :as role]]
            [jepsen.yugabyte.db [master :as master]
                                [tserver :as tserver]]))

(defn db
  "Constructs a new database that runs Yugabyte masters and tservers."
  []
  (role/db {:master (master/->DB)
            :tserver {:db (tserver/->DB)
                      :deps [:master]}}))
