(ns yugabyte.auto
  "Shared automation functions for configuring, starting and stopping nodes."
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [clj-http.client :as http]
            [dom-top.core :as dt]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [util :as util :refer [meh timeout]]]
            [jepsen.control.net :as cn]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [yugabyte.ycql.client :as ycql.client]
            [yugabyte.ysql.client :as ysql.client]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import jepsen.os.debian.Debian))

(def dir
  "Where we unpack the Yugabyte package"
  "/opt/yugabyte")

(def master-log-dir  (str dir "/master/logs"))
(def tserver-log-dir (str dir "/tserver/logs"))
(def installed-url-file (str dir "/installed-url"))
(def yugabyted-bin (str dir "/bin/yugabyted"))
(def ui-bin       (str dir "/bin/yugabyted-ui"))
(def master-bin   (str dir "/bin/yb-master"))
(def tserver-bin  (str dir "/bin/yb-tserver"))

(def data-dir (str dir "/data"))
(def log-dir  (str dir "/logs"))

; Community-edition-specific files

(def ce-data-dir data-dir)
(def ce-master-bin      (str dir "/bin/yb-master"))
(def ce-master-log-dir  (str ce-data-dir "/yb-data/master/logs"))
(def ce-master-logfile  (str ce-master-log-dir "/stdout"))
(def ce-master-pidfile  (str dir "/master.pid"))

(def ce-tserver-bin     (str dir "/bin/yb-tserver"))
(def ce-tserver-log-dir (str ce-data-dir "/yb-data/tserver/logs"))
(def ce-tserver-logfile (str ce-tserver-log-dir "/stdout"))
(def ce-tserver-pidfile (str dir "/tserver.pid"))

(def master-port 7100)
(def tserver-port 9100)
(def ysql-port 5433)
(def ycql-port 9042)

(def max-bump-time-ops-per-test
  "Upper bound on number of bump time ops per test, needed to estimate max
  clock skew between servers"
  100)

(defprotocol Auto
  (install!       [db test])
  (configure!     [db test node])
  (start-master!  [db test node])
  (start-tserver! [db test node])
  (stop-master!   [db])
  (stop-tserver!  [db])
  (wipe!          [db]))

(defn yugabyted!
  "Runs a yugabyted command on the current node."
  [& args]
  (c/su
    (c/cd dir
          (apply c/exec yugabyted-bin args))))

(defn yb-admin
  "Runs a yb-admin command on a node. Args are passed to yb-admin."
  [test & args]
  (apply c/exec (str dir "/bin/yb-admin")
         ;:--master_addresses (master-addresses test)
         args))

(defn list-all-masters
  "Asks a node to list all the masters it knows about."
  [test]
  (->> (yb-admin test :list_all_masters)
       (str/split-lines)
       rest
       (map (fn [line]
              (->> line
                   (re-find #"(\w+)\s+([^\s]+)\s+(\w+)\s+(\w+)")
                   next
                   (zipmap [:uuid :address :state :role]))))))

(defn list-all-tservers
  "Asks a node to list all the tservers it knows about."
  [test]
  (->> (yb-admin test :list_all_tablet_servers)
       (str/split-lines)
       rest
       (map (fn [line]
              (->> line
                   (re-find #"(\w+)\s+([^\s]+)")
                   next
                   (zipmap [:uuid :address]))))))

(defn kill-tserver!
  "Kills the tserver"
  []
  (info "Killing tserver")
  (cu/kill-bin! tserver-bin))

(defn kill-master!
  "Kills the master"
  []
  (info "Killing master")
  (cu/kill-bin! master-bin))

(defn kill-ui!
  "Kills the UI process"
  []
  (info "Killing UI")
  (cu/kill-bin! ui-bin))

(defn kill-yugabyted!
  "Kills the yugabyted supervisor"
  []
  (info "Killing yugabyted")
  ;(cu/grepkill! (str "python3 " yugabyted-bin)))
  ; Ugh, fine, it's the only python process running I guess
  (cu/kill-bin! "/usr/bin/python3"))

(defn version
  "Returns a map of version information by calling `bin/yb-master --version`,
  including:

      :version
      :build
      :revision
      :build-type
      :timestamp"
  []
  (try
    (-> #"version (.+?) build (.+?) revision (.+?) build_type (.+?) built at (.+)"
        (re-find (c/exec (str dir "/bin/yb-master") :--version))
        next
        (->> (zipmap [:version :build :revision :build-type :timestamp])))
    (catch RuntimeException e
      ; Probably not installed
      )))

(defn get-installed-url
  "Returns URL from which YugaByte was installed on node"
  []
  (try
    (c/exec :cat installed-url-file)
    (catch RuntimeException e
      ; Probably not installed
      )))

(defn get-download-url
  "Returns URL to tarball for specific released version"
  [version]
  (let [[m version-without-b]
        (re-find #"^(.+)(-b\d+?)$" version)]
    (str "https://software.yugabyte.com/releases/" version-without-b
         "/yugabyte-" version "-linux-x86_64.tar.gz")))

(defn log-files-without-symlinks
  "Takes a directory, and returns a list of logfiles in that direcory, skipping
  the symlinks which end in .INFO, .WARNING, etc."
  [dir]
  (remove (partial re-find #"\.(INFO|WARNING|ERROR)$")
          (try (cu/ls dir {:full-path? true
                           :recursive? true
                           :types [:file]})
               (catch RuntimeException e nil))))

(defn ce-shared-opts
  "Shared options for both master and tserver"
  [node]
  [; Data files!
   :--fs_data_dirs         ce-data-dir
   ; Limit memory to 2GB
   :--memory_limit_hard_bytes 2147483648
   ; Fewer shards to improve perf
   :--yb_num_shards_per_tserver 4
   ; YB can do weird things with loopback interfaces, so... bind explicitly
   :--rpc_bind_addresses (cn/ip node)
   ; Seconds before declaring an unavailable node dead and initiating a raft
   ; membership change
   ;:--follower_unavailable_considered_failed_sec 10
   ; Clock skew threshold
   ; :--max_clock_skew_usec 1
   ])

(defn master-api-opts
  "API-specific options for master"
  [api node]
  (if (= api :ysql)
    [:--use_initial_sys_catalog_snapshot]
    []))

(defn tserver-api-opts
  "API-specific options for tserver"
  [api node]
  (if (= api :ysql)
    [:--start_pgsql_proxy
     :--pgsql_proxy_bind_address (cn/ip node)]
    []))

(def limits-conf
  "Ulimits, in the format for /etc/security/limits.conf."
  "
* hard nofile 1048576
* soft nofile 1048576")

(defn start-opts
  "Common options we pass on `yugabyted start`."
  [test node]
  ["--base_dir"          dir
   ; Let's say each node is its own region and zone
   "--cloud_location"    (str "jepsen." node "." node)
   "--advertise_address" (cn/local-ip)
   "--fault_tolerance"   "zone"
   "--tserver_flags"     "enable_ysql_conn_mgr=true"
   ])

(defrecord YugaByteDB
  []
  Auto
  (install! [db test]
    (c/su
      (c/cd dir
            ; Post-install takes forever, so let's try and skip this on
            ; subsequent runs
            (let [url           (or (:url test)
                                    (get-download-url (:version test)))
                  installed-url (get-installed-url)]
              (when-not (= url installed-url)
                (info "Old version" installed-url "should be replaced by" url)
                (debian/install ["python3"])

                (info "Installing tarball")
                (cu/install-archive! url dir)

                (cu/write-file! url installed-url-file)
                (info "Done with setup"))))))

  (configure! [db test node]
    ; YB will explode after creating just a handful of tables if we don't raise
    ; ulimits. This is sort of a hack; it won't take effect for the current
    ; session, but will on the second and subsequent runs. We can't run
    ; `ulimit` directly because the shell context doesn't carry over to
    ; subsequent commands. Should write a subshell exec thing to handle this at
    ; some point.
    (c/su (c/exec :echo limits-conf :> "/etc/security/limits.d/jepsen.conf")))

  (stop-master! [db]
    (c/su (cu/stop-daemon! ce-master-bin ce-master-pidfile)))

  (stop-tserver! [db]
    (c/su (cu/stop-daemon! ce-tserver-bin ce-tserver-pidfile))
    (c/su (cu/grepkill! "postgres")))

  (wipe! [db]
    (c/su (c/exec :rm :-rf
                  data-dir
                  log-dir)))

  db/DB
  (setup! [db test node]
    (install! db test)
    (configure! db test node)
    (let [ip      (cn/local-ip)
          primary (jepsen/primary test)]
      ; Start one node
      (when (= node primary)
        (yugabyted! "start"
                    (start-opts test node))
        (cu/await-tcp-port ip master-port {})
        (cu/await-tcp-port ip tserver-port {})
        (info "First node started"))

      (jepsen/synchronize test)

      ; Join other nodes
      (when (not= node primary)
        ; You can't join more than a couple nodes concurrently or YB will give
        ; up, complaining that it ran out of retries after too many "Leader
        ; is not ready for Config Change" errors. <sigh>
        (locking db
          (yugabyted! "start"
                      (start-opts test node)
                      (str "--join=" (cn/ip primary)))
          ; Wait for client ports
          (cu/await-tcp-port ip ycql-port {})
          (cu/await-tcp-port ip ysql-port {})
          (info "Started")))))

  (teardown! [db test node]
    (db/kill! db test node)
    (wipe! db))

  db/Primary
  (setup-primary! [this test node])
  (primaries [this test]
    ; TODO: find Raft leaders
    [])

  db/Process
  (kill! [this test node]
    (kill-yugabyted!)
    (dt/disorderly
      (kill-tserver!)
      (kill-master!)
      (kill-ui!)))

  (start! [this test node]
    (yugabyted! "start" (start-opts test node)))

  db/LogFiles
  (log-files [_ _ _]
    (concat (log-files-without-symlinks log-dir)
            [ce-master-logfile
             ce-tserver-logfile]
            (log-files-without-symlinks ce-master-log-dir)
            (log-files-without-symlinks ce-tserver-log-dir))))

(defn running-masters
  "Returns a list of nodes where master process is running."
  [nodes]
  (->> nodes
       (pmap (fn [node]
               (try
                 (let [is-running
                       (-> (str "http://" node ":7000/jsonmetricz")
                           (http/get)
                           :status
                           (= 200))]
                   [node is-running])
                 (catch Exception e [node false])
                 )))
       (filter second)
       (map first)))
