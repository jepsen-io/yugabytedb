(ns jepsen.yugabyte.db
  "Shared automation functions for configuring, starting and stopping nodes.

  aphyr, 2026-08-13: Claude has been here; treat with suspicion."
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer :all]
            [clojure.data.json :as json]
            [clj-http.client :as http]
            [dom-top.core :as dt]
            [jepsen.control :as c]
            [jepsen.db :as db]
            [jepsen.util :as util :refer [meh]]
            [jepsen.random :as random]
            [jepsen.control.net :as cn]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.yugabyte [util :refer [parse-version]]]
            [version-clj.core :as v]
            [jepsen.yugabyte.ycql.client :as ycql.client]
            [jepsen.yugabyte.ysql.client :as ysql.client :refer [ysql-port]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import jepsen.os.debian.Debian
           jepsen.os.centos.CentOS))

(def dir
  "Where we unpack the Yugabyte package"
  "/home/yugabyte")

(def master-log-dir (str dir "/master/logs"))
(def tserver-log-dir (str dir "/tserver/logs"))
(def installed-url-file (str dir "/installed-url"))

; Community-edition-specific files
(def ce-data-dir (str dir "/data"))

(def ce-master-bin (str dir "/bin/yb-master"))
(def ce-master-log-dir (str ce-data-dir "/yb-data/master/logs"))
(def ce-master-logfile (str ce-master-log-dir "/stdout"))
(def ce-master-pidfile (str dir "/master.pid"))

(def ce-tserver-bin (str dir "/bin/yb-tserver"))
(def ce-tserver-log-dir (str ce-data-dir "/yb-data/tserver/logs"))
(def ce-tserver-logfile (str ce-tserver-log-dir "/stdout"))
(def ce-tserver-pidfile (str dir "/tserver.pid"))

; Versions where things changed
(def minimal-packed-version "2.16.4.0-b1")
(def minimal-skip-prefix-locks-version
  "skip_prefix_locks gflag was introduced in 2026.1; older clusters fail to
  start when it is set."
  "2026.1.0.0-b0")

(def tablespace-name "geo_tablespace")

(defprotocol Auto
  (install! [db test])
  (configure! [db test node])
  (start-master! [db test node])
  (start-tserver! [db test node])
  (stop-master! [db])
  (stop-tserver! [db])
  (wipe! [db]))

(defmacro suppress-interrupted-exception
  "When there's an error encountered on one of the node, the whole cluster worker thread group
  is interrupted (see dom-top.core/real-pmap-helper). This is likely to interrupt a bunch of waits
  and sleeps in SSH connection helpers, which would cause a lot of noise in the log.
  Same happens when the Ctrl+C is pressed.

  Since interruption only happens on error, we can safely suppress those InterruptedExceptions -
  execution as a whole will error out anyway."
  [& body]
  `(try+
     (do ~@body)
     (catch InterruptedException e#
       (info "Interrupted, probably an error happened on another node"))))

(defn master-nodes
  "Given a test, returns the nodes we run masters on."
  [test]
  (let [nodes (take (:replication-factor test)
                    (:nodes test))]
    (assert (= (count nodes) (:replication-factor test))
            (str "We need at least "
                 (:replication-factor test)
                 " nodes as masters, but test only has nodes: "
                 (pr-str (:nodes test))))
    nodes))

(defn master-node?
  "Is this node a master?"
  [test node]
  (some #{node} (master-nodes test)))

(defn master-addresses
  "Given a test, returns a list of master addresses, like \"10.0.0.1:7100,10.0.0.2:7100,...\""
  [test]
  (assert (coll? (:nodes test)))
  (->> (master-nodes test)
       (take (:replication-factor test))
       (map #(str (cn/ip %) ":7100"))
       (str/join ",")))

(defn yb-admin
  "Runs a yb-admin command on a node. Args are passed to yb-admin."
  [test & args]
  (apply c/exec (str dir "/bin/yb-admin")
         :--master_addresses (master-addresses test)
         args))

(defn ysqlsh
  "Runs a ysqlsh command on a node. Args are passed to ysqlsh."
  [test node & args]
  (let [args (concat args
                     [:-h (cn/ip node)
                      :--port (if (:connection-manager test)
                                5431
                                5433)])]
    (info "/bin/ysqlsh" args)
    (apply c/exec (str dir "/bin/ysqlsh")
           args)))

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
  "Asks a node to list all the tservers it knows about. Columns are
  `UUID  RPC-Host/Port  Heartbeat-delay  Status ...`, so we capture the status
  as :state (ALIVE/DEAD) - without it the readiness filter in await-tservers has
  nothing to match on and never counts a tserver as up."
  [test]
  (->> (yb-admin test :list_all_tablet_servers)
       (str/split-lines)
       rest
       (map (fn [line]
              (->> line
                   (re-find #"(\w+)\s+([^\s]+)\s+([^\s]+)\s+(\w+)")
                   next
                   (zipmap [:uuid :address :heartbeat :state]))))))

(defn create-geo-tablespace
  [node tablespace-name replica-placement]
  (info "Creating tablespace" tablespace-name)
  (ysqlsh test node
          :-c (str "CREATE TABLESPACE " tablespace-name " "
                   "WITH (replica_placement='"
                   (json/write-str replica-placement) "');")))

(defn setup-geo-partition
  [node tablespace-name]
  (do
    (create-geo-tablespace
      node
      (str tablespace-name "_1a")
      {
       :num_replicas     2
       :placement_blocks [
                          {
                           :cloud             :ybc
                           :region            :jepsen-1
                           :zone              :jepsen-1a
                           :min_num_replicas  1
                           :leader_preference 1
                           }
                          ]
       })
    (create-geo-tablespace
      node
      (str tablespace-name "_2a")
      {
       :num_replicas     2
       :placement_blocks [
                          {
                           :cloud             :ybc
                           :region            :jepsen-2
                           :zone              :jepsen-2a
                           :min_num_replicas  1
                           :leader_preference 1
                           }
                          ]
       })))

(defn master-voter?
  "True if a `list-all-masters` entry is a committed voting member of the master
  Raft group: it is ALIVE and currently acting as LEADER or FOLLOWER. A master
  that is still being added to the config shows up as a LEARNER (Raft PRE_VOTER)
  and is not yet a dependable quorum member."
  [master]
  (and (= "ALIVE" (:state master))
       (contains? #{"LEADER" "FOLLOWER"} (:role master))))

(defn masters-converged?
  "True when the master Raft group has converged for this test: every expected
  master is a voting member and exactly one leader has been elected.

  NB: the old check compared (count (master-addresses test)) - the LENGTH of the
  joined address string, e.g. 59 - against the ALIVE master count, so it could
  never succeed and await-masters always timed out."
  [test]
  (let [masters (list-all-masters test)]
    ;(info :masters (with-out-str (pprint masters)))
    (and (= (count (master-nodes test))
            (count (filter master-voter? masters)))
         (= 1 (count (filter (comp #{"LEADER"} :role) masters))))))

(defn await-masters
  "Blocks until the master Raft group has converged: every expected master is an
  ALIVE voting member and a single leader has been elected. Retries through the
  transient errors that occur while masters are still starting up and electing a
  leader."
  [test]
  (dt/with-retry [tries 60]
    (when (zero? tries)
      (throw (RuntimeException. "Giving up waiting for masters to converge.")))

    (Thread/sleep 1000)

    (if (masters-converged? test)
      :ready
      (do (info "Waiting for masters to converge...")
          (retry (dec tries))))

    (catch RuntimeException e
      (if (some #(re-find % (.getMessage e))
                [#"Could not locate the leader master"
                 #"Timed out"
                 #"Leader not yet ready to serve requests"
                 #"Leader not yet replicated NoOp"
                 #"Not the leader"
                 #"This leader has not yet acquired a lease"])
        (do (info "Waiting for masters:" (.getMessage e))
            (retry (dec tries)))
        (throw e)))))

(defn await-tservers
  "Waits until all tservers for a test are online, according to this node."
  [test]
  (dt/with-retry [tries 60]
    (when (zero? tries)
      (throw (RuntimeException. "Giving up waiting for tservers.")))

    (Thread/sleep 1000)

    (if (= (count (:nodes test))
           (->> (list-all-tservers test)
                (filter (comp #{"ALIVE"} :state))
                count))
      :ready
      (do (info "Waiting for tservers")
          (retry (dec tries))))

    (catch RuntimeException e
      (condp re-find (.getMessage e)
        #"Leader not yet ready to serve requests" (retry (dec tries))
        #"This leader has not yet acquired a lease" (retry (dec tries))
        #"Could not locate the leader master" (retry (dec tries))
        #"Leader not yet replicated NoOp" (retry (dec tries))
        #"Not the leader" (retry (dec tries))
        (throw e)))))

(defn await-ysqlsh
  "Blocks until we can execute commands using ysqlsh."
  [test node]
  (util/await-fn (fn [] (ysqlsh test node :-c (str "SELECT TRUE;")))
                 {:retry-interval 500
                  :log-interval 10000
                  :log-message "Waiting for YSQL"
                  :timeout 120000}))

(defn start!
  "Start both master and tserver. Only starts master if this node is a master
  node. Waits for masters and tservers."
  [db test node]
  (info "Starting master and tserver for" (name (:api test)) "API")

  (when (master-node? test node)
    (start-master! db test node)
    (await-masters test))

  (start-tserver! db test node)
  (await-tservers test)

  (await-ysqlsh test node)

  (case (:api test)
    :ycql
    (ycql.client/await-setup node)

    :ysql
    (cu/await-tcp-port (cn/ip node) (ysql-port test) {}))

  :started)

(defn stop! [db test node]
  "Stop both master and tserver. Only stops master if this node needs to."
  (stop-tserver! db)
  (when (master-node? test node)
    (stop-master! db))
  :stopped)

(defn signal!
  "Sends a signal to a named process by signal number or name."
  [process-name signal]
  (meh (c/su (c/exec :pkill :--signal signal process-name)))
  :signaled)

(defn kill!
  "Kill a process forcibly."
  [process]
  (signal! process 9)
  (c/exec (c/lit (str "! ps -ce | grep " process)))
  (info process "killed")
  :killed)

(defn kill-tserver!
  "Kills the tserver"
  [db]
  (kill! "yb-tserver")
  (stop-tserver! db))

(defn kill-master!
  "Kills the master"
  [db]
  (kill! "yb-master")
  (stop-master! db))

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

(defn download-url
  "Returns URL to tarball for a test."
  [test]
  (or (:url test)
      (let [{:keys [short full]} (parse-version (:version test))]
        (str "https://software.yugabyte.com/releases/" short
             "/yugabyte-" full "-linux-x86_64.tar.gz"))))

(defn log-files-without-symlinks
  "Takes a directory, and returns a list of logfiles in that direcory, skipping
  the symlinks which end in .INFO, .WARNING, etc."
  [dir]
  (remove (partial re-find #"\.(INFO|WARNING|ERROR)$")
          (try (cu/ls dir {:full-path? true})
               (catch RuntimeException e nil))))


(defn basic-flags
  "Shared configuration flags for both master and tserver"
  [node]
  [; Data files!
   :--fs_data_dirs ce-data-dir
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
   ; Disable YugaByte call-home analytics
   :--callhome_enabled=false
   ])

(defn packed-columns-flags
  "TODO: what is this?"
  [test]
  (if (and (v/newer-or-equal? (:version test) minimal-packed-version)
           (:yb-packed-columns-enabled test))
    [:--ysql_enable_packed_row]
    [])
  )

(defn master-api-flags
  "API-specific options for master"
  [api node]
  (if (= api :ysql)
    [:--use_initial_sys_catalog_snapshot]
    []))

(defn tserver-api-flags
  "API-specific options for tserver"
  [test node]
  (if (:connection-manager test)
    [:--start_pgsql_proxy
     :--pgsql_proxy_bind_address (str (cn/ip node))
     :--ysql_conn_mgr_port 5431
     ]
    [:--start_pgsql_proxy
     :--pgsql_proxy_bind_address (cn/ip node)
     ]))

(defn tserver-read-committed-flags
  "Read committed specific flags"
  [test]
  (if (= :read-committed (:isolation test))
    [:--yb_enable_read_committed_isolation]
    []))

(defn tserver-serializable-flags
  "Serializable-isolation specific flags. skip_prefix_locks only exists in
  2026.1+; setting it on older versions makes the cluster fail to start."
  [test]
  (if (and (= :serializable (:isolation test))
           (v/newer-or-equal? (:version test) minimal-skip-prefix-locks-version))
    [:--skip_prefix_locks=false]
    []))

(defn tserver-table-lock-flags
  "append-table workload flags: transactional DDL, table-level object locking,
  and concurrent DDL. All three are preview-gated, so they're added to
  allowed_preview_flags_csv (apply-extra-gflags collapses this into a single
  flag alongside any other preview flags, e.g. connection manager)."
  [test]
  ; aphyr, 2026-08-14: is the implication here that this test does not pass
  ; unless you specify these options? I suspect the answer is yes:
  ; https://docs.yugabyte.com/stable/explore/transactions/explicit-locking/
  ; suggests that DML and DDL could run concurrently and maybe that messes
  ; things up?
  (if (:table-locks test)
    [:--allowed_preview_flags_csv "enable_object_locking_for_table_locks,ysql_yb_ddl_transaction_block_enabled,ysql_enable_concurrent_ddl"
     :--ysql_yb_ddl_transaction_block_enabled
     :--enable_object_locking_for_table_locks
     :--ysql_enable_concurrent_ddl]
    []))

(defn master-table-lock-flags
  "Object locking is coordinated through the master, so the table-lock flag and
  its preview allow-list entry must be present on the master as well."
  [test]
  (if (:table-locks test)
    [:--allowed_preview_flags_csv "enable_object_locking_for_table_locks,ysql_yb_ddl_transaction_block_enabled"
     :--ysql_yb_ddl_transaction_block_enabled
     :--enable_object_locking_for_table_locks]
    []))

(defn get-random-node-skew
  [max_skew node_ip]
  (random/long max_skew))

(def get-node-skew
  (memoize get-random-node-skew))

(defn random-clock-skew-flags
  "Enable random clock skew

  max-skew parameter is less than (490 / (tservers + master))
  as a result we should avoid random -500 skews in all masters e.g.

  half-skew is needed to generate negative skews"
  [test node]
  (if (:clock-skew-flags test)
    (let [max-skew (int (/ 490 (count (:nodes test))))
          host-skew (if (:extreme-skew test)
                      (get-random-node-skew max-skew (cn/ip node))
                      (get-node-skew max-skew (cn/ip node)))
          half-skew (int (/ max-skew 2))]
      [:--time_source (format "skewed,%s" (- host-skew half-skew))])
    []))


(defn tserver-connection-manager-preview-flags
  "Preview flags for connection manager feature"
  [test]
  (if (:connection-manager test)
    [:--allowed_preview_flags_csv "enable_ysql_conn_mgr"
     :--enable_ysql_conn_mgr]
    []))

(defn geo-partitioning-flags
  "Geo partitioning specific mapping flags
  Each node will be mapped to id in [1 2] and then used in each node."
  [test node nodes]
  (if (:geo-partition test)
    (let [geo-ids (map #(+ 1 (mod % 2)) (range (count nodes)))
          geo-node-map (zipmap nodes geo-ids)
          node-id-int (get geo-node-map node)]
      (info node [:--placement_cloud :ybc
                  :--placement_region (str "jepsen-" node-id-int)
                  :--placement_zone (str "jepsen-" node-id-int "a")])
      [:--placement_cloud :ybc
       :--placement_region (str "jepsen-" node-id-int)
       :--placement_zone (str "jepsen-" node-id-int "a")])
    []))


(defn tserver-heartbeat-flags
  "Heartbeat tracing flags"
  [test]
  (if (:heartbeat-flags test)
    [:--heartbeat_interval_ms 100
     :--heartbeat_rpc_timeout_ms 1500
     :--retryable_rpc_single_call_timeout_ms 2000
     :--rpc_connection_timeout_ms 1500
     :--leader_failure_exp_backoff_max_delta_ms 1000
     :--leader_failure_max_missed_heartbeat_period 3
     :--consensus_rpc_timeout_ms 300
     :--client_read_write_timeout_ms 6000]
    []))

(defn experimental-tuning-flags
  "Speed up recovery from partitions and crashes. Right now it looks like
  these actually make the cluster slower to, or unable to, recover."
  [test]
  (if (:experimental-tuning-flags test)
    [:--client_read_write_timeout_ms 2000
     :--leader_failure_max_missed_heartbeat_periods 2
     :--leader_failure_exp_backoff_max_delta_ms 5000
     :--rpc_default_keepalive_time_ms 5000
     :--rpc_connection_timeout_ms 1500]
    []))

(defn perf-flags
  "Common performance-related flags. YB basically falls over if you have any
  kind of contended workload, and most of these workloads exist specifically to
  test contention. I'm seeing routine 30-second latencies for small
  transactions and throughput of ~1 txn/sec. It's rooough."
  [test]
  [; Poll txn coordinators more often for the status of blocking
   ; transactions. I think this helps reduce latencies a bit?
   :--wait_queue_poll_interval_ms 10])

(defn master-perf-flags
  "Master performance-related flags."
  [test]
  (into (perf-flags test)
        []))

(defn tserver-perf-flags
  "TServer performance-related flags we use to try and improve YB latencies."
  [test]
  (into (perf-flags test)
        [; Reduce retries to try and get a handle on ridiculous latencies
         :--ysql_pg_conf_csv "yb_max_query_layer_retries=5"]))

(defn stress-flags
  "Shared stress-test flags for master and tserver.
  Disabled flags are commented with the reason — re-enable after verifying startup."
  [test]
  (if (:stress-tuning test)
    [; WAL: 512KB segments — may be too small for catalog bootstrap
     ; :--log_segment_size_bytes 524288
;     :--consensus_max_batch_size_bytes 65536        ; 64KB — smaller replication batches
      :--bg_superblock_flush_interval_secs 5
     ]
    []))

(defn master-stress-flags
  "Stress-test flags for master: tablet splitting.
  Disabled — tiny thresholds cause split storms during bootstrap."
  [test]
  (into (stress-flags test)
        (if (:stress-tuning test)
          [; :--enable_automatic_tablet_splitting true
;      :--tablet_split_low_phase_size_threshold_bytes 1024
;      :--tablet_split_high_phase_size_threshold_bytes 4096
;      :--tablet_force_split_threshold_bytes 8192
           ]
          [])))

(defn tserver-stress-flags
  "Stress-test flags for tserver — DocDB, RocksDB, MVCC, intent cleanup."
  [test]
  (into (stress-flags test)
        (if (:stress-tuning test)
          [:--txn_max_apply_batch_records 5
;      :--db_write_buffer_size 524288
           :--db_block_cache_size_bytes 8388608
;     :--aborted_intent_cleanup_ms 1000
           :--timestamp_history_retention_interval_sec 5
;     :--transaction_deadlock_detection_interval_usec 1000000
           :--backfill_index_write_batch_size 10
     ; :--cdc_stream_records_threshold_size_bytes 1024
           ]
          [])))

(defn parse-gflag
  "Parse a gflag spec 'flag_name=value' into [flag-name value], or
  'flag_name' into [flag-name nil] for boolean flags."
  [spec]
  (let [idx (.indexOf ^String spec (int \=))]
    (if (pos? idx)
      [(subs spec 0 idx) (subs spec (inc idx))]
      [spec nil])))

(defn pg-conf-flag?
  "Returns true if flag-name is a pg_conf-style flag whose values should be
  merged rather than overwritten."
  [flag-name]
  (str/includes? flag-name "ysql_pg_conf_csv"))

(defn merge-pg-conf-csv
  "Merge two pg_conf_csv value strings. Settings in `override` take precedence
  over those in `base`. Each string is a CSV of key=value pairs."
  [base override]
  (let [parse (fn [s]
                (when (seq s)
                  (into {}
                        (map (fn [pair]
                               (let [[k v] (str/split pair #"=" 2)]
                                 [k v]))
                             (str/split s #",")))))
        merged (merge (parse base) (parse override))]
    (str/join "," (map (fn [[k v]] (str k "=" v)) merged))))

(defn preview-flags-csv-flag?
  "Returns true if flag-name is (aphyr, 2026-08-13: Uh... that's not what this
  does) allowed_preview_flags_csv, whose value is (aphyr: what???) a plain CSV
  list of flag names that must be unioned rather than overwritten: gflags is
  last-wins, so two occurrences would silently drop earlier entries (e.g.
  enable_ysql_conn_mgr) and make YB reject the preview flag at startup."
  [flag-name]
  (str/includes? flag-name "allowed_preview_flags_csv"))

(defn merge-csv-list
  "Merge two plain CSV list strings into a deduplicated union, preserving the
  order of first appearance."
  [base override]
  (->> (concat (str/split (or base "") #",")
               (str/split (or override "") #","))
       (map str/trim)
       (remove empty?)
       distinct
       (str/join ",")))

(defn merge-fn-for
  "Returns the CSV merge function for a flag name (without the leading --), or
  nil for a regular flag. pg_conf merges key=value settings; preview-flag lists
  are unioned."
  [flag-name]
  (cond
    (pg-conf-flag? flag-name)           merge-pg-conf-csv
    (preview-flags-csv-flag? flag-name) merge-csv-list
    :else                               nil))

(defn collapse-csv-flags
  "Collapse repeated CSV flags (pg_conf, allowed_preview_flags) in a flattened
  flag vector into a single merged occurrence at the position of the first one,
  preserving the order of every other flag. Different parts of the flag list
  (e.g. connection manager and append-table) each emit their own
  allowed_preview_flags_csv; gflags is last-wins, so leaving the duplicates
  would silently drop entries and make YB reject the preview flag at startup."
  [flat]
  (loop [items (seq flat), out [], seen {}]
    (if-not items
      out
      (let [x        (first items)
            merge-fn (when (keyword? x) (merge-fn-for (subs (name x) 2)))]
        (if (and merge-fn (next items))
          (let [v (str (second items))]
            (if-let [i (get seen x)]
              (recur (nnext items)
                     (update out (inc i) #(merge-fn (str %) v))
                     seen)
              (recur (nnext items)
                     (conj out x v)
                     (assoc seen x (count out)))))
          (recur (next items) (conj out x) seen))))))

(defn apply-extra-gflags
  ; aphyr, 2028-08-17: I think this is more Claude nonsense. This does not
  ; apply any extra flags at all! It seems to merge flags together?
  "Apply extra gflags to a flag vector built by start-master!/start-tserver!.
  The flag vector is first flattened and its CSV flags collapsed (so multiple
  features can each emit allowed_preview_flags_csv safely). Regular extra flags
  are then appended (YugaByteDB uses last-wins); extra CSV flags are merged into
  the existing occurrence instead of producing a duplicate."
  [flag-vec extra-specs]
  (let [flat (collapse-csv-flags (vec (flatten flag-vec)))]
    (reduce
      (fn [acc [flag-name value]]
        (let [kw       (keyword (str "--" flag-name))
              merge-fn (merge-fn-for flag-name)]
          (if (and value merge-fn)
            ;; CSV flag — find existing and merge, or append
            (let [idx (.indexOf acc kw)]
              (if (and (>= idx 0) (< (inc idx) (count acc)))
                (assoc acc (inc idx) (merge-fn (str (get acc (inc idx))) value))
                (conj acc kw value)))
            ;; Regular flag — append (last-wins). Render as a single
            ;; --flag=value token: `--flag value` is invalid for boolean
            ;; gflags (the value is left as a stray positional and YB aborts
            ;; with "Error parsing command-line flags"). Matches the
            ;; --skip_prefix_locks=false style used elsewhere.
            (if value
              (conj acc (keyword (str "--" flag-name "=" value)))
              (conj acc kw)))))
      flat
      (map parse-gflag extra-specs))))

(def limits-conf
  "Ulimits, in the format for /etc/security/limits.conf."
  "
* hard nofile 1048576
* soft nofile 1048576")

(defrecord YugaByteDB
  []
  Auto
  (install! [db test]
    (c/su
      (c/cd dir
            (let [url           (download-url test)
                  installed-url (get-installed-url)]
              ; Post-install takes forever, so let's try and skip this on
              ; subsequent runs
              (when-not (= url installed-url)
                (info "Replacing version" installed-url "with" url)
                (debian/install ["python3"])
                (assert (re-find #"Python 3"
                                 (c/exec :python3 :--version (c/lit "2>&1"))))

                (info "Installing tarball into" dir)
                (cu/install-archive! url dir)
                (c/su (let [post-install-script-path "./bin/post_install.sh"]
                        (info "Post-install script")

                        (assert (cu/exists? post-install-script-path)
                                "Post-install script does not exist!")
                        (c/exec post-install-script-path)

                        (c/exec :echo url :>> installed-url-file)
                        (info "Done with setup"))))))))

  (configure! [db test node]
    ; YB will explode after creating just a handful of tables if we don't raise
    ; ulimits. This is sort of a hack; it won't take effect for the current
    ; session, but will on the second and subsequent runs. We can't run
    ; `ulimit` directly because the shell context doesn't carry over to
    ; subsequent commands. Should write a subshell exec thing to handle this at
    ; some point.
    (c/su (c/exec :echo limits-conf :> "/etc/security/limits.d/jepsen.conf")))

  (start-master! [db test node]
    (c/su (c/exec :mkdir :-p ce-master-log-dir)
          (apply cu/start-daemon!
            {:logfile ce-master-logfile
             :pidfile ce-master-pidfile
             :chdir   dir}
            ce-master-bin
            (apply-extra-gflags
              [:--master_addresses (master-addresses test)
               :--replication_factor (:replication-factor test)
               (basic-flags node)
               (experimental-tuning-flags test)
               (geo-partitioning-flags test node (:nodes test))
               (master-api-flags (:api test) node)
               (master-perf-flags test)
               (master-stress-flags test)
               (packed-columns-flags test)
               (master-table-lock-flags test)]
              (:master-flags test)))))

  (start-tserver! [db test node]
    (c/su (info "ulimit\n" (c/exec :ulimit :-a))
          (c/exec :mkdir :-p ce-tserver-log-dir)
          (apply cu/start-daemon!
            {:logfile ce-tserver-logfile
             :pidfile ce-tserver-pidfile
             :chdir   dir}
            ce-tserver-bin
            (apply-extra-gflags
              [:--tserver_master_addrs (master-addresses test)
               ; Tracing
               :--enable_tracing
               :--rpc_slow_query_threshold_ms 1000
               (basic-flags node)
               (experimental-tuning-flags test)
               (geo-partitioning-flags test node (:nodes test))
               (packed-columns-flags test)
               (random-clock-skew-flags test node)
               (tserver-api-flags test node)
               (tserver-connection-manager-preview-flags test)
               (tserver-heartbeat-flags test)
               (tserver-perf-flags test)
               (tserver-read-committed-flags test)
               (tserver-serializable-flags test)
               (tserver-stress-flags test)
               (tserver-table-lock-flags test)]
              (:tserver-flags test)))))

  (stop-master! [db]
    (c/su (cu/stop-daemon! ce-master-pidfile)))

  (stop-tserver! [db]
    (c/su (cu/stop-daemon! ce-tserver-pidfile))
    (c/su (cu/grepkill! "postgres"))
    (c/su (cu/grepkill! "odyssey")))

  (wipe! [db]
    (suppress-interrupted-exception
      (c/su (c/exec :rm :-rf ce-data-dir))))

  db/DB
  (setup! [db test node]
    (suppress-interrupted-exception
      (install! db test)
      (configure! db test node)
      (start! db test node)))

  (teardown! [db test node]
    (suppress-interrupted-exception
      (stop! db test node)
      (wipe! db)))

  db/Primary
  (setup-primary! [this test node]
    "Executed once on a first node in list (i.e. n1 by default) after per-node setup is done"
    (if (= (:api test) :ysql)
      (let [colocated-clause (if (:yb-colocated test)
                               " WITH colocated = true"
                               "")]
        (ysqlsh test node :-c (str "DROP DATABASE IF EXISTS jepsen;"))
        (ysqlsh test node :-c (str "DROP USER IF EXISTS jepsen; "
                                   "CREATE USER jepsen createdb; "))
        (ysqlsh test node :-U "jepsen" :-c (str "CREATE DATABASE jepsen" colocated-clause ";"))
        (if (:geo-partition test)
          (do
            (info "Setup optional geo partitioning")
            (setup-geo-partition node tablespace-name)
            (ysqlsh test node :-c (str "GRANT CREATE ON TABLESPACE " tablespace-name "_1a TO jepsen;"))
            (ysqlsh test node :-c (str "GRANT CREATE ON TABLESPACE " tablespace-name "_2a TO jepsen;")))))))

  db/LogFiles
  (log-files [_ _ _]
    (concat [ce-master-logfile
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
