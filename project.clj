(defproject io.jepsen/yugabyte "0.1.2-SNAPSHOT"
  :description "Jepsen testing for YugaByteDB"
  :url "http://yugabyte.com/"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [clj-http "3.12.3" :exclusions [commons-logging]]
                 [jepsen "0.3.14-SNAPSHOT"]
                 [io.jepsen/sql "0.1.1-SNAPSHOT"]
                 [com.yugabyte/cassandra-driver-core "3.10.3-yb-3"]
                 [org.slf4j/slf4j-api "2.0.17"]
                 [org.clojure/data.json "2.4.0"]
                 [com.yugabyte/jdbc-yugabytedb "42.3.5-yb-3"]
                 [version-clj "2.0.2"]
                 [clj-wallhack "1.0.1"]]
  :main jepsen.yugabyte.cli
  :jvm-opts ["-Djava.awt.headless=true"
             "-Djava.net.preferIPv4Stack=true"
             "-Xms4g"
             "-Xmx24g"
             "-XX:-OmitStackTraceInFastThrow"
             ])
