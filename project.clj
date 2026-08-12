(defproject yugabyte "0.1.2-SNAPSHOT"
  :description "Jepsen testing for YugaByteDB"
  :url "http://yugabyte.com/"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[clj-http "3.13.1" :exclusions [commons-logging
                                                 commons-io
                                                 potemkin]]
                 [jepsen "0.3.13"]
                 [com.yugabyte/cassaforte "3.0.0-alpha2-yb-1"
                  :exclusions [org.clojure/tools.reader]]
                 [org.clojure/java.jdbc "0.7.12"]
                 [org.postgresql/postgresql "42.7.13"]
                 [org.slf4j/jcl-over-slf4j "2.0.18"]
                 [org.slf4j/jul-to-slf4j "2.0.18"]
                 [clj-wallhack "1.0.1"]]
  :main yugabyte.runner)
;  :aot [yugabyte.runner
;        clojure.tools.logging.impl])
