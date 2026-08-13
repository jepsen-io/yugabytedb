(ns jepsen.yugabyte.util
  "Basic support functions we use across the Yugabyte tests."
  (:require [clj-commons.slingshot :refer [try+ throw+]]))

(defn parse-version
  "Parses a Yugabyte version string into a map of the form:

      {:full  '2026.1.0.0-b118'
      :short '2026.1.0.0'
      :b     '118'}"
  [version]
  (let [[full short _ b] (re-find #"^([\d\.]+)(-b(\d+))?$" version)]
    (when-not full
      (throw+ {:type :unknown-version-format
               :version version}))
    {:full full
     :short short
     :b b}))
