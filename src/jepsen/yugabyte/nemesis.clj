(ns jepsen.yugabyte.nemesis
  "Fault injection"
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer :all]
            [jepsen [generator :as gen]
                    [nemesis :as n]
                    [random :as rand]
                    [role :as role]
                    [util :as util]]
            [jepsen.nemesis [combined :as nc]]))

(defn package-gen-helper
  "Helper for package-gen. Takes a collection of packages and draws a random
  nonempty subset of them."
  [packages]
  (when (seq packages)
    (let [pkgs (->> packages
                    ; And pick a random subset of those
                    util/random-nonempty-subset
                    vec)]
      ; If we drew nothing, try again.
      (if (seq pkgs)
        pkgs
        (do ; (info "no draw, retrying")
            (recur packages))))))

(defn package-gen
  "For long-running tests, it's nice to be able to have periods of no faults,
  periods with lots of faults, just one kind of fault, etc. This takes a time
  period in seconds, which is how long to emit nemesis operations for a
  particular subset of packages. Takes a collection of packages. Constructs a
  nemesis generator which emits faults for a shifting collection of packages
  over time."
  [period packages]
  ; We want a sequence of random subsets of packages
  (repeatedly
    (fn rand-pkgs []
      (let [; Pick packages
            pkgs (if (rand/bool 1/4)
                   ; Roughly 1/4 of the time, pick no pkgs
                   []
                   (package-gen-helper packages))
            ; Construct combined generators
            gen       (if (seq pkgs)
                        (apply gen/any (map :generator pkgs))
                        (gen/sleep period))
            final-gen (keep :final-generator pkgs)]
        ; Ops from the combined generator, followed by a final gen
        [(gen/log (str "Shifting to new mix of nemeses: "
                       (pr-str (map (comp n/fs :nemesis) pkgs))))
         (gen/time-limit period gen)
         final-gen]))))

(defn role-faults
  "Takes a set of faults like #{:pause-master} and a role name like :master.
  Returns a set of faults for that specific role, like #{:pause}.

  General faults, like :pause or :kill, apply to every role."
  [faults role]
  (->> faults
       (keep (fn [fault]
               (if-let [[m fault role']
                        (re-find #"^(\w+)-(master|tserver)$"
                                 (name fault))]
                 ; This is a role-specific fault
                 (when (= (name role) role')
                   (keyword fault))
                 ; A general fault always applies
                 fault)))
       set))

(defn update-role-faults
  "Takes CLI options and updates :faults to be specific to the given role."
  [role opts]
  (update opts :faults role-faults role))


(defn package
  "Takes CLI opts. Constructs a nemesis and generator for the test."
  [opts]
  (let [opts (update opts :faults set)
        roles (:roles opts)
        _ (assert (map? roles))
        packages
        (->> (concat
               ; Unfurl each role into a collection of nemesis packages for
               ; that particular role.
               (mapcat (fn [[role nodes]]
                         (->> opts
                              (update-role-faults role)
                              nc/nemesis-packages
                              (map (partial role/restrict-nemesis-package role))))
                       roles)
               ; Custom packages
               [])
             (filter :generator))
        nsp (:stable-period opts)]
    ;(info :packages (map (comp n/fs :nemesis) packages))
    (cond-> (nc/compose-packages packages)
      nsp (assoc :generator (package-gen nsp packages)))))
