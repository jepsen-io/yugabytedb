(ns jepsen.yugabyte.bank-improved
  "Reworked original bank workload that now include inserts and deletes. There
  are three :f's:

      :transfer - Transfer between two extant accounts
      :insert   - Creates a new account, funded by another
      :delete   - Deletes an account, transferring its balance to another"
  (:refer-clojure :exclude [test])
  (:require [jepsen.tests.bank :as bank]
            [clojure [set :as set]]
            [jepsen [history :as h]
                    [generator :as gen]
                    [checker :as checker]
                    [random :as random]
                    [util :as util]]))

(defn rand-account
  "Picks a random account from the given max-account and free-accounts
  set."
  [max-account free-accounts]
  (let [candidate (random/long (inc max-account))]
    (if (contains? free-accounts candidate)
      (recur max-account free-accounts)
      candidate)))

(defn recompute-max-account*
  "Takes a max-account and a free-accounts set, and returns a (possibly
  smaller) max-account."
  [^long max-account free-accounts]
  (if (contains? free-accounts max-account)
    (recur (dec max-account) free-accounts)
    max-account))

(defn recompute-max-account
  "Takes a Generator and returns one with a possibly smaller max-account."
  [{:keys [max-account free-accounts] :as gen}]
  (let [max-account' (recompute-max-account* max-account free-accounts)]
    (if (= max-account max-account')
      gen
      (assoc gen :max-account max-account'))))

(defn update-generator-with-known-accounts
  "Takes a Generator and a collection of accounts we know definitely exist.
  Returns a new Generator with those accounts."
  [gen accounts]
  (let [free-accounts (reduce disj (:free-accounts gen) accounts)
        max-accounts  (reduce max (:max-account gen) accounts)]
    (assoc gen
           :free-accounts free-accounts
           :max-accounts  max-accounts)))

(defrecord Generator
  [fs                 ; A vector of fs to generate like [:insert :transfer]
   max-transfer       ; How much can we transfer at one time?
   ^long max-account  ; The largest account ID we believe exists
   free-accounts      ; The set of accounts smaller than max-account-id we
                      ; believe don't exist
  ]

  gen/Generator
  (op [this test ctx]
    (case (let [n (- max-account (count free-accounts))]
            (cond ; Too few accounts; try to insert
                  (and (< n 2) (some #{:insert} fs))
                  (random/nth [:read :insert])

                  ; Too many accounts; try to delete. Remember, we want a small
                  ; account pool so that we have contention.
                  (and (< 12 n) (some #{:delete} fs))
                  (random/nth [:read :delete])

                  true
                  (random/nth fs)))

      :read [(gen/fill-in-op {:f :read} ctx) this]

      :insert
      (let [new-account (random/nth
                          (vec (conj free-accounts (inc max-account))))]
        [(gen/fill-in-op
           {:f :insert
            :value {:from   (rand-account max-account free-accounts)
                    :to     new-account
                    :amount (inc (random/long max-transfer))}}
           ctx)
         ; Sometimes we want to know a new max account-exists (so we transfer
         ; to it); other times we wait, so that we generate multiple inserts to
         ; the same account ID.
         (random/branch
           this
           (assoc this :max-account (max max-account new-account)))])

      :transfer
      [(gen/fill-in-op
         {:f :transfer
          :value {:from   (rand-account max-account free-accounts)
                  :to     (rand-account max-account free-accounts)
                  :amount (inc (random/long max-transfer))}}
         ctx)
       this]

      :delete
      (let [from (rand-account max-account free-accounts)
            to   (rand-account max-account free-accounts)]
        (if (= from to)
          ; Nope, we need to drain deleted accounts to somewhere else
          (recur test ctx)
          [(gen/fill-in-op {:f     :delete
                            :value {:from from, :to to}}
                           ctx)
           ; Sometimes we want to delete the account; other times it's
           ; more fun not to
           (random/branch
             this
             (let [free-accounts' (conj free-accounts from)]
               (assoc this
                      :free-accounts free-accounts'
                      :max-account  (recompute-max-account*
                                      max-account free-accounts'))))]))))

  (update [this test ctx {:keys [f value] :as op}]
    (if (h/ok? op)
      (random/branch
        ; Remember, this is a concurrent system--we actually want to be a
        ; little sloppy with accepting updates. If we miss something, that's
        ; fine, :read will help us converge later.
        this
        (case (:f op)
          :insert
          (update-generator-with-known-accounts
            this [(:from value) (:to value)])

          :transfer
          (update-generator-with-known-accounts
            this [(:from value) (:to value)])

          :delete
          (-> this
              (update-generator-with-known-accounts [(:to value)])
              (update :free-accounts conj (:from value))
              (recompute-max-account))

          :read
          (let [max-account   (reduce max 0 (keys value))
                free-accounts (set/difference (set (range (inc max-account)))
                                              (set (keys value)))]
            (assoc this
                   :max-account max-account
                   :free-accounts free-accounts))))
      ; Not an OK op
      this)))

(defn generator
  "Constructs a generator with the given vector of fs, e.g. [:insert :transfer
  :delete] and starting accounts 0, 1, ... n-1."
  [fs n]
  (Generator. fs 5 (dec n) #{}))

(defn check-op
  "Based on code from original jepsen.test.bank/check-op
  Here we need to exclude :negative-value and :unexpected-key checks"
  [accts total op]
  (let [ks (keys (:value op))
        balances (vals (:value op))]
    (cond
      (some nil? balances)
      {:type :nil-balance
       :nils (->> (:value op)
                  (remove val)
                  (into {}))
       :op   op}

      (not= total (reduce + balances))
      {:type  :wrong-total
       :total (reduce + balances)
       :op    op})))

(defn checker
  "Based on code from original jepsen.test.bank/checker
  Since we have internal check-op call this function needs to be modified"
  [checker-opts]
  (reify checker/Checker
    (check [this test history opts]
      (let [accts (set (:accounts test))
            total (:total-amount test)
            reads (->> history
                       (h/filter (h/has-f? :read))
                       h/oks)
            errors (->> reads
                        (h/map (partial check-op
                                        accts
                                        total))
                        (h/filter identity)
                        (group-by :type))]
        {:valid?      (every? empty? (vals errors))
         :read-count  (count reads)
         :error-count (reduce + (map count (vals errors)))
         :first-error (util/min-by (comp :index :op) (map first (vals errors)))
         :errors      (->> errors
                           (map
                             (fn [[type errs]]
                               [type
                                (merge {:count (count errs)
                                        :first (first errs)
                                        :worst (util/max-by
                                                 (partial bank/err-badness test)
                                                 errs)
                                        :last  (peek errs)}
                                       (if (= type :wrong-total)
                                         {:lowest  (util/min-by :total errs)
                                          :highest (util/max-by :total errs)}
                                         {}))]))
                           (into {}))}))))

(defn workload
  "A workload which inserts new accounts, transfers between accounts, deletes
  accounts, and reads all accounts."
  [opts]
  {:total-amount 100
   :accounts     (range 5)
   :generator    (generator [:read :insert :transfer :delete] 5)
   :checker      (checker/compose
                   {:bank (checker opts)
                    :plot (bank/plotter)})})

(defn workload-sans-deletes
  "Like `workload`, but never emits a :delete operation. We do this because CQL
  has no safe way to implement deletes--we can't read the balance
  transactionally to transfer it elsewhere."
  [opts]
  (assoc (workload opts)
         :generator (generator [:read :insert :transfer] 5)))
