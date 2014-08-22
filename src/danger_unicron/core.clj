(ns danger-unicron.core
  "Watch for new files, take action when they appear"
  {:author "Matt Halverson"
   :date "Fri Aug 22 09:54:28 PDT 2014"}
  (:require [clojure.tools.logging :as log])
  (:require [roxxi.utils.print :refer [print-expr]]
            [roxxi.utils.common :refer [def-]])
  (:require [diesel.core :refer [definterpreter]])
  (:require [clojure.java.shell :refer [sh]]
            [clj-time.core :as t]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Helpers

(defmacro log-and-throw [msg]
  `(do
     (log/error ~msg)
     (throw (RuntimeException. ~msg))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Defaults

(def default-poll-expr '(hours 1))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # The language

(def- period-exprs
  ;; date functions pass out to clj-time library
  {'months t/months
   'weeks t/weeks
   'days t/days
   'hours t/hours
   'minutes t/minutes})

(defn- period-expr? [expr] (contains? period-exprs (first expr)))
(defn- poll-expr? [expr] (= 'poll-expr (first expr)))
(defn- filter? [expr] (= 'filter (first expr)))

(definterpreter interp-feed []
  ;; top-level
  ['feed => :feed]
  ;; date-expr for the feed
  ['date-expr => :date-expr]
  ;; what action to take
  ['action => :action]
  ['sh => :sh]
  ['root-sh => :root-sh]
  ['clj => :clj]
  ;; how to often to check
  ['poll-expr => :poll-expr]
  ['interval => :interval]
  [period-expr? => :period-expr]
  ['cron => :cron]
  ['crons => :crons]
  ;; filter for finer control of event triggers
  ['filter => :filter]
  ['regex => :regex])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # The implementation

(defmethod interp-feed :feed [[token date-expr action & optl]]
  ;; optl may include one poll-expr and one filter
  (let [poll-exprs (filter poll-expr? optl)
        filters (filter filter? optl)]
    (when (> (count poll-exprs) 1)
      (log-and-throw (format
                      (str "Error: only allowed one poll-expr per feed. "
                           "Several were specified: %s")
                      poll-exprs)))
    (when (> (count filters) 1)
      (log-and-throw (format
                      (str "Error: only allowed one filter per feed. "
                           "Several were specified: %s")
                      filters)))
    (print-expr [token date-expr action optl])
    {:date-expr (interp-feed date-expr)
     :action (interp-feed action)
     :poll-expr (interp-feed (or (first poll-exprs)
                                 default-poll-expr))
     :filters (map interp-feed filters)}))

(defmethod interp-feed :date-expr [[token date-expr]]
  ;; XXX
  ;; Validate that:
  ;;  - it contains at least one conversion spec with a defined granularity
  ;;  - granularities are decreasing from l-to-r (error if there are -- indicates bad prefix design)
  ;;  - there aren't any undefined conversion specs, i.e. erroneous % signs (warn if there are)
  ;; Then just return the pattern-str.
  date-expr)

(defmethod interp-feed :action [[token typed-action]]
  ;; XXX
  ;; Returns a thunk that takes...
  ;;  - a filepath?
  ;;  - a list of filepaths?
  ;; typed-action may be sh, root-sh, clj, ...
  (interp-feed typed-action))

(defmethod interp-feed :sh [[token cmd]]
  ;; Return a thunk that runs the specified shell command(s).
  ;; The thunk returns the map of exit code (:exit), std out (:out),
  ;; and std err (:err).
  #(sh "/bin/bash" "-c" cmd))

(defmethod interp-feed :root-sh [[token cmd]]
  (log-and-throw "root-sh has not been implemented"))

(defmethod interp-feed :clj [[token clj-code]]
  ;; Return a thunk that runs the clj-code they specified.
  ;; The thunk returns whatever the overall body of code evaluates to.
  (cond (string? clj-code) #(eval (read-string clj-code))
        (list? clj-code)   #(eval clj-code)
        (fn? clj-code)     #(apply clj-code '())
        :else (log-and-throw "Don't know how to run your clj-type action")))

(defmethod interp-feed :poll-expr [[token typed-poll-expr]]
  ;; Returns EITHER a clj-time period, OR a list of cron-strs.
  (interp-feed typed-poll-expr))

(defmethod interp-feed :interval [[token period-expr]]
  (interp-feed period-expr))

(defmethod interp-feed :period-expr [[period number]]
  (when (not (and (integer? number)
                  (pos? number)))
    (log-and-throw (format
                    (str
                     "A period-expr must contain a period and a positive "
                     "integer. You didn't put a positive integer -- you put "
                     "%s instead")
                    number)))
  (let [period-maker (get period-exprs period)]
    (period-maker number)))

(defmethod interp-feed :cron [[token cron-str]]
  ;; XXX
  ;; Validate that:
  ;;  - it's a valid cron
  (list cron-str))

(defmethod interp-feed :crons [[token & cron-strs]]
  ;; XXX
  ;; Validate that:
  ;;  - they're all valid crons
  ;;  - at least a single cron has been specified
  (when (empty? cron-strs)
    (log-and-throw "No crons specified in the \"crons\" block!"))
  cron-strs)

(defmethod interp-feed :filter [[token filter-expr]]
  ;; Returns a predicate that takes a filepath and indicates
  ;; whether or not it should actually be processed.
  (interp-feed filter-expr))

(defmethod interp-feed :regex [[token regex-pattern]]
  #(re-find (re-pattern regex-pattern) %))
