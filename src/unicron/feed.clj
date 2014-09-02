(ns unicron.feed
  "Watch for new files, take action when they appear"
  {:author "Matt Halverson", :date "Fri Aug 22 09:54:28 PDT 2014"}
  (:require [clj-time.core :as t]
            [clojure.java.shell :refer [sh]]
            [diesel.core :refer [definterpreter]]
            [roxxi.utils.common :refer [def-]]
            [roxxi.utils.print :refer [print-expr]]
            [unicron.utils :refer [log-and-throw]])
  (:import (org.quartz CronExpression)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Defaults

(def default-poll-expr '(hours 1))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # The language

(def- period-exprs
  ;; date functions pass out to clj-time library
  {'weeks t/weeks
   'days t/days
   'hours t/hours
   'minutes t/minutes
   'seconds t/seconds ;; for DEV only
   })

(defn- period-expr? [expr] (contains? period-exprs (first expr)))
(defn- poll-expr? [expr] (= 'poll-expr (first expr)))
(defn- filter? [expr] (= 'filter (first expr)))

(definterpreter interp-feed []
  ;; top-level
  ['feed => :feed]
  ;; id
  ['id => :id]
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

(defmethod interp-feed :feed [[token id date-expr action & optl]]
  ;; REQUIRED: an id, a date-expr, an action
  ;; OPTIONAL: a poll-expr, a filter
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
    {:id (interp-feed id)
     :date-expr (interp-feed date-expr)
     :action (interp-feed action)
     :poll-expr (interp-feed (or (first poll-exprs)
                                 default-poll-expr))
     :filters (map interp-feed filters)}))

(defmethod interp-feed :id [[token id]]
  id)

(defmethod interp-feed :date-expr [[token date-expr]]
  ;; XXX
  ;; Validate that:
  ;;  - it contains at least one conversion spec with a defined granularity
  ;;  - granularities are decreasing from l-to-r (error if there are -- indicates bad prefix design)
  ;;  - there aren't any undefined conversion specs, i.e. erroneous % signs (warn if there are)
  ;; Then just return the pattern-str.
  date-expr)

(defmethod interp-feed :action [[token typed-action]]
  ;; typed-action may be sh, root-sh, clj, ...
  ;;
  ;; Returns a thunk that takes...
  ;;  - a URI (including a formatted date-expr)
  ;;  - the timestamp of the date-expr
  ;;  - possibly a lazy stream of the file's contents piped into std-in
  ;;      (if with-in-stream option is specified)
  (let [user-action (interp-feed typed-action)]
    (fn [uri timestamp lazy-filestream]
      nil)))

(defmethod interp-feed :with-file-stream [[token arg1 & [arg2]]]
  ;; If you want your typed-action to be fed a lazy stream of the file's
  ;; contents via std-in, enclose it in a with-file-stream block.
  ;;
  ;; Two options here:
  ;;   1. (action (with-file-stream :idempotent (sh ...)))
  ;;   2. (action (with-file-stream (sh ...)))
  ;; Option 1 is (typically) lower-latency, but requires that the action
  ;;   be idempotent at the record/line level (e.g. if the action gets the
  ;;   same input record twice on accident, that should be ok.) Option 1
  ;;   entails streaming the file contents across the network and straight
  ;;   into a pipe.
  ;;   Retry mechanism: retry the stream from the beginning. Failing that,
  ;;   retry using Option 2.
  ;; Option 2 is the robust approach, but higher-latency. It entails
  ;;   downloading the file to local disk and then streaming the file contents
  ;;   from local. NOTE: Option 2 is required if your action is not idempotent.
  ;;   Retry mechanism: retry the download. Failing that, give up and log an
  ;;   error and send an alert.
  (let [idempotent? (and arg2 (= arg1 :idempotent))
        typed-action (or arg2 arg1)]
  nil)

(defmethod interp-feed :sh [[token cmd]]
  ;; Return a thunk that runs the specified shell command(s).
  ;; The thunk returns the map of exit code (:exit), std out (:out),
  ;; and std err (:err).
  ;;
  ;; NOTE from http://clojuredocs.org/clojure_core/clojure.java.shell/sh:
  ;;
  ;; sh is implemented using Clojure futures.  See examples for 'future'
  ;; for discussion of an undesirable 1-minute wait that can occur before
  ;; your standalone Clojure program exits if you do not use shutdown-agents.
  #(sh "/bin/bash" "-c" cmd "<<<" (str %)))

(defmethod interp-feed :root-sh [[token cmd]]
  (log-and-throw "root-sh has not been implemented"))

(defmethod interp-feed :clj [[token clj-code]]
  ;; Return a thunk that runs the clj-code they specified.
  ;; The thunk returns whatever the overall body of code evaluates to.
  (cond (string? clj-code) (eval (read-string clj-code))
        (list? clj-code)   (eval clj-code)
        (fn? clj-code)     clj-code
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

(defn- valid-cron? [cron-str]
  (CronExpression/isValidExpression cron-str))

(defmethod interp-feed :cron [[token cron-str]]
  (when-not (valid-cron? cron-str)
    (log-and-throw (format "Invalid cron-str: %s" cron-str)))
  (list cron-str))

(defmethod interp-feed :crons [[token & cron-strs]]
  (when (empty? cron-strs)
    (log-and-throw "No crons specified in the \"crons\" block!"))
  (let [bad-crons (filter (comp not valid-cron?) cron-strs)]
    (when-not (empty? bad-crons)
      (log-and-throw (format "Invalid cron-strs: %s" bad-crons))))
  cron-strs)

(defmethod interp-feed :filter [[token filter-expr]]
  ;; Returns a predicate that takes a filepath and indicates
  ;; whether or not it should actually be processed.
  (interp-feed filter-expr))

(defmethod interp-feed :regex [[token regex-pattern]]
  #(re-find (re-pattern regex-pattern) %))
