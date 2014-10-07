(ns unicron.feed
  "Watch for new files, take action when they appear"
  {:author "Matt Halverson", :date "Fri Aug 22 09:54:28 PDT 2014"}
  (:require [clojure.tools.logging :as log]
            [clj-time.core :as t]
            [clojure.java.shell :refer [sh]]
            [diesel.core :refer [definterpreter]]
            [roxxi.utils.common :refer [def-]]
            [roxxi.utils.print :refer [print-expr]]
            [unicron.utils :refer [log-and-throw
                                   assert-not-nil
                                   arity]]
            [unicron.connection :refer [interp-conn]]
            [unicron.action-wrapper :as aw])
  (:import (org.quartz CronExpression)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Defaults

(def default-tz "America/Los_Angeles")
(def default-poll-expr '(hours 1))
(def default-dir-ttl '(days 1))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # The language

(def- period-exprs
  ;; date functions pass out to clj-time library
  {'weeks t/weeks
   'days t/days
   'hours t/hours
   'minutes t/minutes
   'seconds t/seconds ;; 'seconds' is intended for DEV only -- the system
   ;; will not play well with file feeds that arrive multiple times per minute.
   })

(defn- period-expr? [expr] (contains? period-exprs (first expr)))
(defn- poll-expr? [expr] (= 'poll-expr (first expr)))
(defn- is-directory? [expr] (= 'is-dir (first expr)))
(defn- filter? [expr] (= 'filter (first expr)))

(definterpreter interp-feed [env]
  ;; top-level
  ['feed => :feed]
  ;; id
  ['id => :id]
  ;; conn
  ['conn => :conn]
  ;; date-expr for the feed
  ['date-expr => :date-expr]
  ;; what is the first date-expr in the feed?
  ['starting-after => :starting-after]
  ;; what action to take
  ['action => :action]
  ['with-file-stream => :with-file-stream]
  ['sh => :sh]
  ['root-sh => :root-sh]
  ['clj => :clj]
  ;; directory-feeds
  ['is-dir => :is-dir]
  ['dir-ttl => :dir-ttl]
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

(defmethod interp-feed :feed [[token id conn date-expr starting-after action & optl] env]
  ;; REQUIRED: an id, a connection, a date-expr with optl tz, the first date-expr in the feed, an action
  ;; OPTIONAL: a poll-expr, any number of filters, an is-dir clause with optl ttl
  (let [poll-exprs (filter poll-expr? optl)
        is-dirs (filter is-directory? optl)
        filters (filter filter? optl)]
    (when (> (count poll-exprs) 1)
      (log-and-throw (format
                      (str "Error: only allowed one poll-expr per feed. "
                           "Several were specified: %s")
                      poll-exprs)))
    (when (> (count is-dirs) 1)
      (log-and-throw (format
                      (str "Error: only allowed one is-directory clause per feed. "
                           "Several were specified: %s")
                      is-dirs)))
    (let [interpreted-conn (interp-feed conn env)
          interpreted-de (interp-feed date-expr env)
          date-expr (:date-expr interpreted-de)
          date-expr-tz (:date-expr-tz interpreted-de)
          interpreted-sf (interp-feed starting-after env)
          interpreted-dir-ttl (and (first is-dirs)
                                   (interp-feed (first is-dirs) env))
          interpreted-filters (map #(interp-feed % env) filters)
          base-map {:conn interpreted-conn
                    :date-expr date-expr
                    :date-expr-tz date-expr-tz
                    :starting-after interpreted-sf
                    :filters interpreted-filters
                    :is-dir (not (nil? interpreted-dir-ttl))
                    :dir-ttl interpreted-dir-ttl}
          enriched-env (merge env base-map)]
      (merge base-map
             {:id (interp-feed id env)
              :action (interp-feed action enriched-env)
              :poll-expr (interp-feed (or (first poll-exprs)
                                          default-poll-expr)
                                      env)}))))

(defmethod interp-feed :id [[token id] env]
  (str id))

(defmethod interp-feed :conn [[token c] env]
  (cond (map? c)
        c
        (symbol? c)
        (throw (RuntimeException. "Haven't yet implemented it so that conns can be looked up"))
        (seq? c)
        (:info (interp-conn c {}))))

(defmethod interp-feed :date-expr [[token date-expr & [tz]] env]
  ;; XXX
  ;; For date-expr, validate that:
  ;;  - it contains at least one conversion spec with a defined granularity
  ;;  - granularities are decreasing from l-to-r (error if there are -- indicates bad prefix design)
  ;;  - there aren't any undefined conversion specs, i.e. erroneous % signs (warn if there are)
  ;; For date-expr-tz, validate that:
  ;;  - it's a valid date-expr-tz (Whatever that means)
  (assert-not-nil date-expr)
  {:date-expr date-expr
   :date-expr-tz (or tz default-tz)})


(defmethod interp-feed :starting-after [[token string] env]
  ;; XXX
  ;; Validation:
  ;;  - pass in the date-expr, verify that the string actually
  ;;    is a valid instance of that date-expr
  ;;
  ;; This represents the zeroth date-expr in the feed... i.e. start looking for
  ;; files/dirs on the remote system that come AFTER this date-expr.
  ;;
  ;; If your first file is "s3://bucket/2014/01/15/file.txt", then your
  ;; starting-after could be "s3://bucket/2014/01/14/file.txt"
  string)

(defmethod interp-feed :action
  [[token typed-action] env]
  "All these keywords are set in the env in `interp-feed :feed`"
  ;; typed-action may be sh, root-sh, clj, ...
  ;;
  ;; Returns a thunk that takes...
  ;;  - a URI (including a formatted date-expr)
  ;;  - the timestamp of the date-expr
  ;;  - possibly a lazy stream of the file's contents piped into std-in
  ;;      (if with-in-stream option is specified)
  (let [action-fn (interp-feed typed-action env)
        date-expr (:date-expr env)
        date-expr-tz (:date-expr-tz env)
        starting-after (:starting-after env)
        conn-info (:conn env)
        filters (:filters env)
        composed-filter (if (empty? filters)
                          (constantly true)
                          (apply every-pred filters))
        h (:history env)]
    (if (:is-dir env)
      ;; the print-expr looks like this:
      ;; Expression job-execution-context evaluates to JobExecutionContext: trigger: 'DEFAULT.foobar job: DEFAULT.foobar fireTime: 'Wed Sep 10 08:34:40 PDT 2014 scheduledFireTime: Wed Sep 10 08:34:40 PDT 2014 previousFireTime: 'null nextFireTime: Wed Sep 10 08:34:50 PDT 2014 isRecovering: false refireCount: 0 of type class org.quartz.impl.JobExecutionContextImpl
      (fn p-dir [job-execution-context]
        (aw/process-dirs h action-fn date-expr date-expr-tz starting-after
                         composed-filter conn-info (:dir-ttl env)))
      (fn p-file [job-execution-context]
        (aw/process-files h action-fn date-expr date-expr-tz starting-after
                          composed-filter conn-info)))))

;; (defn- get-usable-space-in-bytes []
;;   (let [roots (-> (java.nio.file.FileSystems/getDefault)
;;                   .getRootDirectories)
;;         filestores (map #(java.nio.file.Files/getFileStore %) roots)
;;         avail (map #(.getUsableSpace %) filestores)]
;;     (when (not= 1 (count avail))
;;       (log-and-throw (format
;;                       (str "Error: Multiple root directories were found. "
;;                            "Not sure which one is ours. Roots were: %s")
;;                       roots)))
;;     (first avail)))

(defmethod interp-feed :with-file-stream [[token arg1 & [arg2]] env]
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
  ;;   into the action.
  ;;   Retry mechanism: retry the stream from the beginning. Failing that,
  ;;   retry using Option 2.
  ;; Option 2 is the robust approach, but higher-latency. It entails
  ;;   downloading the file to local disk and then streaming the file contents
  ;;   from local. NOTE: Option 2 is required if your action is not idempotent.
  ;;   Retry mechanism: retry the download. Failing that, give up and log an
  ;;   error and send an alert.
  (let [idempotent? (and arg2 (= arg1 :idempotent))
        typed-action (or arg2 arg1)
        new-env (merge env {:with-file-stream? true})
        action-fn (interp-feed typed-action new-env)]
    ;; XXX Implement me
    action-fn))

(defmethod interp-feed :sh [[token cmd] env]
  ;; Return a thunk that runs the specified shell command(s).
  ;; The thunk returns the map of exit code (:exit), std out (:out),
  ;; and std err (:err).
  ;;
  ;; NOTE from http://clojuredocs.org/clojure_core/clojure.java.shell/sh:
  ;;
  ;; sh is implemented using Clojure futures.  See examples for 'future'
  ;; for discussion of an undesirable 1-minute wait that can occur before
  ;; your standalone Clojure program exits if you do not use shutdown-agents.
  (fn [uri ts]
    (let [ret (sh "/bin/bash" "-c" (str cmd) (str uri) (str ts))]
      ;; ret looks like this:
      ;;   {:exit 0, :out "stdout\n", :err "stderr"}
      (when (not= 0 (:exit ret))
        ;; Why throw here? It's not a functional technique and it doesn't seem
        ;; to fit. It should be OK if a shell command exists with a non-zero
        ;; error code.
        ;;
        ;; What about using a function of Map -> Map where it can inspect
        ;; :error and conditionally execute code. Then you can compose easily:
        ;;
        ;; (send-email-on-error (sh ...))
        ;; (send-flowdock-on-error (send-email-on-error (sh ...)))
        (throw
         (RuntimeException.
          (str "sh command exited with nonzero exit code: "
               "exit code was " (:exit ret) "; stderr was "
               (:err ret) "; stdout was " (:out ret) ".")))))))

(defmethod interp-feed :root-sh [[token cmd] env]
  (log-and-throw "root-sh has not been implemented"))

(defmethod interp-feed :clj [[token clj-code] env]
  ;; Return a thunk that runs the clj-code they specified.
  ;; The thunk returns whatever the overall body of code evaluates to.
  (let [f (cond (string? clj-code) (eval (read-string clj-code))
                (list? clj-code)   (eval clj-code)
                (fn? clj-code)     clj-code
                :else (log-and-throw "Don't know how to run your clj-type action"))]
    (when (not= 2 (arity f))
      (log/warnf
       (str "Bad arity: your clj-type should take 2 args (uri and "
            "timestamp). Your arity was "
            (arity f))))
    f))

(defmethod interp-feed :is-dir [[token & [ttl-expr]] env]
  ;; Returns a clj-time period representing the ttl for directories in this feed
  (if ttl-expr
    (interp-feed ttl-expr env)
    (interp-feed default-dir-ttl env)))

(defmethod interp-feed :dir-ttl [[token interval] env]
  (interp-feed interval env))

(defmethod interp-feed :poll-expr [[token typed-poll-expr] env]
  ;; Returns EITHER a clj-time period, OR a list of cron-strs.
  (interp-feed typed-poll-expr env))

(defmethod interp-feed :interval [[token period-expr] env]
  (interp-feed period-expr env))

(defmethod interp-feed :period-expr [[period number] env]
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

(defmethod interp-feed :cron [[token cron-str] env]
  (when-not (valid-cron? cron-str)
    (log-and-throw (format "Invalid cron-str: %s" cron-str)))
  (list cron-str))

(defmethod interp-feed :crons [[token & cron-strs] env]
  (when (empty? cron-strs)
    (log-and-throw "No crons specified in the \"crons\" block!"))
  (let [bad-crons (filter (comp not valid-cron?) cron-strs)]
    (when-not (empty? bad-crons)
      (log-and-throw (format "Invalid cron-strs: %s" bad-crons))))
  cron-strs)

(defmethod interp-feed :filter [[token filter-expr] env]
  ;; Returns a predicate that takes a filepath and indicates
  ;; whether or not it should actually be processed.
  (interp-feed filter-expr env))

(defmethod interp-feed :regex [[token regex-pattern] env]
  #(re-find (re-pattern regex-pattern) %))
