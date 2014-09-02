(ns unicron.scheduler
  "Schedule tasks, handle unions of crons"
  {:author "Matt Halverson"
   :date "Fri Aug 22 16:06:44 PDT 2014"}
  (:require [clojure.tools.logging :as log])
  (:require [roxxi.utils.print :refer [print-expr]]
            [roxxi.utils.common :refer [def-]])
  (:require [clj-time.core :as time])
  (:require [clojurewerkz.quartzite.scheduler :as qs]
            [clojurewerkz.quartzite.schedule.calendar-interval :as ci]
            [clojurewerkz.quartzite.schedule.cron :as cron]
            [clojurewerkz.quartzite.triggers :as t]
            [clojurewerkz.quartzite.jobs :as j]
            [clojurewerkz.quartzite.matchers :as m])
  (:require [unicron.utils :refer [log-and-throw]])
  (:import [org.quartz CalendarIntervalScheduleBuilder]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Debugging

(defn list-all-jobs []
  (let [groups (qs/get-job-group-names)
        keys (mapcat #(qs/get-job-keys (m/group-equals %)) groups)]
    keys))

(defn list-all-triggers []
  (let [groups (qs/get-trigger-group-names)
        keys (mapcat #(qs/get-trigger-keys (m/group-equals %)) groups)]
    keys))

(defn get-triggers-of-job [job-name]
  (qs/get-triggers-of-job job-name))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Helpers

(def- fn-param "job.clojure.fn")
(defn- invoke-fn-from-context [context]
  (let [f (-> context
              .getJobDetail
              .getJobDataMap
              (.get fn-param))]
    (f context)))

(deftype JobThatLooksUpItsFunction []
  org.quartz.Job
  (execute [this context] (invoke-fn-from-context context)))

(defn- build-job [job-key handler]
  (j/build
   (j/of-type JobThatLooksUpItsFunction)
   (j/with-identity job-key)
   (j/using-job-data {fn-param handler})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Single crons (EASY!)

(defn- make-cron-trigger [trigger-key cron]
  (t/build
   (t/with-identity trigger-key)
   ;; (t/start-now) ;; don't start on scheduler start-up! Run when specified.
   (t/with-schedule
     (cron/schedule
      (cron/cron-schedule cron)
      ;; XXX time zone considerations?
      ;;(cron/in-time-zone (java.util.TimeZone/getTimeZone "Europe/Moscow"))
      (cron/with-misfire-handling-instruction-fire-and-proceed)))))

(defn- make-cron-entry [id handler cron opts]
  (let [t-key (t/key id)
        t (make-cron-trigger t-key cron)
        j-key (j/key id)
        j (build-job j-key handler)]
    {:trigger-key t-key
     :trigger t
     :job-key j-key
     :job j}))

;; (def r (make-cron-entry "cron1"
;;                         #(do
;;                              (print "Map is ")
;;                              (clojure.pprint/pprint %))
;;                         "/5 * * * * ? *"
;;                         {:output "bar"}))
;; (schedule-jobs [r])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Multi-crons (aka unions of crons)

;; ## Locks

(defn- make-lock [] (ref nil))

(defn- ask-for-lock
  "Return value indicates whether you got the lock or not"
  [lk id]
  (dosync
   (if (nil? @lk)
     (do
       (ref-set lk id)
       true)
     false)))

(defn- owns-lock [lk id]
  (= @lk id))

(defn- release-lock [lk id]
  (when (not (owns-lock lk id))
    (throw (RuntimeException. "You didn't own the lock!")))
  (dosync
   (ref-set lk nil)))

;; ## Actual cron unions

(defn- wrap-locking [lock numbered-id handler]
  (fn [ctx]
    (cond (owns-lock lock numbered-id)
          (log/infof "Id %s still held lock from last time; no-op" numbered-id)
          (ask-for-lock lock numbered-id)
          (try
            (do
              (log/infof "Id %s got lock!" numbered-id)
              (handler ctx))
            (catch Exception e
              (log/errorf "Caught exception: %s" (.getMessage e)))
            (finally (release-lock lock numbered-id)))
          :else
          (log/infof "Id %s failed to get lock; no-op" numbered-id))))

(defn- make-crons-entries [base-id handler crons opts]
  (let [indices (range (count crons))
        lock (make-lock)]
    (vec
     (map (fn [i cron]
            (let [this-id (str base-id "-" i)]
              (make-cron-entry this-id
                               (wrap-locking lock this-id handler)
                               cron
                               opts)))
          indices
          crons))))

;; (def c (make-crons-entries "crons"
;;                            #(do
;;                               (Thread/sleep 300)
;;                               (println (format "I am crons and arg is %s" %)))
;;                            ["/2 * * * * ? *"
;;                             "/3 * * * * ? *"]
;;                            {}))
;; (schedule-jobs c)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Periods

(defn ->interval [p]
  (let [cisb (CalendarIntervalScheduleBuilder/calendarIntervalSchedule)]
    (cond
     (time/seconds? p) (ci/with-interval-in-seconds cisb (.getSeconds p))
     (time/minutes? p) (ci/with-interval-in-minutes cisb (.getMinutes p))
     (time/hours? p)   (ci/with-interval-in-hours   cisb (.getHours p))
     (time/days? p)    (ci/with-interval-in-days    cisb (.getDays p))
     (time/weeks? p)   (ci/with-interval-in-weeks   cisb (.getWeeks p))
     :else (log-and-throw
            (format "Unrecognized period type: p was %s, type was %s"
                    p
                    (class p))))))

(defn- make-periodic-trigger [trigger-key period]
  (t/build
   (t/with-identity trigger-key)
   ;; Intervals do start at scheduler start-up (unlike crons,
   ;; which run as scheduled.)
   (t/start-now)
   (t/with-schedule
     (ci/with-misfire-handling-instruction-fire-and-proceed
       (->interval period)))))

(defn- make-periodic-entry
  [id handler period opts]
  (let [t-key (t/key id)
        t (make-periodic-trigger t-key period)
        j-key (j/key id)
        j (build-job j-key handler)]
    {:trigger-key t-key
     :trigger t
     :job-key j-key
     :job j}))

;; (def p (make-periodic-entry "period1"
;;                             #(do
;;                                  (print "I am periodic and map is ")
;;                                  (print %))
;;                             (time/seconds 2)
;;                             {:output "bar"}))
;; (schedule-jobs [p])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # feeds -> jobs + triggers

(defn- cronned?  [poll-expr] (list? poll-expr))
(defn- periodic? [poll-expr] (instance? org.joda.time.ReadablePeriod poll-expr))

(defn ->job [feed]
  (let [{id :id
         date-expr :date-expr
         action :action
         poll-expr :poll-expr
         filters :filters}
        feed]
    (cond
     (and (cronned? poll-expr)
          (> (count poll-expr) 1))
     (make-crons-entries id action poll-expr "opts")
     (and (cronned? poll-expr)
          (= (count poll-expr) 1))
     (make-cron-entry id action (first poll-expr) "opts")
     (periodic? poll-expr)
     (make-periodic-entry id action poll-expr "opts")
     :else
     (log-and-throw
      (format "Don't know how to convert poll-expr %s to a cron-entry"
              poll-expr)))))

(defn schedule-jobs [jobs]
  (doseq [j jobs]
    (when-let [pre-existing (qs/get-job (:job-key j))]
      (log/infof "Found existing job named %s; unscheduling the old one"
                 (:job-key j))
      (qs/delete-job (:job-key j)))
    (when-let [pre-existing (qs/get-trigger (:trigger-key j))]
      (log/infof "Found existing trigger named %s; unscheduling the old one"
                 (:trigger-key j))
      (qs/delete-trigger (:trigger-key j)))
    (qs/schedule (:job j) (:trigger j))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Manage scheduler state

(defn init-scheduler! "Call this before doing anything with the scheduler" []
  (qs/initialize))

(defn start-scheduler! []
  (qs/start))

(defn pause-scheduler! []
  ;; What is the difference between this and (qs/pause-all!)? Not sure,
  ;; but I *think* coming out of standby will retrigger any "missed" jobs
  ;; whereas jobs that are unpaused won't retrigger (instead will wait
  ;; until their next scheduled trigger-hit).
  (qs/standby))

(defn clear-scheduler! []
  (qs/clear!))

(defn shutdown-scheduler! "Must do this in order for app to shutdown" []
  (qs/shutdown))

(defn blast-and-load-feeds! "A parsed-feed is a map" [parsed-feeds]
  (clear-scheduler!)
  (schedule-jobs (flatten (map ->job parsed-feeds))))
