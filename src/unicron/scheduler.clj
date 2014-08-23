(ns unicron.scheduler
  "Schedule tasks, handle unions of crons"
  {:author "Matt Halverson"
   :date "Fri Aug 22 16:06:44 PDT 2014"}
  (:require [clojure.tools.logging :as log])
  (:require [roxxi.utils.print :refer [print-expr]]
            [roxxi.utils.common :refer [def-]])
  (:require [clj-time.core :as t]
            [cronj.core :refer :as cj])
  (:require [unicron.utils :refer [log-and-throw]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Single crons (EASY!)

(defn- make-cron-entry [id handler cron opts]
  {:id id
   :handler handler
   :schedule cron
   :opts opts})

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

(defn- make-multi-cron-handler [lock numbered-id handler]
  (fn [t opts]
    (cond (owns-lock lock numbered-id)
          (log/debugf "Id %s still held lock from last time; no-op" numbered-id)
          (ask-for-lock lock numbered-id)
          (do
            (log/debug "Id %s got lock!" numbered-id)
            (handler t opts)
            (release-lock lock numbered-id))
          :else
          (log/debugf "Id %s failed to get lock" numbered-id))))

(defn- make-crons-entries [id handler crons opts]
  (let [indices (range (count crons))
        lock (make-lock)]
    (vec
     (map (fn [i cron]
            (let [this-id (str id "-" i)]
              {:id this-id
               :handler (make-multi-cron-handler lock this-id handler)
               :schedule cron
               :opts opts}))
          indices
          crons))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Periods

(defn- ->cron [p]
  (cond
   (t/minutes? p) (format "0 /%s * * * * *" (.getMinutes p))
   (t/hours? p)   (format "0 * /%s * * * *" (.getHours p))
   (t/days? p)    (format "0 * * /%s * * *" (.getDays p))
   (t/weeks? p)
   (let [w (.getWeeks p)
         d (* 7 w)]
     (when (> d 28)
       (log-and-throw
        (format "Can't cron a weeks-period of longer than 4 weeks... you put %s"
                w)))
     (format "0 * * /%s * * *" d))))

(defn- make-periodic-entry [id handler period opts]
  {:id id
   :handler handler
   :schedule (->cron period)
   :opts opts})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Master cron!

(defn- cronned?  [poll-expr] (list? poll-expr))
(defn- periodic? [poll-expr] (instance? org.joda.time.ReadablePeriod poll-expr))

(defn- ->cron-entry [feed]
  (let [{date-expr :date-expr
         action :action
         poll-expr :poll-expr
         filters :filters}
        feed]
    (cond
     (and (cronned? poll-expr)
          (> (count poll-expr) 1))
     (make-crons-entries "id" "handler" poll-expr "opts")
     (and (cronned? poll-expr)
          (= (count poll-expr) 1))
     (make-cron-entry "id" "handler" poll-expr "opts")
     (periodic? poll-expr)
     (make-periodic-entry "id" "handler" poll-expr "opts")
     :else
     (log-and-throw
      (format "Don't know how to convert poll-expr %s to a cron-entry"
              poll-expr)))))

(defn make-master-cronj [parsed-feeds]
  (let [entries (flatten (map ->cron-entry parsed-feeds))]
    (cronj :entries entries)))

(defn start! [cronj] (cj/start! cronj))
(defn stop!  [cronj] (cj/stop!  cronj))
