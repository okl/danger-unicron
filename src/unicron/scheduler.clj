(ns unicron.scheduler
  "Schedule tasks, handle unions of crons"
  {:author "Matt Halverson"
   :date "Fri Aug 22 16:06:44 PDT 2014"}
  (:require [clojure.tools.logging :as log])
  (:require [roxxi.utils.print :refer [print-expr]]
            [roxxi.utils.common :refer [def-]])
  (:require [cronj.core :refer :all]))

;; # Playing around

(defn print-handler [t opts]
  (println (:output opts) ": " t))
;; (def print-task
;;   {:id "print-task"
;;    :handler print-handler
;;    :schedule "/2 * * * * * *"
;;    :opts {:output "Hello There"}})
;; (def cj (cronj :entries [print-task]))
;; (start! cj)
;; (stop! cj)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Locks

(defn make-lock [] (ref nil))

(defn ask-for-lock
  "Return value indicates whether you got the lock or not"
  [lk id]
  (dosync
   (if (nil? @lk)
     (do
       (ref-set lk id)
       true)
     false)))

(defn owns-lock [lk id]
  (= @lk id))

(defn release-lock [lk id]
  (when (not (owns-lock lk id))
    (throw (RuntimeException. "You didn't own the lock!")))
  (dosync
   (ref-set lk nil)))

;; # More playing

;; (def lock (make-lock))
;; (def cj-2 (cronj :entries [{:id "multi-cron-task-1"
;;                             :handler (fn [t opts]
;;                                        (let [me "multi-cron-task-1"]
;;                                          (cond (owns-lock lock me)
;;                                                (println "Still held lock from last time; no-op")
;;                                                (ask-for-lock lock me)
;;                                                (do
;;                                                  (println (format "Id %s got lock!" me))
;;                                                  (print-handler t opts)
;;                                                  (release-lock lock me))
;;                                                :else
;;                                                (println (format "Id %s failed to get lock" me)))))
;;                             :schedule "/2 * * * * * *"
;;                             :opts {:output "Hello There"}}
;;                            {:id "multi-cron-task-2"
;;                             :handler (fn [t opts]
;;                                        (let [me "multi-cron-task-2"]
;;                                          (cond (owns-lock lock me)
;;                                                (println "Still held lock from last time; no-op")
;;                                                (ask-for-lock lock me)
;;                                                (do
;;                                                  (println (format "Id %s got lock!" me))
;;                                                  (print-handler t opts)
;;                                                  (release-lock lock me))
;;                                                :else
;;                                                (println (format "Id %s failed to get lock" me)))))
;;                             :schedule "/3 * * * * * *"
;;                             :opts {:output "Hello There"}}]))
;; (start! cj-2)
;; (stop! cj-2)

;; # Cron-unions!

(defn multi-cron-handler [lock numbered-id handler]
  (fn [t opts]
    (cond (owns-lock lock numbered-id)
          (println "Still held lock from last time; no-op")
          (ask-for-lock lock numbered-id)
          (do
            (println (format "Id %s got lock!" numbered-id))
            (handler t opts)
            (release-lock lock numbered-id))
          :else
          (println (format "Id %s failed to get lock" numbered-id)))))

(defn multi-cron-entries [id handler crons opts]
  (let [indices (range (count crons))
        lock (make-lock)]
    (vec
     (map (fn [i cron]
            (let [this-id (str id "-" i)]
              {:id this-id
               :handler (multi-cron-handler lock this-id handler)
               :schedule cron
               :opts opts}))
          indices
          crons))))

(def cj-3 (cronj :entries
                 (multi-cron-entries "multi-cron-task"
                                     print-handler
                                     ["/2 * * * * * *"
                                      "/3 * * * * * *"]
                                     {:output "Hello There"})))
(start! cj-3)
(stop! cj-3)
