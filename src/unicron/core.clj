(ns unicron.core
  "Main application loop; load config, manage scheduler state"
  {:author "Matt Halverson", :date "Wed Aug 27 13:58:01 PDT 2014"}
  (:require [unicron.feed :as f]
            [unicron.scheduler :as s]
            [unicron.state.in-memory :as im]))

;; # Feed cfg

(def cfg
  (list
   '(feed (id "test-cron")
          (date-expr "s3://bucket/foo/%Y/%m/%d/bar")
          (action (clj (fn [filepath]
                         (do
                           (print "I am test-cron ")
                           (println (quot (System/currentTimeMillis) 1000))))))
          (poll-expr (cron "/5 * * * * ? *"))
          (filter (regex "foo")))
   '(feed (id "test-crons")
          (date-expr "s3://bucket/foo/%Y/%m/%d/bar")
          (action (clj (fn [filepath]
                         (do
                           (Thread/sleep 200)
                           (print "I am test-crons ")
                           (println (quot (System/currentTimeMillis) 1000))))))
          (poll-expr (crons "/2 * * * * ? *" "/3 * * * * ? *"))
          (filter (regex "foo")))
   '(feed (id "test-period")
          (date-expr "s3://bucket/foo/%Y/%m/%d/bar")
          (action (clj (fn [filepath]
                         (do
                           (print "I am test-period ")
                           (println (quot (System/currentTimeMillis) 1000))))))
          (poll-expr (interval (seconds 4)))
          (filter (regex "foo")))))

(defn read-cfg []
  cfg)

;; # Possibly-useful helper

(defn reload-cfg-and-restart-scheduler []
  (let [cfg (read-cfg)]
    (s/blast-and-load-feeds! (map f/interp-feed cfg))
    (s/start-scheduler!)))


;; # clojure.tools.namespace stuff

(defn create-instance []
  {:history (im/make-in-memory-history)})

(defn start [app]
  (s/init-scheduler!)
  (s/start-scheduler!)
  (let [h (:history app)
        env {:history h}
        feeds (read-cfg)
        parsed-feeds (map #(f/interp-feed % env) feeds)]
    (s/schedule-jobs parsed-feeds)))

(defn stop [app]
  (s/pause-scheduler!)
  (s/clear-scheduler!)
  (s/shutdown-scheduler!))

;; # main

(defn -main [& args]
  (let [app (create-instance)]
    (start app)))
