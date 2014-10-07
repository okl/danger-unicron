(ns unicron.core
  "Main application loop; load config, manage scheduler state"
  {:author "Matt Halverson", :date "Wed Aug 27 13:58:01 PDT 2014"}
  (:require [clojure.tools.cli :refer [parse-opts]]
            [unicron.feed :as f]
            [unicron.scheduler :as s]
            [unicron.config :as cfg]
            [roxxi.utils.print :refer [print-expr]])
  (:import [org.apache.commons.daemon Daemon DaemonContext])
  (:gen-class :implements [org.apache.commons.daemon.Daemon]))

;; # Cfg

(defn- feeds-cfg []
  (cfg/read-feeds-cfg))

(defn- create-history []
  (cfg/make-history-from-cfg))

;; # App lifecycle

(defn create-instance [& {:keys [history feeds-sexps]}]
  (let [h (or history
              (create-history))
        env {:history h}
        fc (or feeds-sexps (feeds-cfg))
        parsed-feeds (map #(f/interp-feed % env) fc)]
    {:history h
     :parsed-feeds parsed-feeds}))

(defn init [app]
  (s/init-scheduler!))

(defn start
  ([]
     (start (create-instance)))
  ([app]
     (when-let [parsed-feeds (:parsed-feeds app)]
       (s/schedule-jobs (s/->jobs parsed-feeds)))
     (s/start-scheduler!)))

(defn stop [app]
  (s/pause-scheduler!)
  (s/clear-scheduler!))

(defn reload-cfg [existing-app]
  (stop existing-app)
  (let [new-app (create-instance)]
    (start new-app)
    new-app))

;; # Daemon stuff

(defn -init [this ^DaemonContext context]
  ;; (.getArguments context)
  (init nil))

(defn -start [this]
  (future (start (create-instance))))

(defn -stop [this]
  (stop nil))

(defn -reload [this]
  (-stop this)
  (-start this))

;; # Main

;; (defn -main [opts]
;;   (let [app (create-instance)]
;;     (start app)))

(defn -main [& args]
  (init args)
  (start))
