(ns unicron.config
  "Reads and serves up config"
  {:author "Matt Halverson", :date "Thu Oct  2 12:28:27 PDT 2014"}
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clj-yaml.core :as yaml]
            [roxxi.utils.print :refer [print-expr]]
            [roxxi.utils.common :refer [def-]])
  (:require [unicron.state.in-memory :as im]
            [unicron.state.jdbc :as jdbc]))

;; # Toplevel fn to read config

(defn- make-cfg-from-yml-file [filename]
  (yaml/parse-string (slurp filename) false))

(defn- read-master-cfg []
  (make-cfg-from-yml-file (io/resource "unicron.yml")))

;; ## System-level properties

(defn cfg-dir []
  (str (get (read-master-cfg) "cfg_dir")
       "/"))

;; # Feeds

(def- feeds-cfg-name "feeds.clj")
(defn- feeds-cfg-path [] (str (cfg-dir) feeds-cfg-name))
(defn- default-feeds-cfg [] (list))

(defn read-feeds-cfg []
  (let [path (feeds-cfg-path)
        s (slurp path)]
    (when (nil? path)
      (throw (RuntimeException. "feeds-cfg-path is nil")))
    (if (empty? s)
      (do
        (log/warn "Feeds-cfg is empty string... using default feeds cfg!")
        (default-feeds-cfg))
      (read-string s))))

;; # History

(def- history-cfg-name "history.yml")
(defn- history-cfg-path [] (str (cfg-dir) history-cfg-name))

(defn- im-history [cfg]
  (im/make-in-memory-history))

(def- sqlite-db-spec-template
  {:classname "org.sqlite.JDBC"
   :subprotocol "sqlite"})
(defn- sqlite-history [cfg]
  (let [filepath (get-in cfg ["db_spec" "path"])
        db-spec (merge sqlite-db-spec-template
                       {:subname filepath})]
    (jdbc/make-sqlite-history db-spec)))

(defn make-history-from-cfg []
  (let [path (history-cfg-path)
        cfg (yaml/parse-string (slurp path) false)
        impl (get cfg "history_impl")]
    (condp = impl
      "in_memory" (im-history cfg)
      "sqlite" (sqlite-history cfg)
      :else (throw (RuntimeException. (str "Unrecognized history_impl: " impl))))))
