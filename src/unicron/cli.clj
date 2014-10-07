(ns unicron.cli
  "CLI wrapper around unicron.core"
  {:author "Matt Halverson", :date "Mon Oct  6 16:03:53 PDT 2014"}
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as s]
            [unicron.core :as core])
  (:gen-class))

(def cli-options
  ;; An option with a required argument
  [["-c" "--config-path CONFIG-PATH" "Path to master config in unicron.yml"
    :default nil
    :validate [#(.exists (clojure.java.io/file %))
               "Must be a filepath to a file that exists"]]
   ["-h" "--help"]])

(defn usage [options-summary]
  (->> ["CLI wrapper around unicron.core"
        ""
        "Usage: program-name [options] action"
        ""
        "Options:"
        options-summary
        ""
        "Actions:"
        "  start    Start unicron"
        "  stop     Stop unicron"
        "  reload   Reload config for unicron"]
       (s/join \newline)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (s/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

l(defn -main [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    ;; Handle help and error conditions
    (cond
      (:help options) (exit 0 (usage summary))
      (not= (count arguments) 1) (exit 1 (usage summary))
      errors (exit 1 (error-msg errors)))
    ;; Execute program with options
    (comment (case (first arguments)
      "start" (core/start options)
      "stop" (core/stop options)
      "reload" (core/reload options)
      (exit 1 (usage summary))))
    (println "Starting")))
