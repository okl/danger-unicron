(defproject com.onekingslane.danger/unicron "0.1.0-SNAPSHOT"
  :description "Watch for new files, take action when they appear"
  :url "https://github.com/okl/danger-unicron"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [com.onekingslane.danger/clojure-common-utils "0.0.24"]
                 [com.onekingslane.danger/diesel "1.0.3"]
                 [clj-time "0.7.0"]
                 [clojurewerkz/quartzite "1.3.0"]
                 [org.slf4j/slf4j-log4j12 "1.7.5"]
                 ;; [compojure "1.1.6"]
                 ;; [ring "1.3.0"]
                 ]
  ;;:plugins [[lein-ring "0.8.11"]]
  ;;:ring {:handler unicron/handler}
  :resource-paths ["resources"]
)
