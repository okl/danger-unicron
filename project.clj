(defproject com.onekingslane.danger/unicron "0.1.0-SNAPSHOT"
  :description "Watch for new files, take action when they appear"
  :url "https://github.com/okl/danger-unicron"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [com.onekingslane.danger/clojure-common-utils "0.0.24"]
                 [com.onekingslane.danger/diesel "1.1.0"]
                 [com.onekingslane.danger/date-expr "0.3.0"]
                 [com.onekingslane.danger/data-watch-dog "0.1.3"]
                 [clj-time "0.7.0"]
                 [clj-yaml "0.4.0"]
                 [org.slf4j/slf4j-log4j12 "1.7.5"]
                 [org.clojure/tools.cli "0.3.1"]
                 ;; Daemonization
                 [org.apache.commons/commons-daemon "1.0.9"]
                 ;; Scheduling
                 [clojurewerkz/quartzite "1.3.0"]
                 ;; History
                 [org.clojure/java.jdbc "0.3.5"]
                 [mysql/mysql-connector-java "5.1.25"]
                 [org.xerial/sqlite-jdbc "3.7.2"]
                 [com.mchange/c3p0 "0.9.2.1"] ;; for connection pooling
                 ;; Alerting
                 [com.draines/postal "1.11.1"]
                 ]
  :main unicron.core
  ;; :main unicron.cli
  :repositories [["danger"
                  {:url "s3p://okl-danger-wagon/releases/"}]]
  :jvm-opts ["-Djna.nosys=true"]
  :resource-paths ["resources"
                   "conf"]
  :target-path "target/%s"
  :profiles {:test {;; disable logging when running tests
                    :resource-paths ["test/resources"]}
             :uberjar {:aot :all}})
