(defproject com.onekingslane.danger/unicron "0.1.0-SNAPSHOT"
  :description "Watch for new files, take action when they appear"
  :url "https://github.com/okl/danger-unicron"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.clojure/core.match "0.2.1"]
                 [com.onekingslane.danger/clojure-common-utils "0.0.24"]
                 [com.onekingslane.danger/diesel "1.1.0"]
                 [com.onekingslane.danger/date-expr "0.3.0"]
                 [dwd "0.1.0-SNAPSHOT"]
                 [clj-time "0.7.0"]
                 [clojurewerkz/quartzite "1.3.0"]
                 [org.slf4j/slf4j-log4j12 "1.7.5"]
                 [aprint "0.1.0"]
                 [org.clojure/java.jdbc "0.3.5"]
                 [mysql/mysql-connector-java "5.1.25"]
                 [org.xerial/sqlite-jdbc "3.7.2"]
                 ]
  :resource-paths ["resources"]
  ;; disable logging when running tests
  :profiles {:test {:resource-paths ["test/resources"]}}
  )
