(defproject danger-unicron "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [com.onekingslane.danger/clojure-common-utils "0.0.24"]
                 [com.onekingslane.danger/diesel "1.0.3"]
                 [clj-time "0.7.0"]
                 ;; [compojure "1.1.6"]
                 ;; [ring "1.3.0"]
                 ]
  ;;:plugins [[lein-ring "0.8.11"]]
  ;;:ring {:handler danger-unicron/handler}
  :resource-paths ["resources"]
)
