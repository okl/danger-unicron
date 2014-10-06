(ns unicron.alert
  "Alerting"
  {:author "Matt Halverson", :date "Thu Oct  2 16:26:15 PDT 2014"}
  (:require [clojure.tools.logging :as log]
            [roxxi.utils.print :refer [print-expr]]
            [roxxi.utils.common :refer [def-]]
            [postal.core :as postal]
            [unicron.config :as cfg]))

(def- default-from "unicron-bot@onekingslane.com")

(defn email
  ([subject body]
     (email default-from (cfg/email-to) subject body))
  ([from to subject body]
     (postal/send-message {:from from
                           :to to
                           :subject subject
                           :body body})))
