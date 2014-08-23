(ns unicron.utils
  "Unicron utils"
  {:author "Matt Halverson"
   :date "Fri Aug 22 17:57:32 PDT 2014"}
  (:require [clojure.tools.logging :as log]))

(defmacro log-and-throw [msg]
  `(do
     (log/error ~msg)
     (throw (RuntimeException. ~msg))))
