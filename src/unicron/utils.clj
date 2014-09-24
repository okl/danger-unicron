(ns unicron.utils
  "Unicron utils"
  {:author "Matt Halverson"
   :date "Fri Aug 22 17:57:32 PDT 2014"}
  (:require [clojure.tools.logging :as log]))

(defmacro log-and-throw [msg]
  `(do
     (log/error ~msg)
     (throw (RuntimeException. ~msg))))

(defmacro assert-not-nil [sym]
  `(when (nil? ~sym)
     (log-and-throw (format "Error: %s was unspecified" ~(name sym)))))

(defn arity
  "Returns the arity of a function. Doesn't work for macros.
See http://stackoverflow.com/questions/1696693/clojure-how-to-find-out-the-arity-of-function-at-runtime"
  [f]
  (let [m (first (.getDeclaredMethods (class f)))
        p (.getParameterTypes m)]
    (alength p)))
