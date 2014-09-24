(ns unicron.connection
  "Parse connection info"
  {:author "Matt Halverson", :date "Thu Sep  4 16:21:07 PDT 2014"}
  (:require [diesel.core :refer [definterpreter]]
            [unicron.utils :refer [log-and-throw]]
            [clojure.tools.logging :as log]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # The language

(definterpreter interp-conn []
  ;; top-level
  ['conn => :conn]
  ;; types
  ['s3 => :s3]
  ['local => :local]
  ['ftp => :ftp]
  ['sftp => :sftp])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # The implementation

(defmacro assert-contains [map field]
  `(when-not (contains? ~map ~field)
     (log/warnf "Map %s was missing the required field %s" ~map ~field)))

(defmethod interp-conn :conn [[token id typed-conn]]
  {:id id
   :info (interp-conn typed-conn)})

(defmethod interp-conn :s3 [[token details]]
  (assert-contains details :secret-key)
  (assert-contains details :access-key)
  {:s3-config details})

(defmethod interp-conn :local [[token details]]
  ;; {:local-config details}
  (log-and-throw "Haven't implemented interp-conn for local yet"))

(defmethod interp-conn :ftp [[token details]]
  ;; {:ftp-config details}
  (log-and-throw "Haven't implemented interp-conn for ftp yet"))

(defmethod interp-conn :sftp [[token details]]
  ;;{:sftp-config details}
  (log-and-throw "Haven't implemented interp-conn for sftp yet"))
