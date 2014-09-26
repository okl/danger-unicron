(ns unicron.state
  "Keep state about what has been seen"
  {:author "Matt Halverson", :date "Thu Sep  4 18:40:00 PDT 2014"}
  (:require [clojure.tools.logging :as log]
            [clj-time.core :as t]
            [diesel.core :refer [definterpreter]]
            [date-expr.core :as de]
            [unicron.utils :refer [log-and-throw]])
  (:import (org.joda.time ReadablePeriod)))

;; # Individual events (at the file-level)

(defrecord Event [event-time date-expr uri uri-time state msg is-dir dir-uri])

(defn- make-event
  ([m]
     (map->Event m))
  ([event-time date-expr uri uri-time state msg is-dir dir-uri]
     (->Event event-time date-expr uri uri-time state msg is-dir dir-uri)))

(defn make-file-event [event-time date-expr uri uri-time state msg]
  (make-event {:event-time event-time
               :date-expr date-expr
               :uri uri
               :uri-time uri-time
               :state state
               :msg msg
               :is-dir false
               :dir-uri nil}))
(defn make-dir-event [event-time date-expr uri uri-time state msg]
  (make-event {:event-time event-time
               :date-expr date-expr
               :uri uri
               :uri-time uri-time
               :state state
               :msg msg
               :is-dir true
               :dir-uri nil}))
(defn make-file-in-dir-event [event-time date-expr uri uri-time state msg dir-uri]
  (make-event {:event-time event-time
               :date-expr date-expr
               :uri uri
               :uri-time uri-time
               :state state
               :msg msg
               :is-dir false
               :dir-uri dir-uri}))

;; # Histories of events (at the datafeed-level)

(defprotocol History
  ;; Read methods
  (file-has-history? [_ date-expr]
    "Returns true if the date-expr has any single-file events recorded for it,
    false otherwise.")
  (dir-has-history? [_ date-expr]
    "Returns true if the date-expr has any directory-level events recorded for
    it, false otherwise.")
  (latest-file-match [_ date-expr]
    "Returns the latest single-file event recorded for the date-expr,
    regardless of the state of that event.")
  (latest-dir-match [_ date-expr]
    "Returns the latest directory-level event recorded for the date-expr,
    regardless of the state of that event.")
  (live-directories [_ date-expr]
    "Returns the directories for that date-expr that are still being
    watched (i.e. they have an observed-at time but not a completed-at time)

    If the date-expr is s3://bucket/%Y/%m/%d/%p, and the feed has a
    directory-ttl of 1 day, and it's the evening of 2014/09/03, then
    live directories might return the following:
      s3://bucket/2014/09/03/AM
      s3://bucket/2014/09/03/PM

    If the feed had a filter clause with regex 'match$', the following file
    WOULD NOT make s3://bucket/2014/09/04/AM a live-directory:
      s3://bucket/2014/09/04/AM/.../foo.txt
    However, the appearance of either of these files WOULD make it a live
    directory, and would start the ttl ticking:
      s3://bucket/2014/09/04/AM/.../01/match
      s3://bucket/2014/09/04/AM/.../02/match")
  (matches-in-directory [_ date-expr directory-uri]
    "With directories, there are 2 levels of uri we care about. There's
    the uri of the directory, and the uris of files within that directory.
    Given a directory-uri, this returns all the *file* uris that have
    matched in that directory.

    In conjunction with `live-directories`, this method enables you to detect
    *new* files in *old* directories!")
  ;; Write methods
  (expire-old-directories! [_ date-expr moment-of-death]
    "Looks for live-directories matching the date-expr, filters just the ones
    that have been watched since before the moment-of-death, and marks them
    as completed")
  (observed-file!        [_ observed-at date-expr uri uri-time msg])
  (observed-dir!         [_ observed-at date-expr uri uri-time msg])
  (observed-file-in-dir! [_ observed-at date-expr uri uri-time msg dir-uri])
  (processing-file!        [_ started-at date-expr uri uri-time msg])
  (processing-file-in-dir! [_ started-at date-expr uri uri-time msg dir-uri])
  (completed-file!        [_ completed-at date-expr uri uri-time msg])
  (completed-dir!         [_ completed-at date-expr uri uri-time msg])
  (completed-file-in-dir! [_ completed-at date-expr uri uri-time msg dir-uri]))

;; # Utils

(defn now "In seconds since epoch" [] (quot (System/currentTimeMillis) 1000))

(defn moment-of-death
  "Given a time-to-live in seconds, computes the moment-of-death in
  seoconds since the epoch"
  [ttl]
  (cond
   (integer? ttl)
   (- (now) ttl)
   (isa? (class ttl) ReadablePeriod)
   (de/->seconds-since-epoch
    (t/minus (de/->joda-date-time (now)) ttl))
   :else
   (throw (RuntimeException.
           (format "Don't know how to compute moment-of-death for %s"
                   ttl)))))
