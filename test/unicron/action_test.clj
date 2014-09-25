(ns unicron.action-test
  {:author "Matt Halverson", :date "Wed Sep 24 14:11:28 PDT 2014"}
  (:require [clojure.test :refer :all]
            [roxxi.utils.print :refer [print-expr]]
            [roxxi.utils.common :refer [def-]]
            [unicron.feed :as f]
            [unicron.core :refer :all])
  (:import [java.io BufferedReader StringReader]))

;; # Helpers

(defn- fresh-action []
  (let [app (create-instance)
        h (:history app)
        f (f/interp-feed
           '(feed
             (id "foobar")
             (conn {:access-key "1234"
                    :secret-key "ABCD"})
             (date-expr "s3://bucket/%Y/%m/%d/dev/" "America/Los_Angeles")
             (starting-after "s3://bucket/2014/08/05/dev/")
             (action (clj
                      (fn [uri ts]
                        (println (format "I am action. uri is %s, ts is %s" uri ts)))))
             (poll-expr (cron "/20 * * * * ? *"))
             (is-dir (dir-ttl (minutes 10)))
             (filter (regex ".+/file/.+"))
             )
           {:history h})
        a (:action f)
        thunk (fn [] (a {}))]
    thunk))

(defmacro line-count [a]
  `(let [s# (with-out-str (~a))]
     (with-open [sr# (StringReader. s#)
                 br# (BufferedReader. sr#)]
       (count (line-seq br#)))))

(defn- my-lfmp [event-seq]
  (fn [_ _ prefix _]
    (filter #(.startsWith (:uri %) prefix)
            event-seq)))

(defn- my-lfnt [event-seq]
  (fn [_ _ newer-than-this _]
    (let [prefix-len (count "s3://bucket/2014/08/07")
          prefix #(subs % 0 prefix-len)]
      (filter #(pos? (compare (prefix (:uri %))
                              (prefix newer-than-this)))
              event-seq))))

;; # Tests!

(def- event-seq
  [{:uri "s3://bucket/2014/08/06/dev/file/01.21.41", :ts 1407308400}
   {:uri "s3://bucket/2014/08/06/dev/file/01.21.47", :ts 1407308400}
   {:uri "s3://bucket/2014/08/06/dev/file/02.31.06", :ts 1407308400}
   {:uri "s3://bucket/2014/08/06/dev/file/02.42.17", :ts 1407308400}
   {:uri "s3://bucket/2014/08/06/dev/file/09.45.09", :ts 1407308400}
   {:uri "s3://bucket/2014/08/06/dev/other-file/09.45.16", :ts 1407308400}
   {:uri "s3://bucket/2014/08/07/dev/file/00.25.20", :ts 1407394800}])

(deftest action-wrapper-test
  (testing "While no new files are showing up, the action doesn't get run"
    (let [e event-seq
          a (fresh-action)]
      (with-redefs
        [unicron.action-wrapper/list-files-matching-prefix (my-lfmp e)
         unicron.action-wrapper/list-files-newer-than (my-lfnt e)]
        (is (= 6 (line-count a)))
        (is (= 0 (line-count a)))
        (is (= 0 (line-count a)))
        (is (= 0 (line-count a)))))))

(comment
  (clojure.tools.namespace.repl/refresh)
  (action-wrapper-test)
  )
(comment
  ((my-lfmp event-seq) nil nil "s3://bucket/2014/08/06" nil)
  ((my-lfmp event-seq) nil nil "s3://bucket/2014/08/07" nil)
  ((my-lfmp event-seq) nil nil "s3://bucket/2014/08/08" nil)
  ((my-lfmp event-seq) nil nil "s3://bucket/2014/08/0" nil)

  ((my-lfnt event-seq) nil nil "s3://bucket/2014/08/04/dev/..." nil)
  ((my-lfnt event-seq) nil nil "s3://bucket/2014/08/05/dev/..." nil)
  ((my-lfnt event-seq) nil nil "s3://bucket/2014/08/06/dev/..." nil)
  ((my-lfnt event-seq) nil nil "s3://bucket/2014/08/07/dev/..." nil)
)
