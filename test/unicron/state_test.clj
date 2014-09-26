(ns unicron.state-test
  {:author "Matt Halverson", :date "Wed Sep 24 09:52:10 PDT 2014"}
  (:require [clojure.test :refer :all]
            [unicron.state :refer :all]
            [unicron.state.in-memory :refer [make-in-memory-history]]
            [unicron.state.jdbc :refer [make-sqlite-history]]
            [clj-time.core :refer [seconds minutes hours days weeks]]
            [roxxi.utils.print :refer [print-expr]]))

;; # State tests

(def sep-24-2014 1411577810)

(defn seconds-ago [sec] (- sep-24-2014 sec))
(defn minutes-ago [min] (seconds-ago (* 60 min)))
(defn hours-ago [hrs] (minutes-ago (* 60 hrs)))
(defn days-ago [days] (hours-ago (* 24 days)))
(defn weeks-ago [weeks] (days-ago (* 7 weeks)))

(deftest moment-of-death-test
  (with-redefs
    [unicron.state/now (constantly sep-24-2014)]
    (testing "Moments of death are correctly computed for"
      (testing "ints"
        (is (= (seconds-ago 5) (moment-of-death 5)))
        (is (= (seconds-ago 532) (moment-of-death 532))))
      (testing "joda Periods"
        (is (= (seconds-ago 5) (moment-of-death (seconds 5))))
        (is (= (minutes-ago 4) (moment-of-death (minutes 4))))
        (is (= (hours-ago 3)   (moment-of-death (hours 3))))
        (is (= (days-ago 2)    (moment-of-death (days 2))))
        (is (= (weeks-ago 1)   (moment-of-death (weeks 1))))))
    (testing "Moments of death can't be computed for other types"
      (is (thrown? Exception (moment-of-death "asdf")))
      (is (thrown? Exception (moment-of-death :a)))
      (is (thrown? Exception (moment-of-death {})))
      (is (thrown? Exception (moment-of-death [])))
      (is (thrown? Exception (moment-of-death '()))))))

;; # In-memory tests

(defn now-ms [] (System/currentTimeMillis))
(def de-1 "s3://foo/bar/%Y/")
(def de-2 "s3://foo/baz/%Y/")


(defmacro expand-into-state-tests [h-type h]
  (let [test-name (symbol (str h-type "-test"))]
    `(deftest ~test-name
       (testing "observing files"
         (let [h# ~h]
           (is (false? (file-has-history? h# de-1)))
           (is (false? (file-has-history? h# de-2)))
           (is (nil? (latest-file-match h# de-1)))
           (is (nil? (latest-file-match h# de-1)))
           (is (nil? (latest-file-match h# de-2)))
           (observed-file! h# (now-ms) de-1 "s3://foo/bar/2014/" 44000
                           "saw one")

           (is (true? (file-has-history? h# de-1)))
           (is (false? (file-has-history? h# de-2)))
           (is (= "s3://foo/bar/2014/" (:uri (latest-file-match h# de-1))))
           (is (nil? (latest-file-match h# de-2)))

           (observed-file! h# (now-ms) de-1 "s3://foo/bar/2015/" 44001
                           "saw another one")

           (is (= "s3://foo/bar/2015/" (:uri (latest-file-match h# de-1))))))

       (testing "observing directories"
         (let [h# ~h]
           (is (false? (dir-has-history? h# de-1)))
           (observed-dir! h# 1000 de-1 "s3://foo/bar/2014/" 44000
                          "saw one; continuing to monitor for 24 hours from now")
           (is (true? (dir-has-history? h# de-1)))
           (is (= 0 (count (matches-in-directory h# de-1 "s3://foo/bar/2014/"))))
           (observed-file-in-dir! h# 1000 de-1 "s3://foo/bar/2014/a.txt" 44000
                                  "found a file in the dir" "s3://foo/bar/2014/")
           (is (= 1 (count (matches-in-directory h# de-1 "s3://foo/bar/2014/"))))
           (observed-file-in-dir! h# 1002 de-1 "s3://foo/bar/2014/b.txt" 44001
                                  "found a file in the dir" "s3://foo/bar/2014/")
           (is (= 2 (count (matches-in-directory h# de-1 "s3://foo/bar/2014/"))))
           (observed-file-in-dir! h# 1003 de-1 "s3://foo/bar/2014/c.txt" 44002
                                  "found a file in the dir" "s3://foo/bar/2014/")
           (is (= 3 (count (matches-in-directory h# de-1 "s3://foo/bar/2014/"))))
           (is (true? (dir-has-history? h# de-1)))
           (is (= "s3://foo/bar/2014/" (:uri (latest-dir-match h# de-1))))
           (is (= 1 (count (live-directories h# de-1))))

           (observed-dir! h# 2000 de-1 "s3://foo/bar/2015/" 45000
                          "saw another, monitoring for a while")
           (is (true? (dir-has-history? h# de-1)))
           (is (= 2 (count (live-directories h# de-1))))
           (is (= "s3://foo/bar/2015/" (:uri (latest-dir-match h# de-1))))

           (expire-old-directories! h# de-1 1999)
           (is (= 1 (count (live-directories h# de-1))))
           (is (= 3 (count (matches-in-directory h# de-1 "s3://foo/bar/2014/"))))
           (is (= 0 (count (matches-in-directory h# de-1 "s3://foo/bar/2015/"))))
           (is (= "s3://foo/bar/2015/" (:uri (latest-dir-match h# de-1))))

           (expire-old-directories! h# de-1 2001)
           (is (= 0 (count (live-directories h# de-1))))
           (is (= 3 (count (matches-in-directory h# de-1 "s3://foo/bar/2014/"))))
           (is (= 0 (count (matches-in-directory h# de-1 "s3://foo/bar/2015/"))))
           (is (= "s3://foo/bar/2015/" (:uri (latest-dir-match h# de-1))))

           (is (false? (dir-has-history? h# de-2)))
           (is (= 0 (count (live-directories h# de-2)))))))))

(expand-into-state-tests
 "in-memory-history" (make-in-memory-history))
(expand-into-state-tests
 "sqlite-history" (make-sqlite-history :blast-away-history? true
                                       :db {:subprotocol "sqlite"
                                            :subname "test/sqlite.db"}))

;; XX Flesh this out with generative testing :D
;; https://github.com/clojure/test.check
;; https://github.com/clojure/test.generative
