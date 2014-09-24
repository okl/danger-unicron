(ns unicron.feed-test
  "Tests for unicron.feed"
  {:author "Matt Halverson", :date "Tue Sep 23 15:02:45 PDT 2014"}
  (:require [clojure.test :refer :all]
            [unicron.feed :refer :all])
  (:import [org.joda.time ReadablePeriod]))

(def env {})
(defmacro interp [expr]
  `(interp-feed ~expr ~env))

;; # Tests!

(deftest id-test
  (testing "ids can be parsed"
    (is (= "eleventy-one"
           (interp '(id "eleventy-one"))))))

(deftest conn-test
  (testing "connection-credentials can be parsed"
    (is (= {:access-key "1234" :secret-key "ABCD"}
           (interp '(conn {:access-key "1234" :secret-key "ABCD"}))))))

(deftest date-expr-test
  (testing "date-exprs can be parsed"
    (testing "when they have a TZ specified"
      (is (= {:date-expr "s3://bucket/foo/%Y/%m/%d/bar",
              :date-expr-tz "Europe/Paris"}
             (interp '(date-expr "s3://bucket/foo/%Y/%m/%d/bar" "Europe/Paris")))))
    (testing "when they have no TZ specified (they get the default one)"
      (is (= {:date-expr "s3://bucket/foo/%Y/%m/%d/bar",
              :date-expr-tz unicron.feed/default-tz}
             (interp '(date-expr "s3://bucket/foo/%Y/%m/%d/bar"))))))
  ;; XX make these tests pass
  ;; (testing "date-exprs can't be parsed if"
  ;;   (testing "they have > 2 args"
  ;;     (is (thrown? Exception (interp '(date-expr "arg-1" "arg-2" "arg-3")))))
  ;;   (testing "they have no args"
  ;;     (is (thrown? Exception (interp '(date-expr))))))
  )

(deftest starting-after-test
  (testing "starting-after clauses can be parsed"
    (is (= "s3://bucket/2014/01/01/"
           (interp '(starting-after "s3://bucket/2014/01/01/"))))))

(deftest action-test
  (let [ok? fn?]
    (testing "actions can be parsed"
      (is (ok? (interp '(action (sh "echo 'Great success in sh'")))))
      (is (ok? (interp '(action (sh "echo 'Great success in sh'")))))
      (is (ok? (interp '(action (sh "echo 'Great success in sh' > /dev/null")))))
      (is (ok? (interp '(action (sh "ps -ef | grep grep")))))
      ;; XX when root-sh is implemented, uncomment this
      ;; (is (ok? (interp '(action (root-sh "echo 'Great success'")))))
      (is (ok? (interp '(action (clj (fn [& _] (println "Great success in clj")))))))
      (is (ok? (interp '(action (clj "(fn [& _] (println \"Great success in clj\"))")))))
      (is (ok? (interp (list 'action (list 'clj (fn [& _] (println "Great success in clj")))))))
      (is (thrown? Exception (interp (list 'action (list 'clj 1))))))))

(deftest is-dir-test
  (let [ok? #(isa? (class %) ReadablePeriod)]
    (testing "is-dir can be parsed"
      (testing "when it has a ttl specified"
        (is (ok? (interp '(is-dir (dir-ttl (hours 1))))))
        (is (ok? (interp '(is-dir (dir-ttl (hours 20))))))
        (is (ok? (interp '(is-dir (dir-ttl (days 2)))))))
      (testing "when no ttl is specified"
        (is (ok? (interp '(is-dir))))
        (is (= (interp unicron.feed/default-dir-ttl)
               (interp '(is-dir))))))))

(deftest poll-expr-test
  (testing "poll-exprs can be parsed"
    (testing "when they're intervals"
      (let [ok? #(isa? (class %) ReadablePeriod)]
        (is (ok? (interp '(poll-expr (interval (weeks 1))))))
        (is (ok? (interp '(poll-expr (interval (days 1))))))
        (is (ok? (interp '(poll-expr (interval (hours 1))))))
        (is (ok? (interp '(poll-expr (interval (minutes 1))))))
        (is (ok? (interp '(poll-expr (interval (minutes 34))))))
        (is (thrown? Exception (interp '(poll-expr (interval (minutes ))))))
        (is (thrown? Exception (interp '(poll-expr (interval (minutes :a))))))))
    (testing "when they're crons"
      (let [ok? #(every? string? %)]
        (is (ok? (interp '(poll-expr (cron "*/5 2 * * * ? *")))))
        (is (thrown? Exception (interp '(poll-expr (cron )))))
        (is (thrown? Exception (interp '(poll-expr (cron "*/5 20000 * * * ? *")))))
        ;; XX make this test pass
        ;; (is (thrown? Exception (interp '(poll-expr (cron "*/5 2 * * * ? *" "* * * * * ? *"))))) ;; shouldn't parse... that's CRON not CRONS
        (is (ok? (interp '(poll-expr (crons "*/5 2 * * * ? *")))))
        (is (ok? (interp '(poll-expr (crons "*/5 2 * * * ? *" "* * * * * ? *")))))
        (is (ok? (interp '(poll-expr (crons "*/5 2 * * * ? *" "* * * * * ? *" "*/15 10 * * * ? *")))))))))

(deftest filter-test
  (testing "filters can be parsed"
    (let [ok? fn?]
      (is (ok? (interp '(filter (regex "md5sums$")))))
      ;; XX make these tests pass
      ;; (is (thrown? Exception (interp '(filter ))))
      ;; (is (thrown? Exception (interp '(filter (regex "foo") (regex "bar")))))
      ))
  (testing "parsing a filter gives you a predicate"
    (let [in ["a/file1" "a/file2" "a/file3" "a/md5sums"]
          out ["a/md5sums"]]
      (is (= (filter (interp '(filter (regex "md5sums$"))) in)
             out)))))



(deftest high-level-example
  (testing "top-level feeds can be parsed"
    (is (not (nil? (interp '(feed
                             (id "")
                             (conn {})
                             ;; in the date-expr, tz is optional
                             (date-expr "s3://bucket/foo/%Y/%m/%d/bar" "America/Los_Angeles")
                             (starting-after "s3://bucket/foo/2014/01/01/bar")
                             ;; optional "with-file-stream" clause around the action
                             ;; action may be sh or clj
                             (action (sh "echo 'Great success'"))
                             ;; poll-expr may have interval, cron, or crons
                             (poll-expr (interval (days 2)))
                             ;; is-dir clause is optional
                             (is-dir (dir-ttl (hours 1)))
                             ;; any number of filters is allowed (0 or more)
                             (filter (regex "endsWithThis$"))
                             (filter (regex ("^startsWithThis"))))))))))
