(ns unicron.feed-test
  (:require [clojure.test :refer :all]
            [unicron.feed :refer :all]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; test cases -- to formalize

(interp-feed '(date-expr )) ;; XXX should throw but doesn't!
(interp-feed '(date-expr "s3://bucket/foo/%Y/%m/%d/bar"))

((interp-feed '(action (sh "echo 'Great success'"))))
((interp-feed '(action (sh "echo 'Great success' > /dev/null"))))
((interp-feed '(action (sh "ps -ef | grep grep"))))
((interp-feed '(action (root-sh "echo 'Great success'"))))
((interp-feed '(action (clj (println "Great success")))))
((interp-feed '(action (clj "(println \"Great success\")"))))
((interp-feed (list 'action (list 'clj #(println "Great success")))))
((interp-feed (list 'action (list 'clj 1))))

(interp-feed '(poll-expr (interval (months 1))))
(interp-feed '(poll-expr (interval (weeks 1))))
(interp-feed '(poll-expr (interval (days 1))))
(interp-feed '(poll-expr (interval (hours 1))))
(interp-feed '(poll-expr (interval (minutes 1))))
(interp-feed '(poll-expr (interval (minutes 34))))
(interp-feed '(poll-expr (interval (minutes ))))
(interp-feed '(poll-expr (interval (minutes :a))))
(interp-feed '(poll-expr (cron ))) ;; XXX shouldn't parse...
(interp-feed '(poll-expr (cron "*/5 2 * * *")))
(interp-feed '(poll-expr (cron "*/5 2 * * *" "* * * * *"))) ;; XXX shouldn't parse...
(interp-feed '(poll-expr (crons "*/5 2 * * *")))
(interp-feed '(poll-expr (crons "*/5 2 * * *" "* * * * *")))
(interp-feed '(poll-expr (crons "*/5 2 * * *" "* * * * *" "*/15 10 * * *")))

(interp-feed '(filter )) ;; XXX shouldn't parse...
(interp-feed '(filter (regex "md5sums$")))
(filter (interp-feed '(filter (regex "md5sums$"))) ["a/file1" "a/file2" "a/file3" "a/md5sums"])

(interp-feed '(feed
               (date-expr "s3://bucket/foo/%Y/%m/%d/bar")
               (action (sh "echo 'Great success'"))))
(interp-feed '(feed
               (date-expr "s3://bucket/foo/%Y/%m/%d/bar")
               (action (sh "echo 'Great success'"))
               (poll-expr (interval (months 1)))))
(interp-feed '(feed
               (date-expr "s3://bucket/foo/%Y/%m/%d/bar")
               (action (sh "echo 'Great success'"))
               (poll-expr (interval (months 1)))
               (poll-expr (interval (months 2))))) ;; XXX clean up the error msg
(interp-feed '(feed
               (date-expr "s3://bucket/foo/%Y/%m/%d/bar")
               (action (sh "echo 'Great success'"))
               (filter (regex "foo"))))
(interp-feed '(feed
               (date-expr "s3://bucket/foo/%Y/%m/%d/bar")
               (action (sh "echo 'Great success'"))
               (filter (regex "foo"))
               (filter (regex "bar")))) ;; clean up the error msg
(interp-feed '(feed
               (date-expr "s3://bucket/foo/%Y/%m/%d/bar")
               (action (sh "echo 'Great success'"))
               (filter (regex "foo")
                       (regex "bar")))) ;; XX shouldn't parse...
