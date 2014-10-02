(list '(feed
        (id "foobar")
        (conn {:access-key "1234"
               :secret-key "ABCD"})
        (date-expr "s3://bucket/%Y/%m/%d/dev/" "America/Los_Angeles")
        (starting-after "s3://bucket/2014/08/05/dev/")
        (action (clj
                 (fn [uri ts]
                   (println (format "I am action. uri is %s, ts is %s" uri ts)))))
        ;; every 15 min
        (poll-expr (cron "0 15 * * * ? *"))
        (is-dir (dir-ttl (minutes 10)))
        (filter (regex ".+/file/.+"))))
