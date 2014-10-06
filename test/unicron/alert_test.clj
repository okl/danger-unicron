(ns unicron.alert-test
  "Testing that unicron.alert/email is invoked when we expect it to be"
  {:author "Matt Halverson", :date "Thu Oct  2 17:40:49 PDT 2014"}
  (:require [clojure.test :refer :all]
            [unicron.alert :refer :all]
            [unicron.action-test :refer [fresh-action
                                         with-remote-uris
                                         event-seq]]
            [roxxi.utils.print :refer [print-expr]]))

;; # Helpers

(defn new-counter [] (atom 0))
(defmacro with-email-stubbed
  "This takes an atom-integer and, instead of invoking the email alerting
  mechanism, increments the counter."
  [atom-counter & exprs]
  `(with-redefs
     [unicron.alert/email (fn [& rest#]
                            (swap! ~atom-counter inc))]
     ~@exprs))

(defn- fresh-shell-action [sh-code]
  (fresh-action (list 'action
                      (list 'sh
                            sh-code))))

(defn- fresh-clj-action [fxn]
  (fresh-action
   (list 'action
         (list 'clj
               fxn))))

;; # Tests

(deftest sh-action-test
  (testing "For shell actions, do they"
    (testing "email for non-zero exit codes"
      (let [c (new-counter)
            a (fresh-shell-action "exit 1")]
        (with-email-stubbed c
          (with-remote-uris (take 1 event-seq)
            (is (= 0 @c))
            (a)
            (is (= 1 @c))
            (a)
            (is (= 1 @c))))
        (with-email-stubbed c
          (with-remote-uris (take 3 event-seq)
            (is (= 1 @c))
            (a)
            (is (= 3 @c))
            (a)
            (is (= 3 @c))))))
    (testing "pass for exit codes that are zero"
      (let [c (new-counter)
            a (fresh-shell-action "exit 0")]
        (with-email-stubbed c
          (with-remote-uris event-seq
            (is (= 0 @c))
            (a)
            (is (= 0 @c))
            (a)
            (is (= 0 @c)))))
      (testing "even when there is output to stdout"
        (let [c (new-counter)
              a (fresh-shell-action "echo 'to stdout'; exit 0")]
          (with-email-stubbed c
            (with-remote-uris event-seq
              (is (= 0 @c))
              (a)
              (is (= 0 @c))
              (a)
              (is (= 0 @c))))))
      (testing "even when there is output to stderr"
        (let [c (new-counter)
              a (fresh-shell-action ">&2 echo 'to stderr'; exit 0")]
          (with-email-stubbed c
            (with-remote-uris event-seq
              (is (= 0 @c))
              (a)
              (is (= 0 @c))
              (a)
              (is (= 0 @c)))))))))

(deftest clj-action-test
  (testing "For clojure actions, do they"
    (testing "email if exceptions are thrown"
      (let [c (new-counter)
            a (fresh-clj-action
               (fn [& _]
                 (throw
                  (RuntimeException. "oh noes, a RuntimeException!"))))]
        (with-email-stubbed c
          (with-remote-uris (take 1 event-seq)
            (is (= 0 @c))
            (a)
            (is (= 1 @c))
            (a)
            (is (= 1 @c))))
        (with-email-stubbed c
          (with-remote-uris (take 3 event-seq)
            (is (= 1 @c))
            (a)
            (is (= 3 @c))
            (a)
            (is (= 3 @c))))))
    (testing "pass if no exceptions are thrown"
      (let [c (new-counter)
            a (fresh-clj-action (fn [& _] (+ 1 1)))]
        (with-email-stubbed c
          (with-remote-uris (take 1 event-seq)
            (is (= 0 @c))
            (a)
            (is (= 0 @c))
            (a)
            (is (= 0 @c)))))
      (testing "even when there is output to stdout"
        (let [c (new-counter)
              a (fresh-clj-action (fn [& _] (println "writing to stdout")))]
          (with-email-stubbed c
            (with-remote-uris (take 1 event-seq)
              (is (= 0 @c))
              (a)
              (is (= 0 @c))
              (a)
              (is (= 0 @c)))))))))
