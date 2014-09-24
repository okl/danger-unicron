(ns unicron.scheduler-test
  {:author "Matt Halverson", :date "Fri Aug 22 16:06:44 PDT 2014"}
  (:require [clojure.test :refer :all]
            [unicron.scheduler :refer :all])
  (:import (org.quartz JobDetail JobKey Trigger TriggerKey)))

(defn no-op [& _] nil)

(deftest ->job-test
  (let [ok? (fn [m]
              (and (isa? (class (:trigger-key m)) TriggerKey)
                   (isa? (class (:trigger m)) Trigger)
                   (isa? (class (:job-key m)) JobKey)
                   (isa? (class (:job m)) JobDetail)))]
    (testing "->job knows how to parse a poll-expr that's"
      (testing "an interval"
        (is (ok? (->job {:id :interval
                         :action no-op
                         :poll-expr (clj-time.core/hours 1)}))))
      (testing "a cron"
        (is (ok? (->job {:id :cron
                         :action no-op
                         :poll-expr (list "/5 * * * * ? *")}))))
      (testing "a bunch of crons"
        (is (every? ok? (->job {:id :crons
                                :action no-op
                                :poll-expr (list "/2 * * * * ? *"
                                                 "/3 * * * * ? *")})))))))

(deftest intervals-fire-immediately
  (testing "intervals fire immediately when scheduled"
    (let [p (promise)
          deliver-fn (fn [& _] (deliver p "signed, sealed, _"))
          j (->job {:id :interval
                     :action deliver-fn
                     :poll-expr (clj-time.core/days 10)})]
      (is (not (realized? p)))
      (do
        (init-scheduler!)
        (start-scheduler!)
        (schedule-jobs [j])
        (pause-scheduler!))
      (is (= "signed, sealed, _" (deref p)))
      (shutdown-scheduler!)
      (is (realized? p)))))
