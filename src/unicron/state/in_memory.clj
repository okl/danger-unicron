(ns unicron.state.in-memory
  "In-memory implementation of History"
  {:author "Matt Halverson", :date "Mon Sep  8 16:43:36 PDT 2014"}
  (:require [unicron.state :refer :all])
  (:require [roxxi.utils.print :refer [print-expr]]
            [clojure.tools.logging :as log]))

;; # Helpers

(defn- add-event [atom-vec event]
  (swap! atom-vec conj event))

(defn- generic-matches [events field val]
  (filter (fn [e] (= val (field e))) events))
(defn- date-expr-matches [events date-expr]
  (generic-matches events :date-expr date-expr))

(defn- sort-events-by-uri-time [event-seq]
  (sort #(compare (:uri-time %1) (:uri-time %2))
        event-seq))

;; # The RAM implementation

(defrecord InMemoryHistory [atom-vec]
  History
  ;; Read methods
  (file-has-history? [_ date-expr]
    (not (empty? (-> @atom-vec
                     (generic-matches :date-expr date-expr)
                     (generic-matches :is-dir false)))))
  (dir-has-history? [_ date-expr]
    (not (empty? (-> @atom-vec
                     (generic-matches :date-expr date-expr)
                     (generic-matches :is-dir true)))))
  (latest-file-match [_ date-expr]
    (-> @atom-vec
        (generic-matches :date-expr date-expr)
        (generic-matches :is-dir false)
        (sort-events-by-uri-time)
        (last)))
  (latest-dir-match [_ date-expr]
    (-> @atom-vec
        (generic-matches :date-expr date-expr)
        (generic-matches :is-dir true)
        (sort-events-by-uri-time)
        (last)))
  (live-directories [_ date-expr]
    (let [matching-dirs (-> @atom-vec
                            (generic-matches :date-expr date-expr)
                            (generic-matches :is-dir true))
          dirs-with-starts (generic-matches matching-dirs :state :observed)
          uris-with-starts (into #{} (map :uri dirs-with-starts))
          dirs-with-ends (generic-matches matching-dirs :state :completed)
          uris-with-ends (into #{} (map :uri dirs-with-ends))
          live-uris (clojure.set/difference uris-with-starts uris-with-ends)
          live-dirs (filter #(contains? live-uris (:uri %)) dirs-with-starts)]
      ;; (print-expr uris-with-starts)
      ;; (print-expr uris-with-ends)
      ;; (print-expr live-dirs)
      live-dirs))
  (matches-in-directory [_ date-expr directory-uri]
    (-> @atom-vec
        (generic-matches :date-expr date-expr)
        (generic-matches :is-dir false)
        (generic-matches :dir-uri directory-uri)
        (generic-matches :state :observed)))
  ;; Write methods
  (expire-old-directories! [h date-expr moment-of-death]
    (let [to-be-reaped (filter #(< (:event-time %) moment-of-death)
                               (live-directories h date-expr))]
      (doseq [e to-be-reaped]
        (log/infof "Expired old directory: uri was %s" (:uri e))
        (completed-dir! h
                        (now)
                        (:date-expr e)
                        (:uri e)
                        (:uri-time e)
                        (format "collected by the reaper; moment-of-death was %s" moment-of-death)))))
  (observed-file! [_ observed-at date-expr uri uri-time msg]
    (add-event atom-vec (make-file-event observed-at date-expr uri
                                         uri-time :observed msg)))
  (observed-dir! [_ observed-at date-expr uri uri-time msg]
    (add-event atom-vec (make-dir-event observed-at date-expr uri
                                        uri-time :observed msg)))
  (observed-file-in-dir! [_ observed-at date-expr uri uri-time msg dir-uri]
    (add-event atom-vec (make-file-in-dir-event observed-at date-expr uri
                                                uri-time :observed msg
                                                dir-uri)))
  (processing-file! [_ started-at date-expr uri uri-time msg]
    (add-event atom-vec (make-file-event started-at date-expr uri
                                         uri-time :processing msg)))
  (processing-file-in-dir! [_ started-at date-expr uri uri-time msg dir-uri]
    (add-event atom-vec (make-file-in-dir-event started-at date-expr uri
                                                uri-time :processing msg
                                                dir-uri)))
  (completed-file! [_ completed-at date-expr uri uri-time msg]
    (add-event atom-vec (make-file-event completed-at date-expr uri
                                         uri-time :completed msg)))
  (completed-dir! [_ completed-at date-expr uri uri-time msg]
    (add-event atom-vec (make-dir-event completed-at date-expr uri
                                        uri-time :completed msg)))
  (completed-file-in-dir! [_ completed-at date-expr uri uri-time msg dir-uri]
    (add-event atom-vec (make-file-in-dir-event completed-at date-expr uri
                                                uri-time :completed msg
                                                dir-uri))))

(defn make-in-memory-history []
  (->InMemoryHistory (atom [])))
