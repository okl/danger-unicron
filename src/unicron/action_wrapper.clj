(ns unicron.action-wrapper
  "Wrap user-defined actions with retry-policies, state management, etc."
  {:author "Matt Halverson", :date "Tue Sep  9 08:21:51 PDT 2014"}
  (:require [roxxi.utils.common :refer [def-]]
            [roxxi.utils.print :refer [print-expr]]
            [clojure.tools.logging :as log]
            [clojure.stacktrace :refer [print-stack-trace]]
            [date-expr.core :as de]
            [dwd.core :as dwd]
            [dwd.check-result :as cr]
            [unicron.utils :refer [log-and-throw]]
            [unicron.connection :refer [interp-conn]]
            [unicron.state :as st]
            [unicron.alert :as alert]))

;; # Helper code

(defn- get-path*ts-pairs [check-result date-expr-pattern-str tz]
  (let [file-listing (cr/result check-result)
        d (de/make-date-expr date-expr-pattern-str tz)
        ;; We need to know the length that the formatted date-exprs will be
        ;; because, when working with directories, we need to truncate
        ;; file-in-dir uris down to just the dir-prefix, so that we can
        ;; parse the path and compute the ts.
        len (count (de/format-expr d (:seconds (de/epoch-time-now))))]
    (when (:truncated? file-listing)
      (log-and-throw (format
                      (str "File-listing was truncated, but unicron doesn't "
                           "know how to keep asking for more listings yet. "
                           "Check-result was: %s")
                      check-result)))
    (map (fn [path] {:uri path, :ts (de/parse-expr d (subs path 0 len))})
         (:paths file-listing))))

(defn list-files-matching-prefix [date-expr tz prefix conn-info]
  (log/infof "Prefix is %s" prefix)
  (let [form `(with-s3
                ~conn-info
                (list-files-matching-prefix ~prefix))
        check-result (dwd/exec-interp-namespaced form {})]
    (get-path*ts-pairs check-result date-expr tz)))

(defn list-files-newer-than [date-expr tz newer-than-this conn-info]
  (let [form `(with-s3
                ~conn-info
                (list-files-newer-than ~date-expr
                                       ~tz
                                       ~newer-than-this))
        check-result (dwd/exec-interp-namespaced form {})]
    (get-path*ts-pairs check-result date-expr tz)))

(defn- new-files-in-old-dir [h date-expr tz conn-info dir-observed-event]
  (let [dir-uri (:uri dir-observed-event)
        all-matches (list-files-matching-prefix date-expr tz dir-uri conn-info)
        all-matches-with-dir (map #(assoc % :dir-uri dir-uri) all-matches)
        already-seen (st/matches-in-directory h date-expr dir-uri)
        already-seen-uris (into #{} (map :uri already-seen))]
    (filter #(not (contains? already-seen-uris (:uri %))) all-matches-with-dir)))
(defn- new-files-in-old-dirs [h date-expr tz conn-info]
  (let [old-dirs (st/live-directories h date-expr)]
    (mapcat #(new-files-in-old-dir h date-expr tz conn-info %) old-dirs)))

(defn- new-files-in-new-dirs [date-expr tz starting-after conn-info]
  (let [new-stuff (list-files-newer-than date-expr tz starting-after conn-info)
        de (de/make-date-expr date-expr tz)
        dir-uri-extractor (fn [f] (de/format-expr de (:ts f)))
        add-dir-uri (fn [f] (assoc f :dir-uri (dir-uri-extractor f)))]
    (map add-dir-uri new-stuff)))

(defn- distill-and-log-new-dirs! [h new-files-in-new-dirs date-expr-pattern-str date-expr-tz]
  ;; For the new-dirs, get the unique timestamps + dir-uris and log
  ;; them as new-dirs, before processing the individual files.
  (let [uniq-times (into #{} (map :ts new-files-in-new-dirs))
        date-expr (de/make-date-expr date-expr-pattern-str date-expr-tz)]
    (doseq [ts uniq-times]
      (let [dir-uri (de/format-expr date-expr ts)]
        (log/infof "Observed new dir for %s: dir is %s"
                   date-expr-pattern-str
                   dir-uri)
        (st/observed-dir! h
                          (st/now)
                          date-expr-pattern-str
                          (de/format-expr date-expr ts)
                          ts
                          nil)))))

;; # The money

(defn- parse-and-apply! [h action-fn date-expr x & [dir-uri]]
  (let [uri (:uri x)
        ts (:ts x)]
    (log/infof "Observed new file for %s: file is %s" date-expr uri)
    (if dir-uri
      (st/observed-file-in-dir! h (st/now) date-expr uri ts nil dir-uri)
      (st/observed-file! h (st/now) date-expr uri ts nil))
    (try
      (log/infof "Processing new file for %s: file is %s" date-expr uri)
      (if dir-uri
        (st/processing-file-in-dir! h (st/now) date-expr uri ts nil dir-uri)
        (st/processing-file! h (st/now) date-expr uri ts nil))
      (action-fn uri ts)
      (if dir-uri
        (st/completed-file-in-dir! h (st/now) date-expr uri ts "success" dir-uri)
        (st/completed-file! h (st/now) date-expr uri ts "success"))
      (catch Exception e
        (log/error (.getMessage e))
        (if dir-uri
          (st/completed-file-in-dir! h (st/now) date-expr uri ts "error" dir-uri)
          (st/completed-file! h (st/now) date-expr uri ts "error"))
        (let [ret (alert/email "Unicron Feed error"
                               (str "Caught exception running action:\n"
                                    (with-out-str (print-stack-trace e))))]
          ;; ret looks like this:
          ;;   {:code 0, :error :SUCCESS, :message "message sent"}
          (when (not= 0 (:code ret))
            (log/error "Oh noes! Sending the alert email failed! D:"))))
      (finally
        (log/infof "Completed new file for %s: file was %s" date-expr uri)))))

(defn- p [x]
  (clojure.string/trim (with-out-str (clojure.pprint/pprint x))))
(defn process-dirs
  [h action-fn date-expr date-expr-tz starting-after filter-fn conn-info dir-ttl]
  (let [first-time? (not (st/dir-has-history? h date-expr))
        new-files-in-old-dirs (if first-time?
                                (list)
                                (new-files-in-old-dirs h date-expr date-expr-tz conn-info))
        ignore (log/infof "new-files-in-old-dirs is %s" (p new-files-in-old-dirs))
        ignore (log/infof "starting-after from configuration  is %s" starting-after)
        starting-after (if first-time?
                        starting-after
                        (:uri (st/latest-dir-match h date-expr)))
        ignore (log/infof "starting-after adding history info is %s" starting-after)
        new-files-in-new-dirs (new-files-in-new-dirs date-expr
                                                     date-expr-tz
                                                     starting-after
                                                     conn-info)
        ignore (log/infof "new-files-in-new-dirs is %s" (p new-files-in-new-dirs))
        new-files (concat new-files-in-old-dirs new-files-in-new-dirs)
        filtered (filter #(filter-fn (:uri %)) new-files)
        moment-of-death (st/moment-of-death dir-ttl)]
    (if first-time?
      (log/infof (str "Processing %s... no matches previously found! Was looking for "
                      "matches starting after %s. Matches were: %s")
                 date-expr starting-after (p filtered))
      (log/infof "Processing %s, which has had matches in the past... new matches are %s"
                 date-expr (p filtered)))
    (distill-and-log-new-dirs! h new-files-in-new-dirs date-expr date-expr-tz)
    (doseq [f filtered]
      (parse-and-apply! h action-fn date-expr f (:dir-uri f)))
    (log/infof (str "Checking for directories that have expired for %s... "
                    "dir-ttl is %s, moment-of-death is %s")
               date-expr
               dir-ttl
               moment-of-death)
    (st/expire-old-directories! h date-expr moment-of-death)))

(defn process-files
  [h action-fn date-expr date-expr-tz starting-after filter-fn conn-info]
  (let [first-time? (not (st/file-has-history? h date-expr))
        ignore (log/infof "starting-after from configuration  is %s" starting-after)
        starting-after (if first-time?
                        starting-after
                        (st/latest-file-match h date-expr))
        ignore (log/infof "starting-after adding history info is %s" starting-after)
        new-files (list-files-newer-than date-expr
                                         date-expr-tz
                                         starting-after
                                         conn-info)
        filtered (filter #(filter-fn (:uri %)) new-files)]
    (if first-time?
      (log/infof (str "Processing %s... no matches previously found! Was looking for "
                      "matches against %s. Matches were: %s")
                 date-expr starting-after (p filtered))
      (log/infof "Processing %s, which has had matches... new matches are %s"
                 date-expr (p filtered)))
    (doseq [f filtered]
      (parse-and-apply! h action-fn date-expr f))))
