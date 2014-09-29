(ns unicron.state.jdbc
  "jdbc-based implementations of History (Mysql, Sqlite)"
  {:author "Matt Halverson", :date "Thu Sep 25 13:54:46 PDT 2014"}
  (:require [unicron.state :refer :all]
            [unicron.utils :refer [log-and-throw]])
  (:require [roxxi.utils.print :refer [print-expr]]
            [roxxi.utils.common :refer [def-]]
            [roxxi.utils.collections :refer [project-map]]
            [clojure.tools.logging :as log]
            [clojure.string :refer [join]])
  (:require [clojure.java.jdbc :as j])
  (:import [java.sql BatchUpdateException]
           [com.mchange.v2.c3p0 ComboPooledDataSource]))

;; # Cfg

(def- table :unicron)

(def- field-specs
  [[:event-time :bigint]
   [:date-expr "varchar(500)"]
   [:uri "varchar(500)"]
   [:uri-time :bigint]
   [:state "varchar(20)"]
   [:msg "varchar(500)"]
   [:is-dir :bool]
   [:dir-uri "varchar(500)"]])

(defn- ->column-name [k]  (keyword (clojure.string/replace (name k) \- \_)))
(defn- ->record-field [k] (keyword (clojure.string/replace (name k) \_ \-)))

(def- column-specs
  (map (fn [[field type]] [(->column-name field) type]) field-specs))

;; pardon my Java-ness
(def- OBSERVED "observed")
(def- PROCESSING "processing")
(def- COMPLETED "completed")

;; # Connection pooling

(defn- pool
  "Vanilla configuration straight from the example at
   http://clojure-doc.org/articles/ecosystem/java_jdbc/connection_pooling.html"
  [spec]
  (let [cpds (doto (ComboPooledDataSource.)
               (.setDriverClass (:classname spec))
               (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
               (.setUser (:user spec))
               (.setPassword (:password spec))
               ;; expire excess connections after 30 minutes of inactivity:
               (.setMaxIdleTimeExcessConnections (* 30 60))
               ;; expire connections after 3 hours of inactivity:
               (.setMaxIdleTime (* 3 60 60)))]
    {:datasource cpds}))

;; # Helpers

;; ## DML

(defn- insert! [db event]
  (let [renamed (project-map event :key-xform ->column-name)]
    (j/insert! db table renamed)))

(defn- select [db filter-pairs & {:keys [select-cols
                                         count?
                                         order-by-cols
                                         group-by-cols
                                         having-clause
                                         limit]
                                  :or {select-cols nil
                                       count? false
                                       order-by-cols nil
                                       group-by-cols nil
                                       having-clause nil
                                       limit nil}}]
  (let [where-cols (map (comp ->column-name first) filter-pairs)
        filter-vals (map second filter-pairs)

        select (cond count? "select count(1)"
                     select-cols (format "select " (join ", " (map name select-cols)))
                     :else "select *")
        from (format "from %s" (name table))
        where (join " "
                    (list* (format "where %s = ?" (name (first where-cols)))
                           (map #(format "and %s = ?" (name %))
                                (rest where-cols))))
        group-by (when group-by-cols
                   (format "group by %s" (join ", " (map (comp name ->column-name)
                                                         group-by-cols))))
        having (when having-clause
                 (format "having %s" having-clause))
        order-by (when order-by-cols
                   (let [cols (map (comp name ->column-name first)
                                   order-by-cols)
                         ad (map #(or (second %) "asc")
                                 order-by-cols)
                         cols-ad (map #(str %1 " " %2) cols ad)]
                     (format "order by %s" (join ", " cols-ad))))
        limit (when limit
                (format "limit %s" limit))
        semicolon ";"
        clauses (filter (comp not nil?)
                        (list select
                              from
                              where
                              group-by
                              having
                              order-by
                              limit
                              semicolon))
        q (join " " clauses)
        q-and-params (vec (cons q filter-vals))
        row-fn #(project-map % :key-xform ->record-field)]
    (j/query db q-and-params :row-fn row-fn)))

(defn- extract-count [result-set]
  ;; If you select count(1), you get back a result-set that looks like this:
  ;; [{:count(1) 0}]
  (get (first result-set) (keyword "count(1)")))

;; ## DDL

(defn- db-type [possibly-pooled-db-spec]
  (let [pooled? (isa? (class (:datasource possibly-pooled-db-spec))
                      ComboPooledDataSource)
        jdbc-url (and pooled?
                      (.getJdbcUrl (:datasource possibly-pooled-db-spec)))
        type (cond
              (or (= "mysql" (:subprotocol possibly-pooled-db-spec))
                  (and pooled? (.contains jdbc-url "mysql")))
              :mysql
              (= "sqlite" (:subprotocol possibly-pooled-db-spec))
              :sqlite
              :else
              (log-and-throw
               (format "Don't recognize type of db-spec %s"
                       (dissoc possibly-pooled-db-spec :password))))]
    type))

(defmulti create-table! db-type)
(defmethod create-table! :mysql [db]
  (let [args (concat (list table)
                     column-specs
                     (list :table-spec "ENGINE=InnoDB"))]
    (j/db-do-commands db (apply j/create-table-ddl args))
    (j/db-do-commands db (list (format "CREATE INDEX %s_ix ON %s ( ? )"
                                                   (name table)
                                                   (name table))
                               (->column-name :date-expr)))))
(defmethod create-table! :sqlite [db]
  (let [args (concat (list table)
                     column-specs
                     (list :table-spec ""))]
    (j/db-do-commands db (apply j/create-table-ddl args))))

(defmulti create-table-if-not-exists! db-type)
(defmethod create-table-if-not-exists! :sqlite [db]
  (try
    (create-table! db)
    (catch BatchUpdateException e
      (if (.contains (.getMessage e)
                     (format "table %s already exists" (name table)))
        (log/info "table already existed")
        (throw e)))))
(defmethod create-table-if-not-exists! :mysql [db]
  (try
    (create-table! db)
    (catch BatchUpdateException e
      (if (.contains (.getMessage e)
                     (format "Table '%s' already exists" (name table)))
        (log/info "table already existed")
        (throw e)))))

(defn drop-table! [db]
  (j/db-do-commands db (j/drop-table-ddl table)))

(defmulti drop-table-if-exists! db-type)
(defmethod drop-table-if-exists! :sqlite [db]
  (try
    (drop-table! db)
    (catch BatchUpdateException e
      (if (.contains (.getMessage e)
                     (format "no such table: %s" (name table)))
        (log/info "table didn't exist")
        (throw e)))))
(defmethod drop-table-if-exists! :mysql [db]
  (try
    (drop-table! db)
    (catch BatchUpdateException e
      (if (.contains (.getMessage e)
                     (format "Unknown table '%s'" (name table)))
        (log/info "table didn't exist")
        (throw e)))))

;; # The JDBC implementation

(defrecord JdbcHistory [db]
  History
  ;; Read methods
  (file-has-history? [_ date-expr]
    (pos? (extract-count (select db
                                 [[:date-expr date-expr]
                                  [:is-dir false]]
                                 :count? true))))
  (dir-has-history? [_ date-expr]
    (pos? (extract-count (select db
                                 [[:date-expr date-expr]
                                  [:is-dir true]]
                                 :count? true))))
  (latest-file-match [_ date-expr]
    (first (select db
                   [[:date-expr date-expr]
                    [:is-dir false]]
                   :order-by-cols [[:uri-time "desc"]]
                   :limit 1)))
  (latest-dir-match [_ date-expr]
    (first (select db
                   [[:date-expr date-expr]
                    [:is-dir true]]
                   :order-by-cols [[:uri-time "desc"]]
                   :limit 1)))
  (live-directories [_ date-expr]
    (select db
            [[:date-expr date-expr]
             [:is-dir true]]
            ;; :select-cols [:dir-uri]
            :group-by-cols [:uri]
            :having-clause "count(distinct state) = 1"))
  (matches-in-directory [_ date-expr directory-uri]
    (select db
            [[:date-expr date-expr]
             [:is-dir false]
             [:dir-uri directory-uri]
             [:state OBSERVED]]))

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
  (observed-file!        [_ observed-at date-expr uri uri-time msg]
    (insert! db (make-file-event observed-at date-expr uri
                                 uri-time OBSERVED msg)))
  (observed-dir!         [_ observed-at date-expr uri uri-time msg]
    (insert! db (make-dir-event observed-at date-expr uri
                                uri-time OBSERVED msg)))
  (observed-file-in-dir! [_ observed-at date-expr uri uri-time msg dir-uri]
    (insert! db (make-file-in-dir-event observed-at date-expr uri
                                        uri-time OBSERVED msg dir-uri)))
  (processing-file!        [_ started-at date-expr uri uri-time msg]
    (insert! db (make-file-event started-at date-expr uri
                                 uri-time PROCESSING msg)))
  (processing-file-in-dir! [_ started-at date-expr uri uri-time msg dir-uri]
    (insert! db (make-file-in-dir-event started-at date-expr uri
                                        uri-time PROCESSING msg dir-uri)))
  (completed-file!        [_ completed-at date-expr uri uri-time msg]
    (insert! db (make-file-event completed-at date-expr uri
                                 uri-time COMPLETED msg)))
  (completed-dir!         [_ completed-at date-expr uri uri-time msg]
    (insert! db (make-dir-event completed-at date-expr uri
                                uri-time COMPLETED msg)))
  (completed-file-in-dir! [_ completed-at date-expr uri uri-time msg dir-uri]
    (insert! db (make-file-in-dir-event completed-at date-expr uri
                                        uri-time COMPLETED msg dir-uri))))

;; # Makers

(defn make-mysql-history [db-spec & {:keys [blast-away-history?
                                            pooled?]
                                     :or   {blast-away-history? false
                                            pooled? true}}]
  (let [db-spec (if pooled? (pool db-spec) db-spec)]
    (when blast-away-history?
      (drop-table-if-exists! db-spec))
    (create-table-if-not-exists! db-spec)
    (->JdbcHistory db-spec)))

(defn make-sqlite-history [db-spec & {:keys [blast-away-history?]
                                      :or   {blast-away-history? false}}]
  (when blast-away-history?
    (drop-table-if-exists! db-spec))
  (create-table-if-not-exists! db-spec)
  (->JdbcHistory db-spec))
