(ns writer
  (:gen-class)
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [next.jdbc :as jdbc]))

(defn make-base-query [table-info]
  {:select [:*]
   :from [(keyword (:name table-info))]
   :order-by [[(keyword (:primary-key table-info)) :asc]]})

(defn build-query
  ([table-info]
   (build-query table-info nil))
  ([table-info last-id]
   (-> (make-base-query table-info)
       (cond-> last-id
         (h/where [:> (keyword (:primary-key table-info)) last-id]))
       (h/limit 5)
       (sql/format))))

(defn fetch [query]
  (let [db (read-string (slurp "env.edn"))
        ds (jdbc/get-datasource db)]
    (jdbc/with-transaction [conn ds]
      (jdbc/execute! conn query))))

(def table-infos {:locations {:name "locations"
                              :primary-key "id"}
                  :users {:name "users"
                          :primary-key "fid"}
                  :casts {:name "casts"
                          :primary-key
                          "hash"}})

(defn write-to-csv
  [filename data headers]
  (if (.exists (io/file filename))
    (with-open [writer (io/writer filename :append true)]
      (csv/write-csv writer (map vals data)))
    (with-open [writer (io/writer filename)]
      (csv/write-csv writer (conj (map vals data) headers)))))

(defn fetch-and-save
  ([table-info]
   (fetch-and-save table-info nil))
  ([table-info last-id]
   (let [query (build-query table-info last-id)
         data (fetch query)
         filename (str (:name table-info) ".csv")
         headers (if (not-empty data) (keys (first data)) nil)
         namespaced-key (keyword (:name table-info) (:primary-key table-info))]
     (println (str "fetched " (count data) (:name table-info) " " last-id))
     (write-to-csv filename data headers)
     (when (seq data)
       (fetch-and-save table-info (get (last data) namespaced-key))))))

;; (fetch-and-save (:locations table-infos))