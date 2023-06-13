(ns writer
  (:gen-class)
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [next.jdbc :as jdbc]))

(def table-infos {:locations {:name "locations"
                              :primary-key "id"}
                  :users {:name "users"
                          :primary-key "fid"}
                  :casts {:name "casts"
                          :primary-key
                          "hash"}})

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

(defn read-data [filename]
  (if (.exists (io/file filename))
    (let [file-content (slurp (io/file filename))]
      (if (empty? file-content) [] (edn/read-string file-content)))
    []))

(defn write-data [filename data]
  (let [existing-data (read-data filename)
        merged-data (concat existing-data data)]
    (spit filename (prn-str merged-data))))

(defn get-filename [table-info]
  (str "data/" (:name table-info) ".edn"))

(defn get-pk-keyword [table-info]
  (keyword (str (:name table-info) "/" (:primary-key table-info))))

(defn fetch-and-save
  ([table-info] ;; first call
   (let [pk-keyword (get-pk-keyword table-info)
         filename (get-filename table-info)
         data (read-data filename)
         last-id (if (seq data)
                   (pk-keyword (last data))
                   nil)]
     (fetch-and-save table-info last-id)))
  ([table-info last-id] ;; recursive call
   (let [query (build-query table-info last-id)
         data (fetch query)
         filename (get-filename table-info)
         pk-keyword (get-pk-keyword table-info)]
     (println (str "fetched " (count data) " " (:name table-info) " " last-id))
     (write-data filename data)
     (when (seq data)
       (fetch-and-save table-info (pk-keyword (last data)))))))

;; (take 16 (reverse (read-data "data/locations.edn")))