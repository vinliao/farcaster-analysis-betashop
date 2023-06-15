(ns writer
  (:gen-class)
  (:require [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [next.jdbc :as jdbc]
            [utils :as u]))

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
       (h/limit 10000)
       (sql/format))))

(defn fetch [query]
  (let [db (read-string (slurp "env.edn"))
        ds (jdbc/get-datasource db)]
    (jdbc/with-transaction [conn ds]
      (jdbc/execute! conn query))))

(defn get-filename [table-info]
  (str "data/" (:name table-info) ".edn"))

(defn get-pk-keyword [table-info]
  (keyword (str (:name table-info) "/" (:primary-key table-info))))

(defn fetch-and-save
  ([table-info] ;; first call
   (let [pk-keyword (get-pk-keyword table-info)
         filename (get-filename table-info)
         data (u/read-edn filename)
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
     (u/write-edn filename data)
     (when (seq data)
       (fetch-and-save table-info (pk-keyword (last data)))))))

;; (fetch-and-save (table-infos :users))