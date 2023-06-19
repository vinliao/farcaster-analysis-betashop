(ns utils
  (:gen-class)
  (:require [clj-time.coerce :as c]
            [clojure.data.csv :as csv]
            [clojure.edn :as edn]
            [clojure.java.io :as io]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; timestamp utils
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti get-timestamp-joda (fn [data]
                               (cond
                                 (:users/registered_at data) :user
                                 (:casts/timestamp data) :cast
                                 :else :default)))
(defmethod get-timestamp-joda :user [user]
  (c/from-long (:users/registered_at user)))
(defmethod get-timestamp-joda :cast [cast]
  (c/from-long (:casts/timestamp cast)))
(defmethod get-timestamp-joda :default [data] data)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; csv utils
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn write-csv [data filename]
  (let [header (map name (keys (first data)))]
    (with-open [writer (io/writer filename)]
      (csv/write-csv writer (cons header (map vals data))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; edn utils
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn read-edn [filename]
  (if (.exists (io/file filename))
    (let [file-content (slurp (io/file filename))]
      (if (empty? file-content) [] (edn/read-string file-content)))
    []))

(defn write-edn [filename data]
  (let [existing-data (read-edn filename)
        merged-data (concat existing-data data)]
    (spit filename (prn-str merged-data))))