(ns core
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [writer :as w]))

(:users w/table-infos)

;; (defn read-csv [filename]
;;   (with-open [reader (io/reader filename)]
;;     (doall
;;      (map (fn [row]
;;             (zipmap [:locations/id :locations/description] row))
;;           (csv/read-csv reader)))))

(defn read-csv [filename]
  (with-open [reader (io/reader filename)]
  (->>
    (csv/read-csv reader)
    (map (fn [row]
           (zipmap [:locations/id :locations/description] row))))))

(read-csv "locations.csv")

;; (def reader (io/reader "locations.csv"))
;; (def data (csv/read-csv reader))