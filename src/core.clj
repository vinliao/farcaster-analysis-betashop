(ns core
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [writer :as w]))

(w/fetch-and-save (:locations w/table-infos))

(defn read-csv [filename]
  (with-open [reader (io/reader filename)]
    (let [rows (csv/read-csv reader)
          headers (map keyword (first rows))
          data (rest rows)]
      (doall (map (fn [row]
                    (zipmap headers row))
                  data)))))

(def data (read-csv "data/locations.csv"))
(take 10 (shuffle data))
