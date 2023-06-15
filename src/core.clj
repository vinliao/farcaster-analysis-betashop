(ns core
  (:gen-class)
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [utils :as u]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; data init
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def users (u/read-edn "data/users.edn"))
(def casts (u/read-edn "data/casts.edn"))
(defn group-casts-by-fid [casts]
  (group-by :casts/author_fid casts))
(def casts-by-fid (group-casts-by-fid casts))

(def start-timestamp (->> users
                          (sort-by :users/registered_at)
                          first
                          u/get-timestamp-joda))

(def end-timestamp (->> casts
                        (sort-by :casts/timestamp)
                        last
                        u/get-timestamp-joda))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; first week cast count, casted last week, register timestamp
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn get-first-week-casts [user]
  (let [casts-by-user (get casts-by-fid (:users/fid user))
        one-week-later (t/plus (u/get-timestamp-joda user) (t/weeks 1))]
    (filter (fn [cast]
              (and (t/after? (u/get-timestamp-joda cast) (u/get-timestamp-joda user))
                   (t/before? (u/get-timestamp-joda cast) one-week-later)))
            casts-by-user)))

(defn user-cast-last-week? [user end-timestamp]
  (let [casts-by-user (get casts-by-fid (:users/fid user))
        one-week-ago (t/minus end-timestamp (t/weeks 1))]
    (or
     (some #(and (t/after? (u/get-timestamp-joda %) one-week-ago)
                 (t/before? (u/get-timestamp-joda %) end-timestamp))
           casts-by-user)
     false)))

(defn get-user-cast-data [users end-timestamp]
  (map (fn [user]
         {:fid (:users/fid user)
          :username (:users/username user)
          :registered-at (u/get-timestamp-joda user)
          :first-week-cast-count (count (get-first-week-casts user))
          :casted-last-week? (user-cast-last-week? user end-timestamp)})
       users))

(def processed-users (get-user-cast-data users end-timestamp))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; frequency matrix
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn week-of [timestamp]
  (let [timestamp-joda (u/get-timestamp-joda timestamp)]
    (if (.isEqual start-timestamp timestamp-joda)
      0
      (-> (t/interval start-timestamp timestamp-joda)
          (.toDuration)
          (.getStandardDays)
          (/ 7)
          int))))

(defn week-to-string [week-num]
  (let [date-after-n-weeks (t/plus start-timestamp (t/weeks week-num))]
    (f/unparse (f/formatter "MMMM d, yyyy") date-after-n-weeks)))

(defn classify-casts [user]
  (let [count (:first-week-cast-count user)]
    (cond
      (zero? count) :cast-0
      (= 1 count) :cast-1
      (>= count 100) :cast-100+
      (>= count 50) :cast-50+
      (>= count 25) :cast-25+
      (>= count 10) :cast-10+
      (>= count 2) :cast-2+)))

(defn users-to-table [users]
  (->> users
       (group-by (comp week-of :registered-at))
       (map (fn [[week users-in-week]]
              {:week week
               :num-signups (count users-in-week)
               :cast-frequencies (->> users-in-week
                                      (map classify-casts)
                                      frequencies)}))
       (sort-by :week)
       reverse
       (map (fn [row] (assoc row :week (week-to-string (:week row)))))))

;; (def frequency-matrix (users-to-table processed-users))
(def frequency-matrix (u/read-edn "data/frequency-matrix.edn"))

(defn format-frequency-matrix-row [data]
  [(or (:week data) "")
   (or (:num-signups data) 0)
   (or (get (:cast-frequencies data) :cast-0) 0)
   (or (get (:cast-frequencies data) :cast-1) 0)
   (or (get (:cast-frequencies data) :cast-2+) 0)
   (or (get (:cast-frequencies data) :cast-10+) 0)
   (or (get (:cast-frequencies data) :cast-25+) 0)
   (or (get (:cast-frequencies data) :cast-50+) 0)
   (or (get (:cast-frequencies data) :cast-100+) 0)])

(defn format-frequency-matrix-csv [data]
  (map format-frequency-matrix-row data))

(defn make-frequency-matrix-csv [data file]
  (with-open [writer (io/writer file)]
    (csv/write-csv writer (cons ["Signup Week" "Number Signups" "Casted 0 Times" "=1x" ">2x" ">10x" ">25x" ">50x" ">100x"] (format-frequency-matrix-csv data)))))

(make-frequency-matrix-csv frequency-matrix "data/frequency-matrix.csv")
