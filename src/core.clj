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

;; last indexed cast in the dataset
;; #clj-time/date-time "2023-06-13T05:54:57.000Z"
(def end-timestamp (->> casts
                        (sort-by :casts/timestamp)
                        last
                        u/get-timestamp-joda))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; user activity data
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn user-cast-last-week? [user end-timestamp]
  (let [casts-by-user (get casts-by-fid (:users/fid user))
        one-week-ago (t/minus end-timestamp (t/weeks 1))]
    (or
     (some #(and (t/after? (u/get-timestamp-joda %) one-week-ago)
                 (t/before? (u/get-timestamp-joda %) end-timestamp))
           casts-by-user)
     false)))

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

(defn classify-cast-count [user]
  (let [count (:total-cast-count user)]
    (cond-> []
      (>= count 1) (conj :cast-1+)
      (>= count 2) (conj :cast-2+)
      (>= count 5) (conj :cast-5+)
      (>= count 10) (conj :cast-10+)
      (>= count 25) (conj :cast-25+)
      (>= count 50) (conj :cast-50+)
      (>= count 100) (conj :cast-100+)
      (zero? count) (conj :cast-0))))

(defn get-users-activity [users]
  (map (fn [user]
         {:fid (:users/fid user)
          :username (:users/username user)
          :registered-at (u/get-timestamp-joda user)
          :registered-at-week (week-of user)
          :total-cast-count (count (casts-by-fid (:users/fid user)))
          :casted-last-week? (user-cast-last-week? user end-timestamp)})
       users))

(defn group-activity-by-registration-week [users]
  (->> users
       (group-by (comp week-of :registered-at))
       (map (fn [[week users-in-week]]
              {:week week
               :num-signups (count users-in-week)
               :cast-frequencies (->> users-in-week
                                      (mapcat classify-cast-count)
                                      frequencies)}))
       (sort-by :week)
       reverse
       (map (fn [row] (assoc row :week (:week row))))))

(def users-activity (get-users-activity users))
;; what users-activity look like
#_(nth users-activity 1)
#_{:fid 2,
   :username "v",
   :registered-at #clj-time/date-time "2021-05-10T19:59:25.000Z",
   :registered-at-week 0,
   :total-cast-count 4149,
   :casted-last-week? true}

(def activity-by-registration-week (group-activity-by-registration-week users-activity))
;; what activity-by-registration-week look like
#_(first activity-by-registration-week)
#_{:week 108,
   :num-signups 234,
   :cast-frequencies {:cast-0 98,
                      :cast-1+ 136,
                      :cast-2+ 92,
                      :cast-5+ 47,
                      :cast-10+ 16,
                      :cast-25+ 2}}
;; explanation: on week 108 (since the first ever registered user), there are 234 signups
;; 98 have not casted, 136 have casted at least once, 92 have casted at least twice, etc.

(defn get-users-of-week [week]
  (->> users-activity
       (filter #(= (:registered-at-week %) week))))

;; TODO: numbers on the two lines below look good, but checks should be done by asserts
;; (get-users-of-week 35)
;; (first (filter #(= (:week %) 35) activity-by-registration-week))

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ;; casted a lot and still casted last week?
;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; (defn filter-count-and-cast-last-week [users cast-count key]
;;   (->> users
;;        (filter #(and (>= (:total-cast-count %) cast-count)
;;                      (:casted-last-week? %)))
;;        (group-by #(week-of (:registered-at %)))
;;        (map (fn [[week users-in-week]]
;;               {(week-to-string week) {key (count users-in-week)}}))))

;; (def count-and-last-week-map
;;   (reduce (fn [accum new-data]
;;             (merge-with merge accum new-data))
;;           (concat (filter-count-and-cast-last-week user-all-casts-data 25 :users-casted-25+-and-casted-last-week)
;;                   (filter-count-and-cast-last-week user-all-casts-data 50 :users-casted-50+-and-casted-last-week)
;;                   (filter-count-and-cast-last-week user-all-casts-data 100 :users-casted-100+-and-casted-last-week))))

;; (defn format-row [[week data]]
;;   [week
;;    (or (:users-casted-25+-and-casted-last-week data) 0)
;;    (or (:users-casted-50+-and-casted-last-week data) 0)
;;    (or (:users-casted-100+-and-casted-last-week data) 0)])

;; (defn format-rows [data]
;;   (map format-row data))

;; (defn make-thing [data file]
;;   (with-open [writer (io/writer file)]
;;     (csv/write-csv writer (cons ["Signup Week" ">25x and casted last week" ">50x and casted last week" ">100x and casted last week"] (format-rows data)))))

;; (defn merge-data [frequency-matrix merged-data]
;;   (map (fn [week-data]
;;          (let [week (get week-data :week)]
;;            (if-let [add-data (get merged-data week)]
;;              (update week-data :cast-frequencies merge add-data)
;;              week-data)))
;;        frequency-matrix))

;; (def merged-data (merge-data frequency-matrix count-and-last-week-map))

;; (defn format-frequency-matrix-row [data]
;;   [(or (:week data) "")
;;    (or (:num-signups data) 0)
;;    (or (get (:cast-frequencies data) :cast-0) 0)
;;    (or (get (:cast-frequencies data) :cast-1+) 0)
;;    (or (get (:cast-frequencies data) :cast-2+) 0)
;;    (or (get (:cast-frequencies data) :cast-5+) 0)
;;    (or (get (:cast-frequencies data) :cast-10+) 0)
;;    (or (get (:cast-frequencies data) :cast-25+) 0)
;;    (or (get (:cast-frequencies data) :users-casted-25+-and-casted-last-week) 0)
;;    (or (get (:cast-frequencies data) :cast-50+) 0)
;;    (or (get (:cast-frequencies data) :users-casted-50+-and-casted-last-week) 0)
;;    (or (get (:cast-frequencies data) :cast-100+) 0)
;;    (or (get (:cast-frequencies data) :users-casted-100+-and-casted-last-week) 0)])

;; (defn format-frequency-matrix-csv [data]
;;   (map format-frequency-matrix-row data))

;; (defn make-frequency-matrix-csv [data file]
;;   (with-open [writer (io/writer file)]
;;     (csv/write-csv writer (cons ["Signup Week"
;;                                  "Number Signups"
;;                                  "Casted 0 Times"
;;                                  ">1x"
;;                                  ">2x"
;;                                  ">5x"
;;                                  ">10x"
;;                                  ">25x"
;;                                  "Casted 25+ and Last Week"
;;                                  ">50x"
;;                                  "Casted 50+ and Last Week"
;;                                  ">100x"
;;                                  "Casted 100+ and Last Week"]
;;                                 (format-frequency-matrix-csv data)))))

;; ;; (make-frequency-matrix-csv merged-data "data/frequency-matrix-2.csv")
