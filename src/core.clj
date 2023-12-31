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

(defn user-cast-three-months? [user end-timestamp]
  (let [casts-by-user (get casts-by-fid (:users/fid user))
        three-months-ago (t/minus end-timestamp (t/months 3))]
    (or
     (some #(and (t/after? (u/get-timestamp-joda %) three-months-ago)
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

(defn get-users-of-week [users-activity week]
  (->> users-activity
       (filter #(= (:registered-at-week %) week))))

;; note: lwa = last week active
(defn classify-cast-count [user]
  (let [count (:total-cast-count user)]
    (cond-> []
      (>= count 1) (conj :cast-1+)
      (>= count 2) (conj :cast-2+)
      (>= count 5) (conj :cast-5+)
      (>= count 10) (conj :cast-10+)
      (>= count 25) (conj :cast-25+)
      (and (>= count 25) (:casted-last-week? user)) (conj :cast-lwa-25+)
      (>= count 50) (conj :cast-50+)
      (and (>= count 50) (:casted-last-week? user)) (conj :cast-lwa-50+)
      (>= count 100) (conj :cast-100+)
      (and (>= count 100) (:casted-last-week? user)) (conj :cast-lwa-100+)
      (zero? count) (conj :cast-0))))

(defn get-users-activity [users]
  (map (fn [user]
         {:fid (:users/fid user)
          :username (:users/username user)
          :registered-at (u/get-timestamp-joda user)
          :registered-at-week (week-of user)
          :total-cast-count (count (casts-by-fid (:users/fid user)))
          ;; :casted-three-months? (user-cast-three-months? user end-timestamp)
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
                      :cast-25+ 2,
                      :cast-lwa-25+ 2}}
;; explanation: on week 108 (since the first ever registered user), there are 234 signups
;; 98 have not casted, 136 have casted at least once, 92 have casted at least twice, etc.
;; lwa = last week active, :cast-lwa-25+ = users who have casted at least 25 times (lifetime)
;; and made at least one cast last week

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; manual asserts to make sure numbers are good this far
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def users-batch (get-users-of-week users-activity 55))
(first (filter #(= (:week %) 40) activity-by-registration-week))

;; (defn ex-active-users [users-activity]
;;   (->> users-activity
;;        (filter (fn [user]
;;                  (and (>= (:total-cast-count user) 50)
;;                       (not (:casted-three-months? user)))))
;;        (sort-by :total-cast-count >)))

;; (count (ex-active-users users-activity))
;; (take 3 (ex-active-users users-activity))
;; (u/write-csv (ex-active-users users-activity) "ex-active-users.csv")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; save to csv
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn stringify-map-week [week-data]
  (assoc week-data :week (week-to-string (:week week-data))))

(def csv-headers '(:week :num-signups :cast-0 :cast-1+ :cast-2+ :cast-5+
                         :cast-10+ :cast-25+ :cast-lwa-25+ :cast-50+
                         :cast-lwa-50+ :cast-100+ :cast-lwa-100+))
(def cast-frequency-keys (filter #(not (#{:week :num-signups} %)) csv-headers))

;; emtpy header defaults to 0
(defn fill-missing-keys [m keys]
  (reduce (fn [m k] (if (contains? m k) m (assoc m k 0))) m keys))

(defn fill-missing-cast-frequencies [data]
  (map (fn [m] (update m :cast-frequencies fill-missing-keys cast-frequency-keys)) data))

(defn flatten-cast-frequencies [m]
  (let [{:keys [week num-signups cast-frequencies]} m]
    (merge {:week week,
            :num-signups num-signups} cast-frequencies)))

(defn write-ordered-csv
  [filename header data]
  (let [ordered-data (map (fn [row] (map row header)) data)
        string-header (map name header)]
    (with-open [writer (io/writer filename)]
      (csv/write-csv writer (cons string-header ordered-data)))))

(def final-map (->> activity-by-registration-week
                    (fill-missing-cast-frequencies)
                    (map flatten-cast-frequencies)
                    (map stringify-map-week)))

(write-ordered-csv "data/final.csv" csv-headers final-map)