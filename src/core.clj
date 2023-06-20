(ns core
  (:gen-class)
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
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

(def csv-headers-map {:week "Signup Week"
                      :num-signups "Number Signups"
                      :cast-0 "Casted 0 Times"
                      :cast-1+ ">1x"
                      :cast-2+ ">2x"
                      :cast-5+ ">5x"
                      :cast-10+ ">10x"
                      :cast-25+ ">25x"
                      :cast-lwa-25+ "Casted 25+ and Casted Last Week"
                      :cast-50+ ">50x"
                      :cast-lwa-50+ "Casted 50+ and Casted Last Week"
                      :cast-100+ ">100x"
                      :cast-lwa-100+ "Casted 100+ and Casted Last Week"})

(def cast-frequency-keys (keys (dissoc csv-headers-map :week :num-signups)))

;; emtpy header defaults to 0
(defn fill-missing-keys [m keys]
  (into m (for [key keys] (when-not (contains? m key) [key 0]))))

(defn fill-missing-cast-frequencies [data keys]
  (map (fn [m] (update m :cast-frequencies fill-missing-keys keys)) data))

(def final-map (map stringify-map-week (fill-missing-cast-frequencies activity-by-registration-week cast-frequency-keys)))

(defn flatten-map [m prefix]
  (reduce-kv
   (fn [acc k v]
     (if (map? v)
       (merge acc (flatten-map v (str prefix k "-")))
       (assoc acc (str prefix k) v)))
   {} m))

(defn flatten-data [data]
  (map #(flatten-map % "") data))

;; (u/write-csv (flatten-data final-map) "data/final.csv")