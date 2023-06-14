(ns core
  (:require [clj-time.coerce :as c]
            [clj-time.core :as t]
            [clj-time.periodic :as p]
            [writer :as w]))

;; TODO: use the actual cast data later on, 1000 for speeding up iteration
(def users (w/read-data "data/users.edn"))
(def casts (w/read-data "data/casts1000.edn"))

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

;; use this for all map-maker
(def start-timestamp (->> users
                          (sort-by :users/registered_at)
                          first
                          get-timestamp-joda))

(defn get-week [start-timestamp timestamp]
  (let [timestamp-joda (get-timestamp-joda timestamp)]
    (if (.isEqual start-timestamp timestamp-joda)
      0
      (-> (t/interval start-timestamp timestamp-joda)
          (.toDuration)
          (.getStandardDays)
          (/ 7)
          int))))

(defn make-registrations-map [users start-timestamp]
  (let [counter (java.util.concurrent.ConcurrentHashMap.)]
    (doseq [user users]
      (let [week (keyword (str "registrations/week-" (get-week start-timestamp (get-timestamp-joda user))))]
        (.put counter week (inc (.getOrDefault counter week 0)))))
    (into {} counter)))

(def registrations-map (make-registrations-map users start-timestamp))
(->> registrations-map
     (sort-by val)
     (reverse)
     (take 10))
(:registrations/week-0 registrations-map)

;; (defn get-weekly-timestamps [start-timestamp]
;;   (let [now (t/now)
;;         start start-timestamp]
;;     (take-while #(t/before? % now) (p/periodic-seq start (t/weeks 1)))))

;; (spit "data/register-groups.edn" (pr-str register-group))
;; (group-users-by-week users first-cast-timestamp)

;; (defn get-weekly-user-counts [casts users]
;;   (let [
;;         weekly-timestamps (get-weekly-timestamps first-cast-timestamp)
;;         start-timestamp (first weekly-timestamps)
;;         user-groups (group-users-by-week users start-timestamp)
;;         timestamp-week-map (zipmap weekly-timestamps (range))]
;;     (into {} (for [[week users] user-groups]
;;                [(get timestamp-week-map week) (count users)]))))

;; (defn generate-weekly-report [users casts week-timestamp]
;;   (let [week-users (filter #(>= (:users/registered_at %) week-timestamp) users)
;;         week-user-ids (map :users/fid week-users)
;;         week-casts (filter #(and (>= (:casts/timestamp %) week-timestamp) (contains? (set week-user-ids) (:casts/author_fid %))) casts)
;;         cast-stats (frequencies (map :casts/author_fid week-casts))]
;;     {:signup_week (java.time.Instant/ofEpochMilli week-timestamp)
;;      :number_signups (count week-users)
;;      :casted_0_times (count (filter #(not (contains? cast-stats %)) week-user-ids))
;;      :casted_1x (count (filter #(= (get cast-stats % 0) 1) week-user-ids))
;;      :casted_>10x (count (filter #(> (get cast-stats % 0) 10) week-user-ids))
;;      :casted_>25x (count (filter #(> (get cast-stats % 0) 25) week-user-ids))
;;      :casted_>50x (count (filter #(> (get cast-stats % 0) 50) week-user-ids))
;;      :casted_>100x (count (filter #(> (get cast-stats % 0) 100) week-user-ids))
;;      :casted_>5_lifetime (count (filter #(> (get cast-stats % 0) 5) week-user-ids))
;;      :casted_last_week (count (filter #(= (get cast-stats % 0) 1) week-user-ids))
;;      :casted_>10_lifetime (count (filter #(> (get cast-stats % 0) 10) week-user-ids))
;;      :casted_>25_lifetime (count (filter #(> (get cast-stats % 0) 25) week-user-ids))}))

;; (defn generate-all-weekly-reports [users casts]
;;   (let [week-timestamps (get-weekly-timestamps casts)]
;;     (map #(generate-weekly-report users casts %) week-timestamps)))

;; (generate-all-weekly-reports users casts)

;; (defn filter-casts-by-week [casts week-start]
;;   (filter-by-week casts week-start :casts/timestamp))

;; (defn filter-users-by-week [users week-start]
;;   (filter-by-week users week-start :users/registered_at))

;; (defn count-by-criteria [users fid casts attr val]
;;   (->> casts
;;        (filter #(= (:casts/author_fid %) fid))
;;        (filter #(>= (:attr %) val))
;;        count))

;; (defn weekly-cast-counts [users fid casts]
;;   (let [weekly-timestamps (get-weekly-timestamps casts)
;;         user-weekly-stats (fn [week-start]
;;                             (let [weekly-casts (filter-casts-by-week casts week-start)
;;                                   weekly-users (filter-users-by-week users week-start)
;;                                   user-counts {:signup (count weekly-users)
;;                                                :casted-0 (count-by-criteria weekly-users fid weekly-casts :casts/timestamp 0)
;;                                                :casted-1 (count-by-criteria weekly-users fid weekly-casts :casts/timestamp 1)
;;                                                :casted->10 (count-by-criteria weekly-users fid weekly-casts :casts/timestamp 10)
;;                                                :casted->25 (count-by-criteria weekly-users fid weekly-casts :casts/timestamp 25)
;;                                                :casted->50 (count-by-criteria weekly-users fid weekly-casts :casts/timestamp 50)
;;                                                :casted->100 (count-by-criteria weekly-users fid weekly-casts :casts/timestamp 100)}]
;;                               user-counts))]
;;     (reduce (fn [counts timestamp]
;;               (let [user-stats (user-weekly-stats timestamp)]
;;                 (assoc counts timestamp user-stats)))
;;             {}
;;             weekly-timestamps)))

;; (defn create-weekly-table [users casts fid]
;;   (let [weekly-cast-counts (weekly-cast-counts users fid casts)]
;;     (->> weekly-cast-counts
;;          (map (fn [[timestamp counts]]
;;                 {:week (f/formatter :date (c/from-long timestamp))
;;                  :counts counts}))
;;          (into {}))))

;; (create-weekly-table users casts 3)