(ns core
  (:require [clj-time.coerce :as c]
            [clj-time.core :as t]
            [clj-time.periodic :as p]
            [writer :as w]))

(def casts (take 1000 (shuffle (w/read-data "data/casts.edn"))))
(def users (w/read-data "data/users.edn"))

(defn get-weekly-timestamps [casts]
  (let [first-cast-timestamp (->> casts (sort-by :casts/timestamp) first :casts/timestamp)
        now (t/now)
        start (c/from-long first-cast-timestamp)
        timestamps (take-while #(t/before? % now) (p/periodic-seq start (t/weeks 1)))]
    (map c/to-long timestamps)))

(defn filter-by-week [data timestamp attr]
  (let [week-start timestamp
        week-end (+ week-start (* 7 24 60 60 1000))]
    (filter (fn [d]
              (let [date (:attr d)]
                (and (>= date week-start)
                     (< date week-end))))
            data)))

(defn filter-casts-by-week [casts week-start]
  (filter-by-week casts week-start :casts/timestamp))

(defn filter-users-by-week [users week-start]
  (filter-by-week users week-start :users/registered_at))

(defn count-by-criteria [users fid casts attr val]
  (->> casts
       (filter #(= (:casts/author_fid %) fid))
       (filter #(>= (:attr %) val))
       count))

(defn weekly-cast-counts [users fid casts]
  (let [weekly-timestamps (get-weekly-timestamps casts)
        user-weekly-stats (fn [week-start]
                            (let [weekly-casts (filter-casts-by-week casts week-start)
                                  weekly-users (filter-users-by-week users week-start)
                                  user-counts {:signup (count weekly-users)
                                               :casted-0 (count-by-criteria weekly-users fid weekly-casts :casts/timestamp 0)
                                               :casted-1 (count-by-criteria weekly-users fid weekly-casts :casts/timestamp 1)
                                               :casted->10 (count-by-criteria weekly-users fid weekly-casts :casts/timestamp 10)
                                               :casted->25 (count-by-criteria weekly-users fid weekly-casts :casts/timestamp 25)
                                               :casted->50 (count-by-criteria weekly-users fid weekly-casts :casts/timestamp 50)
                                               :casted->100 (count-by-criteria weekly-users fid weekly-casts :casts/timestamp 100)}]
                              user-counts))]
    (reduce (fn [counts timestamp]
              (let [user-stats (user-weekly-stats timestamp)]
                (assoc counts timestamp user-stats)))
            {}
            weekly-timestamps)))

(defn create-weekly-table [users casts fid]
  (let [weekly-cast-counts (weekly-cast-counts users fid casts)]
    (->> weekly-cast-counts
         (map (fn [[timestamp counts]]
                {:week (t/format :date (c/from-long timestamp))
                 :counts counts}))
         (into {}))))