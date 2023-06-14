(ns core
  (:require [clj-time.coerce :as c]
            [clj-time.core :as t]
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
        (.put counter week (conj (.getOrDefault counter week []) user))))
    (into {} counter)))

(defn get-casts-by-user [casts user]
  (filter (fn [cast] (= (:casts/author_fid cast) (:users/fid user))) casts))

(defn get-casts-in-week [casts user]
  (let [one-week-later (t/plus (get-timestamp-joda user) (t/weeks 1))]
    (filter (fn [cast]
              (and (t/after? (get-timestamp-joda cast) (get-timestamp-joda user))
                   (t/before? (get-timestamp-joda cast) one-week-later)))
            casts)))

(defn get-weekly-casts [casts users]
  (reduce (fn [result user]
            (assoc result (:users/username user) (-> casts
                                                     (get-casts-by-user user)
                                                     (get-casts-in-week user)
                                                     count)))
          {}
          users))

(def registrations-map (make-registrations-map users start-timestamp))
(def user-batch (:registrations/week-99 registrations-map))
(def weekly-casts (get-weekly-casts casts user-batch))