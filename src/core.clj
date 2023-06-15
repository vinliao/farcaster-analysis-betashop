(ns core
  (:require [clj-time.coerce :as c]
            [clj-time.core :as t]
            [writer :as w]))

(def users (w/read-data "data/users.edn"))
(def casts (w/read-data "data/casts.edn"))

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

(defn get-weekly-casts [casts users]
  (let [get-casts-by-user (fn [casts user]
                            (filter (fn [cast] (= (:casts/author_fid cast) (:users/fid user))) casts))
        get-casts-in-week (fn [casts user]
                            (let [one-week-later (t/plus (get-timestamp-joda user) (t/weeks 1))]
                              (filter (fn [cast]
                                        (and (t/after? (get-timestamp-joda cast) (get-timestamp-joda user))
                                             (t/before? (get-timestamp-joda cast) one-week-later)))
                                      casts)))]
    (reduce (fn [result user]
              (assoc result (:users/username user)
                     (-> casts
                         (get-casts-by-user user)
                         (get-casts-in-week user)
                         count)))
            {}
            users)))

(def registrations-map (w/read-data "data/registrations-map.edn"))
(defn get-weekly-casts-for-all-weeks [casts registrations-map]
  (reduce (fn [result week-key]
            (let [user-batch (get registrations-map week-key)
                  weekly-casts-for-week (get-weekly-casts casts user-batch)]
              (assoc result week-key weekly-casts-for-week)))
          {}
          (keys registrations-map)))

(def weekly-casts-map (get-weekly-casts-for-all-weeks casts registrations-map))