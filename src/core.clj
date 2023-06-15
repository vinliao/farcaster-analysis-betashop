(ns core
  (:require [clj-time.coerce :as c]
            [clj-time.core :as t]
            [writer :as w]))

(def users (w/read-data "data/users.edn"))
(def casts (w/read-data "data/casts.edn"))
(defn group-casts-by-fid [casts]
  (group-by :casts/author_fid casts))
(def casts-by-fid (group-casts-by-fid casts))

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

(def end-timestamp (->> casts
                        (sort-by :casts/timestamp)
                        last
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

(defn get-first-week-casts [user]
  (let [casts-by-user (get casts-by-fid (:users/fid user))
        one-week-later (t/plus (get-timestamp-joda user) (t/weeks 1))]
    (filter (fn [cast]
              (and (t/after? (get-timestamp-joda cast) (get-timestamp-joda user))
                   (t/before? (get-timestamp-joda cast) one-week-later)))
            casts-by-user)))

(defn user-cast-last-week? [user end-timestamp]
  (let [casts-by-user (get casts-by-fid (:users/fid user))
        one-week-ago (t/minus end-timestamp (t/weeks 1))]
    (some #(and (t/after? (get-timestamp-joda %) one-week-ago)
                (t/before? (get-timestamp-joda %) end-timestamp))
          casts-by-user)))

(defn get-user-cast-data [users end-timestamp]
  (map (fn [user]
         {:users/username (:users/username user)
          :users/first-week-cast-count (count (get-first-week-casts user))
          :users/cast-last-week? (user-cast-last-week? user end-timestamp)})
       users))

(get-user-cast-data users end-timestamp)

(defn make-registrations-map [users start-timestamp]
  (let [counter (java.util.concurrent.ConcurrentHashMap.)]
    (doseq [user users]
      (let [week (keyword (str "registrations/week-" (get-week start-timestamp (get-timestamp-joda user))))]
        (.put counter week (conj (.getOrDefault counter week []) user))))
    (into {} counter)))

;; (defn get-weekly-casts-for-all-weeks [casts-by-fid registrations-map]
;;   (reduce (fn [result week-key]
;;             (let [user-batch (get registrations-map week-key)
;;                   weekly-casts-for-week (map #(get-first-week-casts casts-by-fid %) user-batch)]
;;               (assoc result week-key (apply concat weekly-casts-for-week))))
;;           {}
;;           (keys registrations-map)))

;; (def registrations-map (make-registrations-map users start-timestamp))
;; (def weekly-casts-map (get-weekly-casts-for-all-weeks casts-by-fid registrations-map))
;; (first weekly-casts-map)

;; (w/append-data "data/first-week-cast-frequency.edn" weekly-casts-map)
;; (defn get-user [username] (first (filter #(= (:users/username %) username) users)))
;; (get-first-week-casts casts-by-fid (get-user "abc"))