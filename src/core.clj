(ns core
  (:gen-class)
  (:require
   [clj-time.core :as t]
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [utils :as u]
   [writer :as w]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; data init
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def users (w/read-data "data/users.edn"))
(def casts (w/read-data "data/casts.edn"))
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

;; (csv/write-csv "processed-users.csv" (map #(vector (:fid %)
;;                                                    (:username %)
;;                                                    (:registered-at %)
;;                                                    (:first-week-cast-count %)
;;                                                    (:casted-last-week? %))
;;                                           processed-users))

;; (:username (first processed-users))
;; (spit "data/processed-users.edn" (pr-str processed-users))

;; (defn get-week [start-timestamp timestamp]
;;   (let [timestamp-joda (u/get-timestamp-joda timestamp)]
;;     (if (.isEqual start-timestamp timestamp-joda)
;;       0
;;       (-> (t/interval start-timestamp timestamp-joda)
;;           (.toDuration)
;;           (.getStandardDays)
;;           (/ 7)
;;           int))))

;; (defn make-registrations-map [users start-timestamp]
;;   (let [counter (java.util.concurrent.ConcurrentHashMap.)]
;;     (doseq [user users]
;;       (let [week (keyword (str "registrations/week-" (get-week start-timestamp (u/get-timestamp-joda user))))]
;;         (.put counter week (conj (.getOrDefault counter week []) user))))
;;     (into {} counter)))

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