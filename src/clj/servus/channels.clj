(ns servus.channels
  (:require [clojure.core.async :refer [chan close! mult tap]]
            [clojure.tools.logging :refer [info warn]]
            [environ.core :refer [env]]
            [mount.core :refer [defstate]]))

(defstate channels
  :start
  (let [channels {:login-request-in (chan)
                  :login-request-out (chan)
                  :login-response-in (chan)
                  :login-response-out (chan)
                  :create-job-request-in (chan)
                  :create-job-request-out (chan)
                  :create-job-response-in (chan)
                  :create-job-response-out (chan)
                  :debug (chan)}
        route (fn [from to]
                ;(pipe (get channels from) (get channels to) false))]
                (let [m (mult (get channels from))]
                  (tap m (get channels to))
                  (tap m (get channels :debug))))]
    (route :login-request-out :login-response-in)
    (route :login-response-out :create-job-request-in)
    (route :create-job-request-out :create-job-response-in)
    (route :create-job-response-out :debug)
    (info "Created channels")
    channels)

  :stop
  (do
    (info "Closing channels")
    (doseq [ch (vals channels)]
      (close! ch))))
