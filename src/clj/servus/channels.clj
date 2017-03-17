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
                  :create-batch-request-in (chan)
                  :create-batch-request-out (chan)
                  :create-batch-response-in (chan)
                  :create-batch-response-out (chan)
                  :close-job-request-in (chan)
                  :close-job-request-out (chan)
                  :close-job-response-in (chan)
                  :close-job-response-out (chan)
                  :debug-in (chan)}
        route (fn [from to]
                ;(pipe (get channels from) (get channels to) false))]
                (let [m (mult (get channels from))]
                  (tap m (get channels to))
                  (tap m (get channels :debug-in))))]
    (route :login-request-out :login-response-in)
    (route :login-response-out :create-job-request-in)
    (route :create-job-request-out :create-job-response-in)
    (route :create-job-response-out :create-batch-request-in)
    (route :create-batch-request-out :create-batch-response-in)
    (route :create-batch-response-out :close-job-request-in)
    (route :close-job-request-out :close-job-response-in)
    (route :close-job-response-out :debug-in)
    (info "Created channels")
    channels)

  :stop
  (do
    (info "Closing channels")
    (doseq [ch (vals channels)]
      (close! ch))))
