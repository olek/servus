(ns servus.channels
  (:require [clojure.core.async :refer [chan close! mult tap]]
            [clojure.tools.logging :refer [info warn]]
            [environ.core :refer [env]]
            [mount.core :refer [defstate]]))

(defstate ^:private channels
  :start
  (let [channels (atom {:login-request-in (chan)
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
                        :debug-in (chan)})
        route (fn [from to]
                ;(pipe (get @channels from) (get @channels to) false))]
                (let [m (mult (get @channels (keyword (str (name from) "-out"))))]
                  (tap m (get @channels (keyword (str (name to) "-in"))))
                  (tap m (get @channels :debug-in))))
        routing-chain [:login-request
                       :login-response
                       :create-job-request
                       :create-job-response
                       :create-batch-request
                       :create-batch-response
                       :close-job-request
                       :close-job-response
                       :debug]]
    (doseq [[from to] (zipmap routing-chain
                                  (rest routing-chain))]
      (route from to))
    (info "Created channels")
    channels)

  :stop
  (do
    (info "Closing channels")
    (doseq [ch (vals @channels)]
      (close! ch))))

(defn channel-for [engine direction]
  (get @channels (keyword (str (name engine) "-" (name direction)))))
