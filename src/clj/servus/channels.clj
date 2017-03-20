(ns servus.channels
  (:require [clojure.core.async :refer [<! >! chan close! mult tap go-loop]]
            [clojure.tools.logging :refer [info warn]]
            [environ.core :refer [env]]
            [mount.core :refer [defstate]]))

(defstate ^:private channels
  :start
  (atom {:manifold (chan)})

  :stop
  (close! (:manifold @channels)))

(defn create-channel! [engine]
  (swap! channels
         merge
         {(keyword engine) (chan)}))

(defn engine-channel [engine]
  (get @channels (keyword engine)))

(defn close-channel! [engine]
  (close! (engine-channel engine))
  (swap! channels
         dissoc
         (keyword engine)))

(defn manifold-channel []
  (@channels :manifold))

(def ^:private routing-chain [:login-request
                              :login-response
                              :create-job-request
                              :create-job-response
                              :create-batch-request
                              :create-batch-response
                              :close-job-request
                              :close-job-response
                              :push-data])

(defn- route [source]
  (->> routing-chain
       rest
       (zipmap routing-chain)
       source))

(defstate ^:private manifold-engine
  :start
  (let [
        ch (:manifold @channels)
        quit-atom# (atom false)]
    ;; TODO catch all errors in go-loop
    (go-loop [input-message :start]
      (condp = input-message
        :start
        (info "Waiting for requests in manifold...")

        nil
        (info "Not waiting for requests in manifold anymore, exiting")

        (let [[source & message] input-message
              target (route source)]
          (when-not (= target :debug)
            (>! (engine-channel :debug) message))
          (>! (engine-channel target) message))
        )
      ;; TODO add timeout
      (when (and (not @quit-atom#) input-message)
        (recur  (<! ch))))
    quit-atom#)

  :stop
  (reset! manifold-engine true))
