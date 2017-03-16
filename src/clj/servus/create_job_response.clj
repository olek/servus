(ns servus.create-job-response
  (:require [clojure.core.async :refer [<! >!! go-loop]]
            [clojure.tools.logging :refer [info warn]]
            [environ.core :refer [env]]
            [mount.core :refer [defstate]]
            [servus.bulk-api :as bulk-api]
            [servus.channels :refer [channels]]))

(defn- process [username {:keys [response session]}]
  (>!! (:create-job-response-out channels)
       [username (assoc session :job-id (bulk-api/parse-and-extract response :id))]))

(defstate ^:private engine
  :start
  (let [ch (:create-job-response-in channels)
        quit-atom (atom false)]
    (go-loop [input :start]
      (condp = input
        :start
        (info "Waiting for create job responses...")

        nil
        (info "Not waiting for create job responses anymore, exiting")

        (apply process input))
      (when (and (not @quit-atom) input)
        (recur  (<! ch))))
    quit-atom)

  :stop
  (reset! engine true))
