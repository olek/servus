(ns servus.create-job-request
  (:require [clojure.core.async :refer [<! >!! go-loop chan close!]]
            [clojure.tools.logging :refer [info warn]]
            [environ.core :refer [env]]
            [mount.core :refer [defstate]]
            [servus.bulk-api :as bulk-api]
            [servus.channels :refer [channels]]))

(defn- process [username {:keys [session-id server-instance] :as session}]
  (bulk-api/request username session "job" "create-job" {:object "Case"}
                    (fn [response]
                      (>!! (:create-job-request-out channels) [username {:response response
                                                                         :session session}])
                      response)))

(defstate ^:private engine
  :start
  (let [ch (:create-job-request-in channels)
        quit-atom (atom false)]
    (go-loop [input :start]
      (condp = input
        :start
        (info "Waiting for create job requests...")

        nil
        (info "Not waiting for create job requests anymore, exiting")

        (apply process input))
      (when (and (not @quit-atom) input)
        (recur  (<! ch))))
    quit-atom)

  :stop
  (reset! engine true))
