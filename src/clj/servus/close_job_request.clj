(ns servus.close-job-request
  (:require [clojure.core.async :refer [<! >!! go-loop chan close!]]
            [clojure.tools.logging :refer [info warn]]
            [clostache.parser :refer [render-resource]]
            [environ.core :refer [env]]
            [mount.core :refer [defstate]]
            [org.httpkit.client :as http]
            [servus.bulk-api :as bulk-api]
            [servus.channels :refer [channels]]))

(defn- process [username {:keys [job-id] :as session}]
  (bulk-api/request username session (str "job/" job-id) "close-job" {}
                    (fn [response]
                      (>!! (:close-job-request-out channels) [username {:response response
                                                                        :session session}])
                      response)))

(defstate ^:private engine
  :start
  (let [ch (:close-job-request-in channels)
        quit-atom (atom false)]
    (go-loop [input :start]
      (condp = input
        :start
        (info "Waiting for close job requests...")

        nil
        (info "Not waiting for close job requests anymore, exiting")

        (apply process input))
      (when (and (not @quit-atom) input)
        (recur  (<! ch))))
    quit-atom)

  :stop
  (reset! engine true))
