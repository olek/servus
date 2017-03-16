(ns servus.login-request
  (:require [clojure.core.async :refer [<! >!! go-loop]]
            [clojure.tools.logging :refer [info warn]]
            [environ.core :refer [env]]
            [mount.core :refer [defstate]]
            [servus.bulk-api :as bulk-api]
            [servus.channels :refer [channels]]))

(defn- process [username password]
  (bulk-api/login-request username password
                  (fn [response]
                    (>!! (:login-request-out channels) [username response])
                    response)))

(defstate ^:private engine
  :start
  (let [ch (:login-request-in channels)
        quit-atom (atom false)]
    (go-loop [input :start]
      (condp = input
        :start
        (info "Waiting for login requests...")

        nil
        (info "Not waiting for login requests anymore, exiting")

        (apply process input))
      ;; TODO add timeout
      (when (and (not @quit-atom) input)
        (recur  (<! ch))))
    quit-atom)

  :stop
  (reset! engine true))
