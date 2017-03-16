(ns servus.login-response
  (:require [clojure.core.async :refer [<! >!! go-loop]]
            [clojure.tools.logging :refer [info warn]]
            [environ.core :refer [env]]
            [mount.core :refer [defstate]]
            [servus.bulk-api :as bulk-api]
            [servus.channels :refer [channels]]))

(defn- process [username response]
  (>!! (:login-response-out channels)
       [username (let [data (bulk-api/parse-and-extract response :sessionId :serverUrl)]
                   {:session-id (:sessionId data)
                    :server-instance (-> #"\w+.salesforce.com"
                                         (re-find (:serverUrl data)))})]))

(defstate ^:private engine
  :start
  (let [ch (:login-response-in channels)
        quit-atom (atom false)]
    (go-loop [input :start]
      (condp = input
        :start
        (info "Waiting for login responses...")

        nil
        (info "Not waiting for login responses anymore, exiting")

        (apply process input))
      (when (and (not @quit-atom) input)
        (recur  (<! ch))))
    quit-atom)

  :stop
  (reset! engine true))
