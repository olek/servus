(ns servus.create-job-request
  (:require [clojure.core.async :refer [<! >!! go-loop chan close!]]
            [clojure.tools.logging :refer [info warn]]
            [clostache.parser :refer [render-resource]]
            [environ.core :refer [env]]
            [mount.core :refer [defstate]]
            [org.httpkit.client :as http]
            [servus.channels :refer [channels]]))

(def ^:private socket-timeout 3000) ; in ms
(def ^:private keepalive 0) ; in ms
(def service-prefix "/services/async/39.0/")

(defn- non-login [username {:keys [session-id server-instance] :as session}
                  path
                  template
                  data]
  (http/post (str "https://" server-instance service-prefix path)
             {:body (render-resource (str "templates/" template ".xml.mustache") data)
              :timeout socket-timeout
              :keepalive keepalive
              :headers {"Content-Type" "application; charset=UTF-8"
                        "X-SFDC-Session" session-id
                        "Accept" "application/json"}}
             (fn [response]
               (>!! (:create-job-request-out channels) [username response])
               response)))

(defn- process [username {:keys [session-id server-instance] :as session}]
  (non-login username session "job" "create-job" {:object "Case"}))
             ; #(info "Extracted JobID" (extract-job-id %))))

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
