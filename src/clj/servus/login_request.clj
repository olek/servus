(ns servus.login-request
  (:require [clojure.core.async :refer [<! >!! go-loop]]
            [clojure.tools.logging :refer [info warn]]
            [clostache.parser :refer [render-resource]]
            [desk.util.debug :refer [pprint-map]]
            [environ.core :refer [env]]
            [mount.core :refer [defstate]]
            [org.httpkit.client :as http]
            [servus.channels :refer [channels]]))

(def ^:private socket-timeout 3000) ; in ms
(def ^:private keepalive 0) ; in ms
(def login-url "https://login.salesforce.com/services/Soap/u/39.0")

(defn- generate-login-payload [username password]
  (render-resource "templates/login.xml.mustache" {:username username
                                                   :password password}))

(defn- process [username password]
  (http/post login-url
             {:body (generate-login-payload username password)
              :timeout socket-timeout
              :keepalive keepalive
              :headers {"Content-Type" "text/xml; charset=UTF-8"
                        "SOAPAction" "login"
                        "Accept" "application/json"}}
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
