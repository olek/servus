(ns servus.login-response
  (:require [clojure.core.async :refer [<! >!! go-loop]]
            [clojure.tools.logging :refer [info warn]]
            [clojure.xml :as xml]
            [desk.util.debug :refer [pprint-map]]
            [environ.core :refer [env]]
            [mount.core :refer [defstate]]
            [servus.channels :refer [channels]]))

(defn- xml-node-content [xml node-name]
  (->> xml
       xml-seq
       (filter #(= (:tag %) node-name))
       first
       :content
       first))

(defn- extract-session-id-server-host [response]
  (let [response-seq (->> response
                          :body
                          .getBytes
                          java.io.ByteArrayInputStream.
                          xml/parse)]
    {:session-id (xml-node-content response-seq :sessionId)
     :server-instance (-> #"\w+.salesforce.com"
                          (re-find (xml-node-content response-seq :serverUrl)))}))

(defn process [username response]
  (>!! (:login-response-out channels)
       [username (extract-session-id-server-host response)]))

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
