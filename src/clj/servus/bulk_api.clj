(ns servus.bulk-api
  (:require [clojure.tools.logging :refer [info warn]]
            [org.httpkit.client :as http]
            [clostache.parser :refer [render-resource]]
            [clojure.xml :as xml]
            [clojure.core.async :refer [<! >!! go-loop]]
            [mount.core :refer [defstate]]
            [servus.channels :refer [channels]]))

(def ^:private socket-timeout 3000) ; in ms
(def ^:private keepalive 0) ; in ms
(def ^:private login-url "https://login.salesforce.com/services/Soap/u/39.0")
(def ^:private service-prefix "/services/async/39.0/")

(defn xml-node-content [xml node-name]
  (->> xml
       xml-seq
       (filter #(= (:tag %) node-name))
       first
       :content
       first))

(defn parse-response-body [response]
  (->> response
       :body
       .getBytes
       java.io.ByteArrayInputStream.
       xml/parse))

(defn parse-and-extract
  [response & tags]
  (let [xml (parse-response-body response)]
    (if (= 1 (count tags))
      (xml-node-content xml
                        (first tags))
      (zipmap tags
              (map (partial xml-node-content xml) tags)))))

(defn- generate-payload [template data]
  (render-resource (str "templates/" template ".xml.mustache") data))

(defn- do-request [url body headers handler]
  (http/post url
             {:body body
              :timeout socket-timeout
              :keepalive keepalive
              :headers (merge {"Content-Type" "text/xml; charset=UTF-8"}
                              headers)}
             handler))

(defn request [username
               {:keys [session-id server-instance]}
               path
               template
               data
               handler]
  (do-request (str "https://" server-instance service-prefix path)
              (generate-payload template data)
              {"X-SFDC-Session" session-id}
              handler))

(defn login-request [username password handler]
  (do-request login-url
              (generate-payload "login" {:username username
                                         :password password})
              {"SOAPAction" "login"}
              handler))
