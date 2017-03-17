(ns servus.bulk-api
  (:require [clojure.tools.logging :refer [info warn]]
            [org.httpkit.client :as http]
            [clostache.parser :refer [render-resource]]
            [clojure.xml :as xml]
            [mount.core :refer [defstate]]
            [clojure.string :as s]))

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

(defn parse-xml [body]
  (->> body
       .getBytes
       java.io.ByteArrayInputStream.
       xml/parse))

(defn parse-response-body [response]
  (->> response
       :body
       parse-xml))

(defn parse-and-extract
  [response & tags]
  (let [xml (parse-response-body response)]
    (if (= 1 (count tags))
      (xml-node-content xml
                        (first tags))
      (zipmap tags
              (map (partial xml-node-content xml) tags)))))

(defn- generate-payload [template data]
  (render-resource (str "templates/" template ".mustache") data))

(defn- compress-xml [body]
  (s/replace (with-out-str (xml/emit (parse-xml body))) #"[\n\r]" ""))

(defn- do-request [username url body headers handler]
  (let [content-type (or (headers "Content-Type")
                         "text/xml; charset=UTF-8")
        xml-content? (.startsWith content-type "text/xml")
        compressed-body (if xml-content?
                          (compress-xml body)
                          body)]
    (info (str "Request [" username "]") url compressed-body headers)
    (http/post url
               {:body compressed-body
                :timeout socket-timeout
                :keepalive keepalive
                :headers (merge {"Content-Type" "text/xml; charset=UTF-8"}
                                headers)}
               handler)))

(defn request [path username options handler]
  (let [{{:keys [session-id server-instance]} :session
         template :template
         data :data} options]
    (do-request username
                (str "https://" server-instance service-prefix path)
                (generate-payload template data)
                {"X-SFDC-Session" session-id
                 "Content-Type" (when (.endsWith template ".sql") "text/csv")}
                handler)))

(defn login-request [username password handler]
  (do-request username
              login-url
              (generate-payload "login.xml" {:username username
                                             :password password})
              {"SOAPAction" "login"}
              handler))
