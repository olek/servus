(ns servus.salesforce-api
  (:require [clojure.tools.logging :refer [info warn]]
            [org.httpkit.client :as http]
            [clostache.parser :refer [render-resource]]
            [clojure.xml :as xml]
            [mount.core :refer [defstate]]
            [clojure.string :as s]))

(def ^:private socket-timeout 7000) ; in ms
(def ^:private keepalive 0) ; in ms
(def ^:private login-url "https://login.salesforce.com/services/Soap/u/39.0")
(def ^:private bulk-api-prefix "/services/async/39.0/")
(def ^:private data-api-prefix "/services/data/v39.0/")

(defn xml-node-content [xml node-name]
  (->> xml
       xml-seq
       (filter #(= (:tag %) node-name))
       first
       :content
       first))


(defn xml-node-contents [xml node-names]
  (->> xml
       xml-seq
       (filter #(some #{(:tag %)} node-names))
       (map :content)
       (map first)))

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

(defn parse-and-extract-all
  [response & tags]
  (let [xml (parse-response-body response)
        raw-list (xml-node-contents xml tags)]
    (if (= (count tags) 1)
      raw-list
      (partition (count tags) raw-list))))

(defn- generate-payload [template data]
  (when (and template data)
    (render-resource (str "templates/" template ".mustache") data)))

(defn- compress-xml [body]
  (s/replace (with-out-str (xml/emit (parse-xml body))) #"[\n\r]" ""))

(defn- do-request [engine username url body headers handler]
  (let [content-type (or (headers "Content-Type")
                         "text/xml; charset=UTF-8")
        xml-content? (.startsWith content-type "text/xml")
        compressed-body (if (and body
                                 xml-content?)
                          (compress-xml body)
                          body)
        [http-fn http-method] (if body [http/post "POST"] [http/get "GET"])]
    (info (str "[" username "]") (name engine) "callout" "-" http-method url compressed-body headers)
    (http-fn url
               {:body compressed-body
                :timeout socket-timeout
                :keepalive keepalive
                :headers (merge {"Content-Type" "text/xml; charset=UTF-8"}
                                headers)}
               handler)))

(defn data-request [engine username session-id server-instance path options handler]
  (let [{template :template
         data :data} options]
    (do-request engine
                username
                (str "https://" server-instance data-api-prefix path)
                (generate-payload template data)
                {"Authorization" (str "Bearer " session-id)
                 "Accept" "application/xml"
                 "Content-Type" (when (and template
                                           (.endsWith template ".sql"))
                                  "text/csv")}
                handler)))

(defn bulk-request [engine username session-id server-instance path options handler]
  (let [{template :template
         data :data} options]
    (do-request engine
                username
                (str "https://" server-instance bulk-api-prefix path)
                (generate-payload template data)
                {"X-SFDC-Session" session-id
                 "Content-Type" (when (and template
                                           (.endsWith template ".sql"))
                                  "text/csv")}
                handler)))

;; TODO figure out how to solve ugly problem of 2 args (session-id and server-instance) that are not
;; actually meaningful/needed here
(defn login-request [engine username session-id server-instance password handler]
  (do-request engine
              username
              login-url
              (generate-payload "login.xml" {:username username
                                             :password password})
              {"SOAPAction" "login"}
              handler))
