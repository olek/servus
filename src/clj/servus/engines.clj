(ns servus.engines
  (:require [clojure.tools.logging :refer [info]]
            [servus.bulk-api :as bulk-api]
            [servus.engine-factory :refer [create-engine]]))

(create-engine :login-request
  (let [[username {:keys [password]}] input-message]
    (bulk-api/login-request username password output-handler)))

(create-engine :login-response
  (let [response (:response (last input-message))
        data (bulk-api/parse-and-extract response
                                         :sessionId :serverUrl)
        session-id (:sessionId data)
        server-instance (-> #"\w+.salesforce.com"
                            (re-find (:serverUrl data)))]
    (output-handler [session-id server-instance]
                    {:session-id session-id
                     :server-instance server-instance})))

(create-engine :create-job-request
  (let [[username session] input-message]
    (bulk-api/request username session "job" "create-job" {:object "Case"} output-handler)))

(create-engine :create-job-response
  (let [response (:response (last input-message))
        job-id (bulk-api/parse-and-extract response :id)]
    (output-handler job-id {:job-id job-id})))

(create-engine :close-job-request
  (let [[username session] input-message]
    (bulk-api/request username session (str "job/" (:job-id session)) "close-job" {} output-handler)))

(create-engine :close-job-response
  (let [response (:response (last input-message))
        job-id (bulk-api/parse-and-extract response :id)]
    (output-handler job-id {:job-id nil})))

(create-engine :debug
  (let [data (or (:response (last input-message))
                 input-message)
        data (or (:body data)
                 data)]
    (info (str "Response [" (first input-message) "]") (pr-str data))))
