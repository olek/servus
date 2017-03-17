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
    (bulk-api/request "job"
                      username
                      {:session session
                       :template "create-job.xml"
                       :data {:object "Case"}}
                      output-handler)))

(create-engine :create-job-response
  (let [response (:response (last input-message))
        job-id (bulk-api/parse-and-extract response :id)]
    (output-handler job-id {:job-id job-id})))

(create-engine :create-batch-request
  (let [[username session] input-message]
    (bulk-api/request (str "job/" (:job-id session) "/batch")
                      username
                      {:session session
                       :template "create-batch.sql"
                       :data {:object "Case"
                              :fields "Subject"
                              :limit 2}}
                      output-handler)))

(create-engine :create-batch-response
  (let [session (last input-message)
        response (:response session)
        prior-batches (:queued-batch-ids session)
        batch-id (bulk-api/parse-and-extract response :id)]
    (output-handler batch-id {:queued-batch-ids (conj prior-batches batch-id)})))

(create-engine :close-job-request
  (let [[username session] input-message]
    (bulk-api/request (str "job/" (:job-id session))
                      username
                      {:session session
                       :template "close-job.xml"
                       :data {}}
                      output-handler)))

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
