(ns servus.engines
  (:require [clojure.stacktrace :refer [print-cause-trace]]
            [clojure.tools.logging :refer [info error]]
            [servus.bulk-api :as bulk-api]
            [servus.engine-factory :refer [create-engine]]))

(create-engine :login-request
  (let [[username {:keys [password]}] input-message]
    (bulk-api/login-request :login-request username password output-handler)))

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
    (bulk-api/request :create-job-request
                      "job"
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
    (bulk-api/request :create-batch-request
                      (str "job/" (:job-id session) "/batch")
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

(create-engine :check-batch-request
  (let [[username session] input-message]
    ;; TODO figure out way to handle more than one batch
    (bulk-api/request :check-batch-request
                      (str "job/" (:job-id session) "/batch/" (first (:queued-batch-ids session)))
                      username
                      {:session session}
                      output-handler)))

(create-engine :check-batch-response
  (let [session (last input-message)
        response (:response session)
        queued-batches (:queued-batch-ids session)
        completed-batches (:completed-batch-ids session)
        batch-state (bulk-api/parse-and-extract response :state)
        batch-id (bulk-api/parse-and-extract response :id)]
    (if (= "Completed" batch-state)
      (output-handler batch-state {:queued-batch-ids (remove #{batch-id} queued-batches)
                                :completed-batch-ids (conj queued-batches batch-id)})
      (output-handler batch-state {}))))

(create-engine :close-job-request
  (let [[username session] input-message]
    (bulk-api/request :close-job-request
                      (str "job/" (:job-id session))
                      username
                      {:session session
                       :template "close-job.xml"
                       :data {}}
                      output-handler)))

(create-engine :close-job-response
  (let [response (:response (last input-message))
        job-id (bulk-api/parse-and-extract response :id)]
    (output-handler job-id {:job-id nil})))

(create-engine :push-data
  (let [[username session] input-message]
    (info (str "[" username "]") "Make-believe pushing data to lala-land")))

(create-engine :debug
  (let [[username session] input-message
        source (name (:engine session))
        data (or (:response session)
                 input-message)

        data (or (:body data)
                 data)]
    (info (str "[" username "]") source "produced" (pr-str data))))

(create-engine :error
  (let [[username session] input-message
        source (name (:engine session))
        data (or (:response session)
                 session)

        data (if (isa? (class data) Exception)
               (with-out-str (print-cause-trace data))
               (pr-str data))]
    ;; TODO exctact/output exceptionCode and exceptionMessage tags nicely if available in response xml
    (error (str "[" username "]") source "caused error" data)))
