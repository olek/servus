(ns servus.engines
  (:require [clojure.stacktrace :refer [print-cause-trace]]
            [clojure.tools.logging :refer [info error]]
            [servus.bulk-api :as bulk-api]
            [servus.engine-factory :refer [create-terminal-engine create-callout-engine]]))

(create-callout-engine :login
  :send (let [[username {:keys [password]}] message]
          (bulk-api/login-request :login-request username password proceed))
  :parse (let [response (:response (last message))
               data (bulk-api/parse-and-extract response
                                                :sessionId :serverUrl)
               session-id (:sessionId data)
               server-instance (-> #"\w+.salesforce.com"
                                   (re-find (:serverUrl data)))]
           (proceed [session-id server-instance]))
  :process (let [response (:response (last message))]
             (proceed :create-job
                             {:session-id (first response)
                              :server-instance (last response)}))
  :error (proceed :finish))

(create-callout-engine :create-job
  :send (let [[username session] message]
          (bulk-api/request :create-job-request
                            "job"
                            username
                            {:session session
                             :template "create-job.xml"
                             :data {:object "Case"}}
                            proceed))

  :parse (let [response (:response (last message))
               job-id (bulk-api/parse-and-extract response :id)]
           (proceed job-id))
  :process (let [response (:response (last message))]
             (proceed :create-batch
                             {:job-id response}))
  :error (proceed :finish))

(create-callout-engine :create-batch
  :send (let [[username session] message]
          (bulk-api/request :create-batch-request
                            (str "job/" (:job-id session) "/batch")
                            username
                            {:session session
                             :template "create-batch.sql"
                             :data {:object "Case"
                                    :fields "Subject"
                                    :limit 2}}
                            proceed))

  :parse (let [session (last message)
               response (:response session)
               batch-id (bulk-api/parse-and-extract response :id)]
           (proceed batch-id))

  :process (let [session (last message)
                 response (:response session)
                 prior-batches (:queued-batch-ids session)]
             (proceed :check-batch
                             {:queued-batch-ids (conj prior-batches response)}))
  :error (proceed :close-job))

(create-callout-engine :check-batch
  :send (let [[username session] message]
          ;; TODO figure out way to handle more than one batch
          (bulk-api/request :check-batch-request
                            (str "job/" (:job-id session) "/batch/" (first (:queued-batch-ids session)))
                            username
                            {:session session}
                            proceed))

  :parse (let [session (last message)
               response (:response session)
               batch-state (bulk-api/parse-and-extract response :state)
               batch-id (bulk-api/parse-and-extract response :id)]
           (proceed [batch-id batch-state]))

  :process (let [session (last message)
                 [batch-id batch-state] (:response session)
                 queued-batches (:queued-batch-ids session)
                 completed-batches (:completed-batch-ids session)]
             (if (= "Completed" batch-state)
               (proceed :close-job {:queued-batch-ids (remove #{batch-id} queued-batches)
                                :completed-batch-ids (conj queued-batches batch-id)})
               (proceed :close-job nil)))
  :error (proceed :close-job))

(create-callout-engine :close-job
  :send (let [[username session] message]
          (bulk-api/request :close-job-request
                            (str "job/" (:job-id session))
                            username
                            {:session session
                             :template "close-job.xml"
                             :data {}}
                            proceed))

  :parse (let [response (:response (last message))
               job-id (bulk-api/parse-and-extract response :id)]
           (proceed job-id ))
  :process (let [response (:response (last message))]
             (proceed :drain
                             {:job-id nil}))
  :error (proceed :finish))

(create-callout-engine :drain
  :send (let [[username session] message]
          (info (str "[" username "]") "drain-request make-believe draining collected data to lala-land")
          (proceed "NOOP"))
  :parse (let [[username session] message]
           (info (str "[" username "]") "drain-response make-believe parsing of the reply from lala-land")
           (proceed "NOOP"))
  :process (let [response (:response (last message))]
             (proceed :finish nil))
  :error (proceed :finish))

(create-terminal-engine :finish
  (let [[username session] message]
    (info (str "[" username "]") "all processing finished")))

(create-terminal-engine :trace
  (let [[username session] message
        source (name (:engine session))
        data (or (:response session)
                 message)

        data (or (:body data)
                 data)]
    (info (str "[" username "]") source "produced" (pr-str data))))

(create-terminal-engine :error
  (let [[username session] message
        source (name (:engine session))
        data (or (:response session)
                 session)

        text (if (isa? (class data) Exception)
               (str "caused error " (with-out-str (print-cause-trace data)))
               (str "detected error " (pr-str data)))]
    ;; TODO exctact/output exceptionCode and exceptionMessage tags nicely if available in response xml
    (error (str "[" username "]") source text)))
