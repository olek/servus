(ns servus.engines
  (:require [clojure.core.async :refer [<! go timeout]]
            [clojure.stacktrace :refer [print-cause-trace]]
            [clojure.tools.logging :refer [info warn error]]
            [clojure.string :as s]
            [servus.bulk-api :as bulk-api]
            [servus.engine-factory :refer [create-terminal-engine create-callout-engine]]))

(create-callout-engine :login
  :send (let [[username {:keys [password]}] message]
          (bulk-api/login-request :login-request username password callback))
  :parse (let [response (:response (last message))
               data (bulk-api/parse-and-extract response
                                                :sessionId :serverUrl)
               session-id (:sessionId data)
               server-instance (-> #"\w+.salesforce.com"
                                   (re-find (:serverUrl data)))]
           [session-id server-instance])
  :process (let [response (:response (last message))]
             (transition-to :create-job
                            {:session-id (first response)
                             :server-instance (last response)}))
  :error (transition-to :finish))

(create-callout-engine :create-job
  :send (let [[username session] message]
          (bulk-api/request :create-job-request
                            "job"
                            username
                            {:session session
                             :template "create-job.xml"
                             :data {:object "Case"}}
                            callback))

  :parse (let [response (:response (last message))
               job-id (bulk-api/parse-and-extract response :id)]
           job-id)
  :process (let [response (:response (last message))]
             (transition-to :create-batch
                            {:job-id response}))
  :error (transition-to :finish))

(create-callout-engine :create-batch
  :send (let [[username session] message]
          (bulk-api/request :create-batch-request
                            (s/join "/" ["job" (:job-id session) "batch"])
                            username
                            {:session session
                             :template "create-batch.sql"
                             :data {:object "Case"
                                    :fields "Subject"
                                    :limit 2}}
                            callback))
  :parse (let [session (last message)
               response (:response session)
               batch-id (bulk-api/parse-and-extract response :id)]
           batch-id)
  :process (let [session (last message)
                 response (:response session)
                 prior-batches (:queued-batch-ids session)]
             (transition-to :check-batch
                            {:queued-batch-ids (conj prior-batches response)}))
  :error (transition-to :close-job {:success false}))

(create-callout-engine :check-batch
  :send (let [[username session] message]
          ;; TODO figure out way to handle more than one batch
          (bulk-api/request :check-batch-request
                            (s/join "/" ["job" (:job-id session) "batch" (first (:queued-batch-ids session))])
                            username
                            {:session session}
                            callback))
  :parse (let [session (last message)
               response (:response session)
               batch-state (bulk-api/parse-and-extract response :state)
               batch-id (bulk-api/parse-and-extract response :id)]
           [batch-id batch-state])
  :process (let [session (last message)
                 [batch-id batch-state] (:response session)
                 queued-batches (:queued-batch-ids session)
                 completed-batches (:completed-batch-ids session)]
             (if (= "Completed" batch-state)
               (transition-to :collect-batch-result-ids {:queued-batch-ids (remove #{batch-id} queued-batches)
                                                         :completed-batch-ids (conj queued-batches batch-id)
                                                         :times-attempted nil})
               (let [times-attempted (get session :times-attempted 1)]
                 (if (< times-attempted 3)
                   (do
                     (warn "Postponing check-batch, attempted" times-attempted "times")
                     (go
                       (<! (timeout 5000))
                       (warn "Retrying check-batch, attempted" times-attempted "times")
                       (transition-to :check-batch {:times-attempted (inc times-attempted)})))
                   (do
                     (warn "Aborting retries of check-batch after" times-attempted "attempts")
                     (transition-to :close-job {:success false
                                                :times-attempted nil}))))))
  :error (transition-to :close-job {:success false}))

(create-callout-engine :collect-batch-result-ids
  :send (let [[username session] message]
          ;; TODO figure out way to handle more than one batch
          (bulk-api/request :collect-batch-result-ids
                            (s/join "/" ["job" (:job-id session) "batch" (first (:completed-batch-ids session)) "result"])
                            username
                            {:session session}
                            callback))
  :parse (let [session (last message)
               response (:response session)
               batch-id (first (:completed-batch-ids session))
               result-id (bulk-api/parse-and-extract response :result)]
           [batch-id result-id])
  :process (let [session (last message)
                 [batch-id result-id] (:response session)]
             (transition-to :collect-batch-result {:result-id result-id}))
  :error (transition-to :close-job {:success false}))

(create-callout-engine :collect-batch-result
  :send (let [[username session] message]
          ;; TODO figure out way to handle more than one batch
          (bulk-api/request :collect-batch-result
                            (s/join "/" [ "job" (:job-id session) "batch" (first (:completed-batch-ids session)) "result" (:result-id session)])
                            username
                            {:session session}
                            callback))
  :parse (let [session (last message)
               response (:response session)
               batch-id (first (:completed-batch-ids session))
               csv-text (:body response)]
           [batch-id csv-text])
  :process (let [session (last message)
                 [batch-id csv-text] (:response session)]
             (transition-to :close-job {:result-id nil}))
  :error (transition-to :close-job {:success false}))

(create-callout-engine :close-job
  :send (let [[username session] message]
          (bulk-api/request :close-job-request
                            (str "job/" (:job-id session))
                            username
                            {:session session
                             :template "close-job.xml"
                             :data {}}
                            callback))

  :parse (let [response (:response (last message))
               job-id (bulk-api/parse-and-extract response :id)]
           job-id)
  :process (let [session (last message)
                 response (:response session)
                 success? (not= false (:success session))]
             (if success?
               (transition-to :drain {:job-id nil})
               (transition-to :finish {:job-id nil})))
  :error (transition-to :finish))

(create-callout-engine :drain
  :send (let [[username session] message]
          (info (str "[" username "]") "drain-request make-believe draining collected data to lala-land")
          (callback "NOOP"))
  :parse (let [[username session] message]
           (info (str "[" username "]") "drain-response make-believe parsing of the reply from lala-land")
           "NOOP")
  :process (let [response (:response (last message))]
             (transition-to :finish))
  :error (transition-to :finish))

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
