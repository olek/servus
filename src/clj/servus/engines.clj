(ns servus.engines
  (:require [clojure.core.async :refer [<! go timeout]]
            [clojure.stacktrace :refer [print-cause-trace]]
            [clojure.tools.logging :refer [info warn error]]
            [clojure.string :as s]
            [servus.salesforce-api :as sf-api]
            [servus.engine-factory :refer [create-terminal-engine create-callout-engine]]))

(create-callout-engine :login
  :send (let [[username {:keys [password]}] message]
          [:login password])
  :parse (let [response (:response (last message))
               data (sf-api/parse-and-extract response
                                              :sessionId :serverUrl)
               session-id (:sessionId data)
               server-instance (-> #"\w+.salesforce.com"
                                   (re-find (:serverUrl data)))]
           [session-id server-instance])
  :process (let [response (:response (last message))
                 data {:session-id (first response)
                       :server-instance (last response)}]
             (transition-to :count-records data)
             (transition-to :create-job data))
  :error (transition-to :finish))

(create-callout-engine :count-records
  :send (let [[username session] message]
          [:data-request
           (str "query?q=select+count()+from+case")
           nil])
  :parse (let [response (:response (last message))
               total-size (sf-api/parse-and-extract response :totalSize)]
           total-size)
  :process nil
  :error nil)

(create-callout-engine :create-job
  :send (let [[username session] message]
          [:bulk-request
           "job"
           {:template "create-job.xml"
            :data {:object "Case"}}])

  :parse (let [response (:response (last message))
               job-id (sf-api/parse-and-extract response :id)]
           job-id)
  :process (let [response (:response (last message))]
             (transition-to :create-batch
                            {:job-id response}))
  :error (transition-to :finish))

(create-callout-engine :create-batch
  :send (let [[username session] message]
          [:bulk-request
           (s/join "/" ["job" (:job-id session) "batch"])
           {:template "create-batch.sql"
            :data {:object "Case"
                   :fields "Subject"
                   :limit 2}}])
  :parse (let [session (last message)
               response (:response session)
               batch-id (sf-api/parse-and-extract response :id)]
           batch-id)
  :process (let [session (last message)
                 response (:response session)
                 prior-batches (:queued-batch-ids session #{})]
             (transition-to :close-job)
             (transition-to :check-batches)
             #_(transition-to :check-batch
                            {:queued-batch-ids (conj prior-batches response)}))
  :error (transition-to :close-job))

(create-callout-engine :check-batches
  :send (let [[username session] message]
          [:bulk-request
           (s/join "/" ["job" (:job-id session) "batch"])
           nil])
  :parse (let [session (last message)
               response (:response session)
               batch-state (sf-api/parse-and-extract-all response :id :state)]
           batch-state)
  :process (let [session (last message)
                 response (:response session)
                 completed-batches (:completed-batch-ids session)]
             (doseq [[batch-id batch-status] response]
               (when (and (= batch-status "Completed")
                          (not (some #{batch-id} completed-batches)))
                 (transition-to :collect-batch-result-ids {:completed-batch-ids (conj completed-batches batch-id)
                                                           :times-attempted nil})))
             (when (some #{"Queued"} (map last response))
               (let [times-attempted (get session :times-attempted 1)]
                 (if (< times-attempted 3)
                   (do
                     (warn "Postponing check-batches, attempted" times-attempted "times")
                     (go
                       (<! (timeout 5000))
                       (warn "Retrying check-batches, attempted" times-attempted "times")
                       (transition-to :check-batches {:times-attempted (inc times-attempted)})))
                   (do
                     (warn "Aborting retries of check-batches after" times-attempted "attempts")
                     (transition-to :finish {:times-attempted nil}))))))
  :error (transition-to :finish))

;(create-callout-engine :check-batch
;  :send (let [[username session] message]
;          ;; TODO figure out way to handle more than one batch
;          [:bulk-request
;           (s/join "/" ["job" (:job-id session) "batch" (first (:queued-batch-ids session))])
;           nil])
;  :parse (let [session (last message)
;               response (:response session)
;               batch-state (sf-api/parse-and-extract response :state)
;               batch-id (sf-api/parse-and-extract response :id)]
;           [batch-id batch-state])
;  :process (let [session (last message)
;                 [batch-id batch-state] (:response session)
;                 queued-batches (:queued-batch-ids session #{})
;                 completed-batches (:completed-batch-ids session #{})]
;             (condp = batch-state
;               "Completed"
;               (transition-to :collect-batch-result-ids {:queued-batch-ids (disj queued-batches #{batch-id})
;                                                         :completed-batch-ids (conj queued-batches batch-id)
;                                                         :times-attempted nil})
;               "Not Processed"
;               (comment "PK chunking enabled, real work done in extra batches")

;               "Queued"
;               (let [times-attempted (get session :times-attempted 1)]
;                 (if (< times-attempted 3)
;                   (do
;                     (warn "Postponing check-batch, attempted" times-attempted "times")
;                     (go
;                       (<! (timeout 5000))
;                       (warn "Retrying check-batch, attempted" times-attempted "times")
;                       (transition-to :check-batch {:times-attempted (inc times-attempted)})))
;                   (do
;                     (warn "Aborting retries of check-batch after" times-attempted "attempts")
;                     (transition-to :close-job {:times-attempted nil}))))

;               ;;"Failed" "InProgress"
;               (transition-to :finish {:times-attempted nil})))
;  :error (transition-to :finish))

(create-callout-engine :collect-batch-result-ids
  :send (let [[username session] message]
          ;; TODO figure out way to handle more than one batch
          [:bulk-request
           (s/join "/" ["job" (:job-id session) "batch" (first (:completed-batch-ids session)) "result"])
           nil])
  :parse (let [session (last message)
               response (:response session)
               batch-id (first (:completed-batch-ids session))
               result-id (sf-api/parse-and-extract response :result)]
           [batch-id result-id])
  :process (let [session (last message)
                 [batch-id result-id] (:response session)]
             (transition-to :collect-batch-result {:result-id result-id}))
  :error (transition-to :finish))

(create-callout-engine :collect-batch-result
  :send (let [[username session] message]
          ;; TODO figure out way to handle more than one batch
          [:bulk-request
           (s/join "/" [ "job" (:job-id session) "batch" (first (:completed-batch-ids session)) "result" (:result-id session)])
           nil])
  :parse (let [session (last message)
               response (:response session)
               batch-id (first (:completed-batch-ids session))
               csv-text (:body response)]
           [batch-id csv-text])
  :process (let [session (last message)
                 [batch-id csv-text] (:response session)]
             (transition-to :drain {:result-id nil}))
  :error (transition-to :finish))

(create-callout-engine :close-job
  :send (let [[username session] message]
          [:bulk-request
           (str "job/" (:job-id session))
           {:template "close-job.xml"
            :data {}}])

  :parse (let [response (:response (last message))
               job-id (sf-api/parse-and-extract response :id)]
           job-id)
  :process nil
  :error nil)

(create-callout-engine :drain
  :send (let [[username session] message]
          (info (str "[" username "]") "drain-request make-believe draining collected data to lala-land")
          nil)
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
