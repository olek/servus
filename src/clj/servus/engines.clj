(ns servus.engines
  (:require [clojure.core.async :refer [<! go timeout]]
            [clojure.stacktrace :refer [print-cause-trace]]
            [clojure.tools.logging :refer [info warn error]]
            [clojure.set :as set]
            [clojure.string :as s]
            [servus.salesforce-api :as sf-api]
            [servus.engine-factory :refer [create-terminal-engine create-callout-engine]]))

(create-callout-engine :login
  :send [:login (:password message)]
  :parse (let [data (sf-api/parse-and-extract response
                                              :sessionId :serverUrl)
               session-id (:sessionId data)
               server-instance (-> #"\w+.salesforce.com"
                                   (re-find (:serverUrl data)))]
           [session-id server-instance])
  :process (let [data {:session-id (first response)
                       :server-instance (last response)}]
             (update-session #(merge % data))
             (transition-to :count-records)
             (transition-to :create-job data))
  :error (transition-to :finish))

(create-callout-engine :count-records
  :send [:data-request
         (str "query?q=select+count()+from+case")
         nil]
  :parse (let [total-size (sf-api/parse-and-extract response :totalSize)]
           total-size)
  :process nil
  :error nil)

(create-callout-engine :create-job
  :send [:bulk-request
         "job"
         {:template "create-job.xml"
          :data {:object "Case"}}]

  :parse (let [job-id (sf-api/parse-and-extract response :id)]
           job-id)
  :process (do
             (update-session #(assoc % :job-id response))
             (transition-to :create-batch))
  :error (transition-to :finish))

(create-callout-engine :create-batch
  :send [:bulk-request
         (s/join "/" ["job" (:job-id session) "batch"])
         {:template "create-batch.sql"
          :data {:object "Case"
                 :fields "Subject"
                 :limit 2}}]
  :parse (sf-api/parse-and-extract response :id)
  :process (do
             (update-session #(update % :queued-batch-ids set/union #{response}))
             (transition-to :close-job)
             (transition-to :check-batches))
  :error (transition-to :close-job))

(create-callout-engine :check-batches
  :send [:bulk-request
         (s/join "/" ["job" (:job-id session) "batch"])
         nil]
  :parse (sf-api/parse-and-extract-all response :id :state)
  :process (let [prev-completed-batches (:completed-batch-ids session)
                 all-completed-batches (->> response
                                            (filter #(= "Completed" (last %)))
                                            (map first)
                                            set)
                 new-completed-batches (set/difference all-completed-batches prev-completed-batches)]
             (update-session #(-> %
                                  (update :completed-batch-ids set/union new-completed-batches)
                                  (update :queued-batch-ids set/difference new-completed-batches)))
             (doseq [batch-id new-completed-batches]
               (transition-to :collect-batch-result-ids {:batch-id batch-id}))
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
                     (transition-to :finish))))))
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
  :send [:bulk-request
         (s/join "/" ["job" (:job-id session) "batch" (:batch-id message) "result"])
         nil]
  :parse (let [batch-id (:batch-id message)
               result-id (sf-api/parse-and-extract response :result)]
           [batch-id result-id])
  :process (let [[batch-id result-id] response]
             (transition-to :collect-batch-result {:batch-id batch-id
                                                   :result-id result-id}))
  :error (transition-to :finish))

(create-callout-engine :collect-batch-result
  :send [:bulk-request
         (s/join "/" [ "job" (:job-id session) "batch" (:batch-id message) "result" (:result-id message)])
         nil]
  :parse (let [batch-id (:batch-id message)
               result-id (:result-id message)
               csv-text (:body response)]
           [batch-id result-id csv-text])
  :process (transition-to :drain)
  :error (transition-to :finish))

(create-callout-engine :close-job
  :send [:bulk-request
         (str "job/" (:job-id session))
         {:template "close-job.xml"
          :data {}}]

  :parse (sf-api/parse-and-extract response :id)
  :process nil
  :error nil)

(create-callout-engine :drain
  :send (info (str "[" username "]") "drain-request make-believe draining collected data to lala-land")
  :parse (info (str "[" username "]") "drain-response make-believe parsing of the reply from lala-land")
  :process (transition-to :finish)
  :error (transition-to :finish))

(create-terminal-engine :finish
  (info (str "[" username "]") "all processing finished"))

;(create-terminal-engine :trace
;  (let [[username data] message
;        source (name (:engine message-data))]
;    (info (str "[" username "]") source "produced" (pr-str data))))

(create-terminal-engine :error
  (let [source (name (:engine message))
        response (or (:response message)
                     message)

        text (if (isa? (class response) Exception)
               (str "caused error " (with-out-str (print-cause-trace response)))
               (str "detected error " (pr-str response)))]
    ;; TODO extract/output exceptionCode and exceptionMessage tags nicely if available in response xml
    (error (str "[" username "]") source text)))
