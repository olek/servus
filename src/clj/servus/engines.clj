(ns servus.engines
  (:require [clojure.core.async :refer [<! go timeout]]
            [clojure.stacktrace :refer [print-cause-trace]]
            [clojure.tools.logging :refer [info warn error]]
            [clojure.set :as set]
            [clojure.string :as s]
            [clojure-csv.core :refer [parse-csv]]
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
             (transition-to :create-job))
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
             (transition-to :create-planning-batch))
  :error (transition-to :finish))

(create-callout-engine :create-planning-batch
  :send [:bulk-request
         (s/join "/" ["job" (:job-id session) "batch"])
         ;; rename sql to soql
         {:template "create-planning-batch.sql"
          :data {:object "Case"}}]
  :parse (sf-api/parse-and-extract response :id)
  :process (do
             (update-session #(merge % {:stage :planning
                                        :planning-batch-id response}))
             (transition-to :check-batch {:batch-id response}))
  :error (transition-to :close-job))

(create-callout-engine :create-batch
  :send (let [planning-markers (:planning-markers session)
              from (first planning-markers)
              to (second planning-markers)]
          [:bulk-request
           (s/join "/" ["job" (:job-id session) "batch"])
           ;; rename sql to soql
           {:template "create-batch.sql"
            :data {:object "Case"
                   :fields "Subject"
                   :from from
                   :to to
                   :limit 100}}])
  :parse (sf-api/parse-and-extract response :id)
  :process (do
             (update-session (comp #(update % :queued-batch-ids set/union #{response})
                                   #(update % :planning-markers (partial drop 1))))
             (transition-to :check-batches))
  :error (transition-to :close-job))

(create-callout-engine :check-batches
  :send [:bulk-request
         (s/join "/" ["job" (:job-id session) "batch"])
         nil]
  :parse (sf-api/parse-and-extract-all response :id :state)
  :process (let [planning-markers (:planning-markers session)
                 prev-completed-batches (:completed-batch-ids session)
                 planning-batch-id (:planning-batch-id session)
                 all-completed-batches (->> response
                                            (filter #(= "Completed" (last %)))
                                            (map first)
                                            set)
                 new-completed-batches (set/difference all-completed-batches prev-completed-batches #{planning-batch-id})]
             (update-session #(-> %
                                  (update :completed-batch-ids set/union new-completed-batches)
                                  (update :queued-batch-ids set/difference new-completed-batches)))
             ;; TODO track/report batches in "Failed" state
             (doseq [batch-id new-completed-batches]
               (transition-to :collect-batch-result-ids {:batch-id batch-id}))
             ;; TODO fix not fetching last block of ids
             ;; TODO fix very sequential nature of batches
             ;; TODO add code to auto-adjust batch size based on previous batches duration
             (if (> (count planning-markers) 1)
               (transition-to :create-batch)
               (transition-to :close-job))
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

(create-callout-engine :check-batch
  :send [:bulk-request
         (s/join "/" ["job" (:job-id session) "batch" (:batch-id message)])
         nil]
  :parse (sf-api/parse-and-extract response :state)
  :process (condp = response
             "Completed"
             (transition-to :collect-batch-result-ids message)

             "Queued"
             (let [times-attempted (get session :times-attempted 1)]
               (if (< times-attempted 3)
                 (do
                   (warn "Postponing check-batch, attempted" times-attempted "times")
                   (go
                     (<! (timeout 5000))
                     (warn "Retrying check-batch, attempted" times-attempted "times")
                     (transition-to :check-batch (assoc message :times-attempted (inc times-attempted)))))
                 (do
                   (warn "Aborting retries of check-batch after" times-attempted "attempts")
                   (transition-to :close-job))))

             ;;"Failed" "InProgress" "Not Processed"
             (do
               (warn "Unexpected batch state" response "- aborting")
               (transition-to :close-job)))
  :error (transition-to :close-job))

(create-callout-engine :collect-batch-result-ids
  :send [:bulk-request
         (s/join "/" ["job" (:job-id session) "batch" (:batch-id message) "result"])
         nil]
  :parse (sf-api/parse-and-extract response :result)
  :process (transition-to :collect-batch-result {:batch-id (:batch-id message)
                                                 :result-id response})
  :error (transition-to :finish))

(create-callout-engine :collect-batch-result
  :send [:bulk-request
         (s/join "/" [ "job" (:job-id session) "batch" (:batch-id message) "result" (:result-id message)])
         nil]
  :parse (let [csv-text (:body response)
               csv-data (parse-csv csv-text)]
           (info "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD" (pr-str csv-data))
           csv-data)
  :process (if (= :planning (:stage session))
             (do
               ;; TODO fix not fetching last block of ids
               (update-session #(merge % {:stage :fetching
                                          :planning-markers (->> response
                                                                 rest
                                                                 (map first)
                                                                 (take-nth 2))}))
               (transition-to :create-batch))
             ;; TODO fix multiple draining
             (transition-to :drain))
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
  ;; TODO erase session on completion
  (info (str "[" username "]") "all processing finished"))

;(create-terminal-engine :trace
;  (let [[username data] message
;        source (name (:engine message-data))]
;    (info (str "[" username "]") source "produced" (pr-str data))))

(create-terminal-engine :error
  (let [source (name (:engine message))
        value (:error message)
        failed-message (:message message)

        text (if (isa? (class value) Exception)
               (str "caused error " (with-out-str (print-cause-trace value)))
               (str "detected error " (pr-str value)))]
    ;; TODO extract/output exceptionCode and exceptionMessage tags nicely if available in response xml
    (error (str "[" username "]") source (pr-str failed-message) text)))
