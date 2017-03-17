(ns servus.close-job-request
  (:require [servus.bulk-api :as bulk-api]
            [servus.engine :as engine]))

(engine/create :close-job-request
               (let [[username session] input-message]
                 (bulk-api/request username session (str "job/" (:job-id session)) "close-job" {} #(output-handler {:response %}))))
