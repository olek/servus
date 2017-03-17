(ns servus.create-job-request
  (:require [servus.bulk-api :as bulk-api]
            [servus.engine :as engine]))

(engine/create :create-job-request
               (let [[username session] input-message]
                 (bulk-api/request username session "job" "create-job" {:object "Case"} #(output-handler {:response %}))))
