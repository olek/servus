(ns servus.close-job-response
  (:require [mount.core :refer [defstate]]
            [servus.bulk-api :as bulk-api]
            [servus.engine :as engine]))

(engine/create :close-job-response
               (let [response (:response (last input-message))]
                 (output-handler {:response nil
                                  :job-id (bulk-api/parse-and-extract response :id)})))
