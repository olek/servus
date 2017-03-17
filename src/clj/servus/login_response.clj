(ns servus.login-response
  (:require [servus.bulk-api :as bulk-api]
            [servus.engine :as engine]))

(engine/create :login-response
               (let [response (:response (last input-message))
                     data (bulk-api/parse-and-extract response
                                                      :sessionId :serverUrl)]
                 (output-handler {:response nil
                                  :session-id (:sessionId data)
                                  :server-instance (-> #"\w+.salesforce.com"
                                                       (re-find (:serverUrl data)))})))
