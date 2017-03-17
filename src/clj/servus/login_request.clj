(ns servus.login-request
  (:require [servus.bulk-api :as bulk-api]
            [servus.engine :as engine]))

(engine/create :login-request
               (let [[username {:keys [password]}] input-message]
                 (bulk-api/login-request username password #(output-handler {:response %}))))
