(ns servus.engine-factory
  (:require [clojure.tools.logging :refer [info warn error]]
            [clojure.core.async :refer [>! <! >!! go-loop chan alts!]]
            [mount.core :refer [defstate]]
            [servus.channels :refer [create-channel! close-channel! engine-channel manifold-channel]]))

(defn start-engine [handle engine-fn]
  (let [_ (create-channel! handle)
        ch (engine-channel handle)
        stop-channel (chan)
        output-handler (fn self
                          ([input-message response]
                           (self input-message response {}))
                          ([[username session] response session-overrides]
                           (>!! (manifold-channel) [handle [username (merge session
                                                                              session-overrides
                                                                              {:response response})]])))]
    ;; TODO catch all errors in go-loop
    (go-loop [input-message :start]
      (condp = input-message
        :start
        (info "Waiting for requests to" handle "engine...")

        :stop-engine
        (info "Not waiting for requests to" handle "engine anymore, exiting")

        nil
        (info "Duh, done with the" handle)

        (try
          (engine-fn input-message (partial output-handler input-message))
          (catch Exception e
            (error "Caught exception:" (str e))
            (output-handler input-message e))))
      (when (and input-message
                 (not= :stop-engine input-message))
        (recur (first (alts! [stop-channel ch] :priority true)))))
    stop-channel))

(defn stop-engine [handle ch]
  (>!! ch :stop-engine)
  (close-channel! handle))

(defmacro create-engine [handle & code]
  (let [engine-name (gensym (str "engine-" (name handle)))]
    `(defstate ^:private ~engine-name
       :start
       (start-engine ~handle (fn [~'input-message ~'output-handler] ~@code))

       :stop
       (stop-engine ~handle ~engine-name))))

;(defn create-super-engine [handle opts]
;  (let [send-fn (:send opts)
;        parse-fn (:parse opts)
;        process-fn (:process opts)
;        send-handle (keyword (str (name handle) "-request"))
;        parse-fn (keyword (str (name handle) "-response"))
;        process-fn (keyword (str (name handle) "-process"))
;        ]
;    (create-engine send-handle))
;  )
