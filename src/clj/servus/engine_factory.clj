(ns servus.engine-factory
  (:require [clojure.tools.logging :refer [info warn error]]
            [clojure.core.async :refer [>! <! >!! go-loop chan alts!]]
            [mount.core :refer [defstate]]
            [servus.channels :refer [create-channel! close-channel! engine-channel manifold-channel]]))

(defn- distribute [engine message]
  (>!! (engine-channel engine) message))

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
          (engine-fn input-message)
          (catch Exception e
            (error (str "Caught exception in " (name handle) ":") (str e))
            (output-handler input-message e))))
      (when (and input-message
                 (not= :stop-engine input-message))
        (recur (first (alts! [stop-channel ch] :priority true)))))
    stop-channel))

(defn stop-engine [handle ch]
  (>!! ch :stop-engine)
  (close-channel! handle))

(defmacro create-terminal-engine [handle & code]
  (let [engine-name (gensym (str "engine-" (name handle)))]
    `(defstate ^:private ~engine-name
       :start
       (start-engine ~handle (fn [~'input-message] ~@code))

       :stop
       (stop-engine ~handle ~engine-name))))

(defmacro create-callout-engine [handle & options]
  (let [code (apply hash-map options)
        handle-str (name handle)
        send-handle (keyword (str handle-str "-request"))
        parse-handle (keyword (str handle-str "-response"))
        process-handle (keyword (str handle-str "-process"))
        send-engine-name (gensym (str "engine-" handle-str "-request"))
        parse-engine-name (gensym (str "engine-" handle-str "-response"))
        process-engine-name (gensym (str "engine-" handle-str "-process"))]
    `(do
       (defstate ^:private ~send-engine-name
         :start
         (start-engine ~send-handle
                       (fn [~'input-message]
                         (let [~'output-handler (fn [response#]
                                                  (info (str "[" (first ~'input-message) "]") (name ~send-handle) "returned" (pr-str (:body response#)))
                                                  (>!! (engine-channel ~parse-handle)
                                                       (update-in ~'input-message [1 :response] (constantly response#))))]
                           ~(:send code))))

         :stop
         (stop-engine ~send-handle ~send-engine-name))

       (defstate ^:private ~parse-engine-name
         :start
         (start-engine ~parse-handle
                       (fn [~'input-message]
                         (let [~'output-handler (fn [response#]
                                                  (info (str "[" (first ~'input-message) "]") (name ~parse-handle) "returned" (pr-str response#))
                                                  (>!! (engine-channel ~process-handle)
                                                       (update-in ~'input-message [1 :response] (constantly response#))))]
                           ~(:parse code))))

         :stop
         (stop-engine ~parse-handle ~parse-engine-name))

       (defstate ^:private ~process-engine-name
         :start
         (start-engine ~process-handle
                       (fn [~'input-message]
                         (let [~'output-handler (fn [response#]
                                                  (info (str "[" (first ~'input-message) "]") (name ~process-handle) "returned" (pr-str response#))
                                                  (>!! (manifold-channel) [~process-handle (update-in ~'input-message [1] (comp (partial merge response#)
                                                                                                                                #(dissoc % :response)))]))]
                           ~(:process code))))

         :stop
         (stop-engine ~process-handle ~process-engine-name)))))
