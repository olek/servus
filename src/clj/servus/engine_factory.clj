(ns servus.engine-factory
  (:require [clojure.tools.logging :refer [info warn error fatal]]
            [clojure.core.async :refer [>! <! >!! go-loop chan alts! close!]]
            [clojure.stacktrace :refer [print-cause-trace]]
            [mount.core :refer [defstate]]
            [servus.channels :refer [create-channel! close-channel! engine-channel]]))

(defn- distribute [engine message]
  (>!! (engine-channel engine) message))

(defn start-engine [handle local-channels ch engine-fn error-fn]
  (let [ch (if-let [local-ch (get local-channels ch)]
             local-ch
             (create-channel! ch))
        stop-channel (chan)
        error-callback (fn [e message target]
                         (>!! (engine-channel :error) (update-in message [1] (partial merge {:engine handle
                                                                                             :response e})))
                         (>!! (engine-channel target) (update-in message [1] dissoc :response)))]
    (go-loop [input-message :start]
      (condp = input-message
        :start
        (info "Waiting for requests to" handle "engine...")

        :stop-engine
        (info "Not waiting for requests to" handle "engine anymore, exiting")

        nil
        (info "Duh, done with the" handle)

        (let [response (:response (last input-message))
              error? (or (isa? (class response) Exception)
                         (and (:status response)
                              (> (:status response) 299)))
              protected-error-fn (fn [f]
                                   (try
                                     (error-fn f)
                                     (catch Exception e
                                       ;; Processing is interrupted, this should really never happen.
                                       (fatal (str "Caught totally unexpected exception in " (name handle) ":")
                                              (with-out-str (print-cause-trace e))))))]
          (if (and error?
                   error-fn)
            (protected-error-fn (partial error-callback response input-message))
            (try
              (engine-fn input-message)
              (catch Exception e
                (error (str "Caught exception in " (name handle) ":") (str e))
                (when error-fn
                  (protected-error-fn (partial error-callback e input-message))))))))
      (when (and input-message
                 (not= :stop-engine input-message))
        (recur (first (alts! [stop-channel ch] :priority true)))))
    stop-channel))

(defn start-send-endpoint-engine [handle send-handle local-channels engine-fn error-fn]
  (start-engine send-handle
                local-channels
                handle
                (fn [input-message]
                  (let [output-handler (fn [response]
                                                 (info (str "[" (first input-message) "]") (name send-handle) "returned" (pr-str (:body response)))
                                                 (>!! (local-channels :raw-response)
                                                      (update-in input-message [1 :response] (constantly response))))]
                    (engine-fn input-message output-handler)))
                error-fn))

(defn start-parse-endpoint-engine [handle parse-handle local-channels engine-fn error-fn]
  (start-engine parse-handle
                local-channels
                :raw-response
                (fn [input-message]
                  (let [output-handler (fn [response]
                                         (info (str "[" (first input-message) "]") (name parse-handle) "returned" (pr-str response))
                                         (>!! (local-channels :parsed-response)
                                              (update-in input-message [1 :response] (constantly response))))]
                    (engine-fn input-message output-handler)))
                error-fn))

(defn start-process-endpoint-engine [handle process-handle local-channels engine-fn error-fn]
  (start-engine process-handle
                local-channels
                :parsed-response
                (fn [input-message]
                  (let [output-handler (fn [target response]
                                         (info (str "[" (first input-message) "]") (name process-handle) "returned" (pr-str response))
                                         (>!! (engine-channel target) (update-in input-message [1] (comp (partial merge response)
                                                                                                         #(dissoc % :response)))))]
                    (engine-fn input-message output-handler)))
                error-fn))

(defn stop-engine
  ([handle control-ch]
   (>!! control-ch :stop-engine))
  ([handle control-ch input-ch]
   (stop-engine handle control-ch)
   (close-channel! input-ch)))

(defmacro create-terminal-engine [handle & code]
  (let [engine-name (gensym (str "engine-" (name handle)))]
    `(defstate ^:private ~engine-name
       :start
       (start-engine ~handle
                     nil
                     ~handle
                     (fn [~'input-message] ~@code)
                     nil)

       :stop
       (stop-engine ~handle ~engine-name ~handle))))

(defmacro create-callout-engine [handle & options]
  (let [code (apply hash-map options)
        handle-str (name handle)
        engine-channels-name (gensym (str "engine-" handle-str "-channels"))
        send-handle (keyword (str handle-str "-send"))
        parse-handle (keyword (str handle-str "-parse"))
        process-handle (keyword (str handle-str "-process"))
        send-engine-name (gensym (str "engine-" handle-str "-send"))
        parse-engine-name (gensym (str "engine-" handle-str "-parse"))
        process-engine-name (gensym (str "engine-" handle-str "-process"))]
    `(do
       (defstate ^:private ~engine-channels-name
         :start
         {:raw-response (chan)
          :parsed-response (chan)}

         :stop
         (doseq [ch# (vals ~engine-channels-name)]
           (close! ch#)))

       (defstate ^:private ~send-engine-name
         :start
         (start-send-endpoint-engine ~handle
                                     ~send-handle
                                     ~engine-channels-name
                                     (fn [~'input-message ~'output-handler]
                                       ~(:send code))
                                     (fn [~'output-handler] ~(:error code)))
         :stop
         (stop-engine ~send-handle ~send-engine-name ~handle))

       (defstate ^:private ~parse-engine-name
         :start
         (start-parse-endpoint-engine ~handle
                                      ~parse-handle
                                      ~engine-channels-name
                                      (fn [~'input-message ~'output-handler]
                                        ~(:parse code))
                                      (fn [~'output-handler] ~(:error code)))
         :stop
         (stop-engine ~parse-handle ~parse-engine-name))

       (defstate ^:private ~process-engine-name
         :start
         (start-process-endpoint-engine ~handle
                                        ~process-handle
                                        ~engine-channels-name
                                        (fn [~'input-message ~'output-handler]
                                          (let [~'channels engine-channel]
                                            ~(:process code)))
                                        (fn [~'output-handler] ~(:error code)))
         :stop
         (stop-engine ~process-handle ~process-engine-name)))))
