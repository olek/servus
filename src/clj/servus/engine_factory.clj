(ns servus.engine-factory
  (:require [clojure.tools.logging :refer [info error fatal]]
            [clojure.core.async :refer [>!! go-loop chan alts! close!]]
            [clojure.stacktrace :refer [print-cause-trace]]
            [mount.core :refer [defstate]]
            [servus.channels :refer [create-channel! close-channel! engine-channel]]
            [servus.salesforce-api :as sf-api]
            [servus.store :as store]))

(defn start-message-loop [handle ch engine-fn error-fn]
  (let [stop-channel (chan)]
  (go-loop [message :start]
    (condp = message
      :start
      (info "Waiting for requests to" handle "engine...")

      :stop-message-loop
      (info "Not waiting for requests to" handle "engine anymore, exiting")

      nil
      (info "Duh, done with the" handle)

      (try
        (apply engine-fn message)
        (catch Exception e
          (error (str "Caught exception in " (name handle) ":") (str e))
          (when error-fn
            (try
              (apply error-fn e message)
              (catch Exception e
                ;; Processing is interrupted, this should really never happen.
                (fatal (str "Caught totally unexpected exception in " (name handle) ":")
                       (with-out-str (print-cause-trace e)))))))))
    (if (and message
             (not= :stop-message-loop message))
      (recur (first (alts! [stop-channel ch] :priority true)))
      (close! stop-channel)))
  stop-channel))

(def ^:private request-map {:login sf-api/login-request
                            :data-request sf-api/data-request
                            :bulk-request sf-api/bulk-request})

;; TODO 'process' transitions should not be executed immediately because of the potential errors aborting
;; processing; they should be 'enqueued' only after 'process' code is finished.
(defn transition-raw
  ([username from to]
   (transition-raw username from to nil))
  ([username from to data]
   (info (str "[" username "]")
         (name from) "transitions to" to
         "with" (pr-str data))
   (>!! (engine-channel to) [username data])))

(defn wrap-error-fn [from error-fn value username message]
  (>!! (engine-channel :error) [username {:message (dissoc message :response)
                                          :engine from
                                          :error value}])
  (error-fn username
            message
            (store/session username)
            value
            (partial transition-raw username from)
            (partial store/update-session username)))

(defn start-send-message-loop [loop-handle channel-name local-channels engine-fn error-fn]
  (start-message-loop loop-handle
                      (create-channel! channel-name)
                      (fn [username message]
                        (let [session (store/session username)
                              callback (fn [response]
                                         (info (str "[" username "]") (name loop-handle) "returned" (pr-str (:body response)))
                                         (>!! (local-channels :raw-response)
                                              [username (update message :response (constantly response))]))
                              data (engine-fn username message session callback)]
                          (when data
                            (let [request (first data)
                                  args (concat [channel-name username (:session-id session) (:server-instance session)] (rest data) [callback])]
                              (apply (request-map request) args)))))
                      (partial wrap-error-fn loop-handle error-fn)))

(defn start-parse-message-loop [loop-handle local-channels engine-fn error-fn]
  (let [wrapped-error-fn (partial wrap-error-fn loop-handle error-fn)]
    (start-message-loop loop-handle
                        (local-channels :raw-response)
                        (fn [username message]
                          (let [http-response (:response message)
                                error? (> (:status http-response) 299)
                                response (when-not error?
                                           (engine-fn username message (store/session username)
                                                      (:response message)))]
                            (if error?
                              (wrapped-error-fn http-response username message)
                              (do
                                (info (str "[" username "]") (name loop-handle) "returned" (pr-str response))
                                (>!! (local-channels :parsed-response)
                                     [username (update message :response (constantly response))])))))
                        wrapped-error-fn)))

(defn start-process-message-loop [loop-handle local-channels engine-fn error-fn]
  (start-message-loop loop-handle
                      (local-channels :parsed-response)
                      (fn [username message]
                        ;; TODO figure out how to avoid locking of the go-loop, it is not good.
                        (locking (store/session-atom username)
                          (engine-fn username
                                     message
                                     (store/session username)
                                     (:response message)
                                     (partial transition-raw username loop-handle)
                                     (partial store/update-session username))))
                      (partial wrap-error-fn loop-handle error-fn)))

(defn stop-message-loop
  ([control-ch]
   (>!! control-ch :stop-message-loop))
  ([control-ch input-ch]
   (stop-message-loop control-ch)
   (close-channel! input-ch)))

(defmacro create-terminal-engine [handle & code]
  (let [engine-name (gensym (str "engine-" (name handle)))]
    `(defstate ^:private ~engine-name
       :start
       (start-message-loop ~handle
                                  (create-channel! ~handle)
                                  (fn [~'username ~'message] ~@code)
                                  nil)

       :stop
       (stop-message-loop ~engine-name ~handle))))

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
           (start-send-message-loop ~send-handle
                                    ~handle
                                    ~engine-channels-name
                                    (fn [~'username ~'message ~'session ~'callback]
                                      ~(:send code))
                                    (fn [~'username ~'message ~'session ~'error ~'transition-to ~'update-session]
                                      ~(:error code)))
           :stop
           (stop-message-loop ~send-engine-name ~handle))

         (defstate ^:private ~parse-engine-name
           :start
           (start-parse-message-loop ~parse-handle
                                     ~engine-channels-name
                                     (fn [~'username ~'message ~'session ~'response]
                                       ~(:parse code))
                                     (fn [~'username ~'message ~'session ~'error ~'transition-to ~'update-session]
                                       ~(:error code)))
           :stop
           (stop-message-loop ~parse-engine-name))

         (defstate ^:private ~process-engine-name
           :start
           (start-process-message-loop ~process-handle
                                       ~engine-channels-name
                                       (fn [~'username ~'message ~'session ~'response ~'transition-to ~'update-session]
                                         (let [~'channels engine-channel]
                                           ~(:process code)))
                                       (fn [~'username ~'message ~'session ~'error ~'transition-to ~'update-session]
                                         ~(:error code)))
           :stop
           (stop-message-loop ~process-engine-name)))))
