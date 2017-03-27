(ns servus.channels
  (:require [clojure.core.async :refer [<! >! >!! chan close! mult tap go-loop go timeout alts!]]
            [clojure.stacktrace :refer [print-cause-trace]]
            [clojure.tools.logging :refer [info warn error]]
            [environ.core :refer [env]]
            [mount.core :refer [defstate]]))

(defstate ^:private channels
  :start
  (atom {:manifold (chan)})

  :stop
  (close! (:manifold @channels)))

(defn create-channel! [engine]
  (let [ch (chan)]
    (swap! channels
           merge
           {(keyword engine) ch})
    ch))

(defn engine-channel [engine]
  (or
    (get @channels (keyword engine))
    (throw (ex-info (str "No channel for engine " (pr-str engine)) {}))))

(defn close-channel! [engine]
  (when-let [ch (engine-channel engine)]
    (close! ch)
    (swap! channels
           dissoc
           (keyword engine))))

(defn manifold-channel []
  (@channels :manifold))

(def ^:private routing-chain [:login
                              :create-job
                              :create-batch
                              :check-batch
                              :close-job
                              :drain
                              :finish])

(defn- chain-route [source message]
  (let [chain-map (->> routing-chain
                       rest
                       (zipmap routing-chain))]
    [(chain-map source) message]))

(defn- update-message [message field update-fn]
  (update-in message [1 field] update-fn))

(defn- special-route [source message]
  (cond
    (and (= source :check-batch-process)
         (= "Queued" (get-in message [1 :response])))
    (let [times-attempted (get-in message [1 :times-attempted] 1)]
      (if (< times-attempted 3)
        (do
          (warn "Postponing " source "- attempted" times-attempted "times")
          (go
            (<! (timeout 5000))
            (warn "Retrying " source "- attempted" times-attempted "times")
            ;; TODO pushing in previous engine is super-ugly, fix it
            (>! (manifold-channel) [:create-batch-process (update-message message :times-attempted (fnil inc 1))]))
          [:finish message])
        (do (warn "Aborting retries of" source "after" times-attempted "attempts")
          [:close-job-request [(first message) (dissoc (last message) :times-attempted)]])))

    :else nil))

(defn- route [source message]
  (or (special-route source message)
      (chain-route source message)))

(defn- start-manifold-engine []
  (let [
        ch (:manifold @channels)
        stop-channel (chan)]
    ;; TODO catch all errors in go-loop
    (go-loop [input-message :start]
      (try
        (condp = input-message
          :start
          (info "Waiting for requests in manifold...")

          :stop-engine
          (info "Not waiting for requests in manifold anymore, exiting")

          nil
          (info "Duh, manifold processing done")

          (let [[source message] input-message
                [target message] (route source message)
                response (:response (last message))
                ;; skip parsing response if exception was raised while processing request
                error? (or (isa? (class response) Exception)
                           (and (:status response)
                                (> (:status response) 299)))
                [target message] (if (and error?
                                          (.endsWith (name target) "-response"))
                                   (route (route target message)) ; skip -response and -process
                                   [target message])
                _ (when error?
                    (>! (engine-channel :error) (update-message message :engine (constantly source))))
                message (update-message message :response #(if error? nil %))]
            (>! (engine-channel :trace) (update-message message :engine (constantly source)))
            (when-not (= :finish target)
              (>! (engine-channel target) message))))
        (catch Exception e
          (error "Caught exception in manifold loop:" (with-out-str (print-cause-trace e)))))
      (when (and input-message
                 (not= :stop-engine input-message))
        (recur  (first (alts!  [stop-channel ch] :priority true)))))
    stop-channel))

(defstate ^:private manifold-engine
  :start
  (start-manifold-engine)

  :stop
  (>!! manifold-engine :stop-engine))
