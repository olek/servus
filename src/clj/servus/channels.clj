(ns servus.channels
  (:require [clojure.core.async :refer [<! >! >!! chan close! mult tap go-loop go timeout alts!]]
            [clojure.stacktrace :refer [print-cause-trace]]
            [clojure.tools.logging :refer [info warn error]]
            [environ.core :refer [env]]
            [mount.core :refer [defstate]]))

(defstate ^:private channels
  :start
  (atom {}))

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


;(defn- special-route [source message]
;  (cond
;    (and (= source :check-batch-process)
;         (= "Queued" (get-in message [1 :response])))
;    (let [times-attempted (get-in message [1 :times-attempted] 1)]
;      (if (< times-attempted 3)
;        (do
;          (warn "Postponing " source "- attempted" times-attempted "times")
;          (go
;            (<! (timeout 5000))
;            (warn "Retrying " source "- attempted" times-attempted "times")
;            ;; TODO pushing in previous engine is super-ugly, fix it
;            (>! (manifold-channel) [:create-batch-process (update-message message :times-attempted (fnil inc 1))]))
;          [:finish message])
;        (do (warn "Aborting retries of" source "after" times-attempted "attempts")
;          [:close-job-request [(first message) (dissoc (last message) :times-attempted)]])))

;    :else nil))
