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
