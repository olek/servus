(ns servus.debug)
(ns servus.debug
  (:require [clojure.core.async :refer [<! go-loop]]
            [clojure.tools.logging :refer [info]]
            [environ.core :refer [env]]
            [mount.core :refer [defstate]]
            [servus.channels :refer [channels]]))

(defn process [username data]
  (let [data (or (:body data)
                 data)]
    (info (str "[" username "]") (pr-str data))))

(defstate ^:private engine
  :start
  (let [ch (:debug channels)
        quit-atom (atom false)]
    (go-loop [input :start]
      (condp = input
        :start
        (info "Waiting for debug input...")

        nil
        (info "Not waiting for debug input anymore, exiting")

        (apply process input))
      (when (and (not @quit-atom) input)
        (recur  (<! ch))))
    quit-atom)

  :stop
  (reset! engine true))
