(ns servus.engine-factory
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.core.async :refer [<! >!! go-loop]]
            [mount.core :refer [defstate]]
            [servus.channels :refer [channel-for]]))

(defmacro create-engine [ref & code]
  (let [engine-name (gensym (str "engine-" (name ref)))]
    `(defstate ^:private ~engine-name
       :start
       (let [ch# (channel-for ~ref :in)
             quit-atom# (atom false)
             output-handler# (fn self#
                               ([input-message# response#]
                                (self# input-message# response# {}))
                               ([[username# session#] response# session-overrides#]
                                (>!! (channel-for ~ref :out) [username# (merge session#
                                                                               session-overrides#
                                                                               {:response response#})])))]
         ;; TODO catch all errors in go-loop
         (go-loop [~'input-message :start]
           (condp = ~'input-message
             :start
             (info "Waiting for requests for " ~ref "engine...")

             nil
             (info "Not waiting for requests for" ~ref "engine anymore, exiting")

             ;           (apply process ~'input-message)
             (let [~'output-handler (partial output-handler# ~'input-message)]
               ~@code))
           ;; TODO add timeout
           (when (and (not @quit-atom#) ~'input-message)
             (recur  (<! ch#))))
         quit-atom#)

       :stop
       (reset! ~engine-name true))))
