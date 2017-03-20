(ns servus.engine-factory
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.core.async :refer [<! >!! go-loop]]
            [mount.core :refer [defstate]]
            [servus.channels :refer [create-channel! close-channel! engine-channel manifold-channel]]))

(defmacro create-engine [ref & code]
  (let [engine-name (gensym (str "engine-" (name ref)))]
    `(defstate ^:private ~engine-name
       :start
       (let [_# (create-channel! ~ref)
             ch# (engine-channel ~ref)
             quit-atom# (atom false)
             output-handler# (fn self#
                               ([input-message# response#]
                                (self# input-message# response# {}))
                               ([[username# session#] response# session-overrides#]
                                (>!! (manifold-channel) [~ref, username# (merge session#
                                                                                session-overrides#
                                                                                {:response response#})])))]
         ;; TODO catch all errors in go-loop
         (go-loop [~'input-message :start]
           (condp = ~'input-message
             :start
             (info "Waiting for requests to" ~ref "engine...")

             nil
             (info "Not waiting for requests to" ~ref "engine anymore, exiting")

             (let [~'output-handler (partial output-handler# ~'input-message)]
               ~@code))
           (when (and (not @quit-atom#) ~'input-message)
             (recur  (<! ch#))))
         quit-atom#)

       :stop
       (do
         (reset! ~engine-name true)
         (close-channel! ~ref)))))
