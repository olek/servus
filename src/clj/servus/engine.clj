(ns servus.engine
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.core.async :refer [<! >!! go-loop]]
            [mount.core :refer [defstate]]
            [servus.channels :refer [channels]]))

(defmacro create [ref & code]
  (let [input-channel (keyword (str (name ref) "-in"))
        output-channel (keyword (str (name ref) "-out"))
        engine-name (gensym (str "engine-" (name ref)))]
    `(defstate ^:private ~engine-name
       :start
       (let [ch# (get channels ~input-channel)
             quit-atom# (atom false)
             output-handler# (fn [[username# session#] output#]
                              (>!! (get channels ~output-channel) [username# (merge session# output#)])
                               output#)]
         ;; TODO catch all errors in go-loop
         (go-loop [~'input-message :start]
           (condp = ~'input-message
             :start
             (info "Waiting for requests on" ~input-channel "channel...")

             nil
             (info "Not waiting for requests on" ~input-channel "channel anymore, exiting")

             ;           (apply process ~'input-message)
             (let [~'output-handler (partial output-handler# ~'input-message)]
               ~@code))
           ;; TODO add timeout
           (when (and (not @quit-atom#) ~'input-message)
             (recur  (<! ch#))))
         quit-atom#)

       :stop
       (reset! ~engine-name true))))
