(ns servus.store
  (:require [clojure.tools.logging :refer [info warn error]]
            [mount.core :refer [defstate]]))

(defstate sessions
  :start
  (atom {}))

(defn session-atom [username]
  (get (swap! sessions
              update-in
              [username]
              #(or % (atom {})))
       username))

(defn session [username]
  @(session-atom username))

(defn update-session [username f]
  (swap! (session-atom username)
         f))
