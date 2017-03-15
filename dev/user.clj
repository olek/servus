(ns user
  (:require [clojure.tools.namespace.repl :as repl]
            [clojure.repl :refer :all] ; convenient access to basic repl helpers
            [clojure.stacktrace :refer [e print-throwable]] ; convenient access to stacktrace helpers
            [desk.util.debug :refer [pprint-map]]
            [mount.core :as mount]))

(defn go []
  (mount/start)
  :ready)

(defn reset
  "Call this function to (re)load all app namespaces in the project"
  []
  (mount/stop)
  (repl/disable-reload!) ; keep reset function around even when some other code fails to compile
  (repl/set-refresh-dirs "./src")
  (repl/refresh :after 'user/go))

(defn reset-test []
  (mount/stop)
  (repl/disable-reload!) ; keep reset function around even when some other code fails to compile
  (repl/set-refresh-dirs "./src" "./test")
  (repl/refresh :after 'user/go))

(defn hints
  "Prints some (hopefully) useful hints on using this REPL"
  []
  (println "
REPL hints:
  - reload changed namespaces with (user/reset)
  - print last exception stack trace (pst)
"))
