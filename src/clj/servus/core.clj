(ns servus.core
  (:require [clojure.core.async :refer [>!!]]
            [servus.channels :refer [engine-channel]]))

(defn login [username password]
  (>!! (engine-channel :login) [username {:password password}]))
