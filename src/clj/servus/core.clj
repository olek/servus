(ns servus.core
  (:require [clojure.core.async :refer [>!!]]
            [servus.channels :refer [channel-for]]))

(defn login [username password]
  (>!! (channel-for :login-request :in) [username {:password password}]))
