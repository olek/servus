(ns servus.core
  (:require [clojure.core.async :refer [>!!]]
            [servus.channels :refer [channels]]))

(defn login [username password]
  (>!! (:login-request-in channels) [username password]))
