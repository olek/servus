(ns desk.util.debug
  (:require [clojure.pprint :refer [*print-miser-width*]]))

(defmacro dbg [f]
  (let [m (meta &form)]
    `(let [r# ~f]
       (println "DBG:" ~(str *ns* ":" (:line m) ":" (:column m))
                '~f)
       (clojure.pprint/pprint r#)
       r#)))

(defn- truncate
  "Truncate a string with suffix (ellipsis by default) if it is
   longer than specified length."

  ([string length suffix]
     (let [string-len (count string)
           suffix-len (count suffix)]

       (if (<= string-len length)
         string
         (str (subs string 0 (- length suffix-len)) suffix))))

  ([string length]
     (truncate string length "...")))

(defn pprint-map [m]
  (println "{")
  (doall (for [[k v] m] (println (str "  " (pr-str k) ": " (truncate (pr-str v) 120)))))
  (println "}"))
