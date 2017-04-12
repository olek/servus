(defproject servus "0.1.0"
  :description "Pumping data"
  :url "https://en.wikipedia.org/wiki/Servus"
  :main servus.core
  :dependencies [[org.clojure/clojure "1.8.0"]

                 ;; logging
                 [org.clojure/tools.logging "0.3.1"]
                 [ch.qos.logback/logback-classic "1.1.7"]

                 ;; parsers/generators
                 [cheshire "5.6.3"]
                 [clojure-csv/clojure-csv "2.0.1"]
                 [de.ubercode.clostache/clostache "1.4.0"]

                 ;; other
                 [environ "1.1.0"]
                 [http-kit "2.2.0"]
                 [mount "0.1.10"]
                 [org.clojure/core.async "0.2.395"]]

  :plugins [[lein-environ "1.0.1"]
            [lein-var-file "0.3.1"]]

  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :jvm-opts ["-Xmx1g" "-server" "-Duser.timezone=UTC"]
             ;"-Djavax.net.debug=ssl" ;good to enable when debugging ssl issues
  :repl-options {:timeout 60000}

  :profiles {:test {:env {:foo "BAR"}
                    :dependencies [[org.clojure/tools.namespace "0.2.11"]]}
             :uberjar {:aot [servus.core]}})
