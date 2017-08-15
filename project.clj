(defproject sparkling-ml-example "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [gorillalabs/sparkling "2.1.2"]]
  :aot [#".*" sparkling.serialization sparkling.destructuring]
  :main sparkling-ml-example.core
  :profiles {:provided {:dependencies [[org.apache.spark/spark-core_2.11 "2.2.0"]
                                       [org.apache.spark/spark-sql_2.11 "2.2.0"]]}
             :dev {:plugins [[lein-dotenv "RELEASE"]]}})
