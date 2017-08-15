(ns sparkling-ml-example.core
  (:require [sparkling.conf :as conf]
            [sparkling.core :as spark]))

(defn -main
  [& args]
  (spark/with-context
    sc (-> (conf/spark-conf)
           (conf/app-name "sparkling-test")
           (conf/master "local"))
    (let [lines-rdd
          (spark/into-rdd
           sc ["This is a firest line"
               "Testing spark"
               "and sparkling"
               "Happy hacking!"])]
      (spark/collect                      
       (spark/filter                     
        #(.contains % "spark")          
        lines-rdd)))))

