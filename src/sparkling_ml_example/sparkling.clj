(ns sparkling-ml-example.sparkling
  (:gen-class)
  (:require [clojure.string :as str]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.debug :as s-dbg]
            [sparkling.destructuring :as s-de]
            [sparkling.kryo :as k]
            [sparkling.scalaInterop :as scala]
            [clojure.java.io :as io]
            [incanter.charts :as c]
            [incanter.core :as i]
            [incanter.datasets :as d]
            [incanter.stats :as s])
  (:import [org.apache.spark.api.java JavaRDD]
           [org.apache.spark.mllib.linalg Vector SparseVector]
           [org.apache.spark.mllib.linalg.distributed RowMatrix]
           [org.apache.spark.mllib.recommendation ALS Rating]
           [distributions PoissonDistribution]))

(defn to-long [s]
  (Long/parseLong s))

(defn line->item-tuple [line]
  (let [[id name] (str/split line #"\|")]
    (vector (to-long id) name)))

(defn load-items [path]
  (with-open [rdr (io/reader (io/resource path))]
    (->> (line-seq rdr)
         (map line->item-tuple)
         (into {}))))

;; (parse-long "10") ;;=> 10
(defn parse-long [i]
  (Long/parseLong i))

;; (parse-double "1.1") ;;=> 1.1
(defn parse-double [i]
  (Double/parseDouble i))

;; (spark/with-context sc (-> (conf/spark-conf) (conf/master "local") (conf/app-name "ch7")) (count-ratings sc))
(defn count-ratings [sc]
  (->> (spark/text-file sc "resources/data/ml-100k/ua.base")
       (spark/count)))

(defn parse-line [line]
  (->> (str/split line #"\t")
       (map parse-long)))

(defn parse-rating [line]
  (let [[user item rating time] (->> (str/split line #"\t")
                                     (map parse-long))]
    (spark/tuple (mod time 10)
                 (Rating. user item rating))))

(defn parse-ratings [sc]
  (->> (spark/text-file sc "resources/data/ml-100k/ua.base")
       (spark/map-to-pair parse-rating)))

(defn training-ratings [ratings]
  (->> ratings
       (spark/filter (fn [tuple]
                       (< (s-de/key tuple) 8)))
       (spark/values)))

(defn test-ratings [ratings]
  (->> ratings
       (spark/filter (s-de/first-value-fn
                      (fn [key] (>= key 8))))
       (spark/values)))

(defn user-product [rating]
  (spark/tuple (.user rating)
               (.product rating)))

(defn user-product-rating [rating]
  (spark/tuple (user-product rating)
               (.rating rating)))

(defn parse-movie [movie]
  (let [[mid mname] (str/split movie #"::")]
    [(parse-long mid)
     mname]))

(defn to-java-rdd [rdd]
  (JavaRDD/fromRDD rdd scala/OBJECT-CLASS-TAG))

(defn to-mllib-rdd [rdd]
  (.rdd rdd))

(defn from-mllib-rdd [rdd]
  (JavaRDD/fromRDD rdd scala/OBJECT-CLASS-TAG))

(defn predict [model data]
  (->> (spark/map-to-pair user-product data)
       (to-mllib-rdd)
       (.predict model)
       (from-mllib-rdd)
       (spark/map-to-pair user-product-rating)))

(defn squared-error [y-hat y]
  (Math/pow (- y-hat y) 2))

(defn sum-squared-errors [predictions actuals]
  (->> (spark/join predictions actuals)
       (spark/values)
       (spark/map (s-de/val-val-fn squared-error))
       (spark/reduce +)))

(defn rmse [model data]
  (let [predictions  (spark/cache (predict model data))
        actuals (->> (spark/map-to-pair user-product-rating
                                        data)
                     (spark/cache))]
    (-> (sum-squared-errors predictions actuals)
        (/ (spark/count data))
        (Math/sqrt))))

(defn compute [data validation-set]
  (for [rank [50 60 70 80 90 100 110 120 130 140 150]]
    (let [model (ALS/trainImplicit (.rdd  data) rank 20 0.01 8)]
      (println (str rank "\t" (rmse model validation-set))))))

(defn alternating-least-squares [data {:keys [rank num-iter
                                              lambda]}]
  (ALS/train (to-mllib-rdd data) rank num-iter lambda 10))

(defn to-sparse-vector [cardinality]
  (fn [ratings]
    (SparseVector. cardinality
                   (int-array (map #(.product %) ratings))
                   (double-array (map #(.rating %) ratings)))))

(defn to-matrix [rdd]
  (RowMatrix. (.rdd rdd)))

(defn ratings->matrix [data]
  (let [cardinality (-> data
                        (spark/map #(.product %))
                        (spark/reduce max)
                        (inc))]
    (->> data
         (spark/group-by #(.user %))
         (spark/values)
         (spark/map (to-sparse-vector cardinality))
         (to-matrix))))

(defn pca [matrix]
  (.computePrincipalComponents matrix 10))

;;;;;; call test
(defn ex-7-34 []
  (spark/with-context sc (-> (conf/spark-conf)
                             (conf/master "local")
                             (conf/app-name "ch7"))
    (count-ratings sc)))


(defn ex-7-35 []
  (spark/with-context sc (-> (conf/spark-conf)
                             (conf/master "local")
                             (conf/app-name "ch7"))
    (->> (parse-ratings sc)
         (spark/collect)
         (first))))

(defn ex-7-36 []
  (spark/with-context sc (-> (conf/spark-conf)
                             (conf/master "local")
                             (conf/app-name "ch7"))
    (let [ratings (spark/cache (parse-ratings sc))
          train (training-ratings ratings)
          test  (test-ratings ratings)]
      (println "Training:" (spark/count train))
      (println "Test:"     (spark/count test)))))

(defn ex-7-37 []
  (spark/with-context sc (-> (conf/spark-conf)
                             (conf/master "local")
                             (conf/app-name "ch7"))
    (-> (parse-ratings sc)
        (training-ratings)
        (alternating-least-squares {:rank 10
                                    :num-iter 10
                                    :lambda 1.0}))))

(defn ex-7-38 []
  (spark/with-context sc (-> (conf/spark-conf)
                             (conf/master "local")
                             (conf/app-name "ch7"))
    (let [options {:rank 10
                   :num-iter 10
                   :lambda 1.0}
          model (-> (parse-ratings sc)
                    (training-ratings )
                    (alternating-least-squares options))]
      (into [] (.recommendProducts model 1 3)))))

(defn ex-7-39 []
  (spark/with-context sc (-> (conf/spark-conf)
                             (conf/master "local")
                             (conf/app-name "ch7"))
    (let [items   (load-items "data/ml-100k/u.item")
          id->name (fn [id] (get items id))
          options {:rank 10
                   :num-iter 10
                   :lambda 1.0}
          model (-> (parse-ratings sc)
                    (training-ratings )
                    (alternating-least-squares options))]
      (->> (.recommendProducts model 1 3)
           (map (comp id->name #(.product %)))))))


(defn ex-7-40 []
  (spark/with-context sc (-> (conf/spark-conf)
                             (conf/master "local")
                             (conf/app-name "ch7"))
    (let [options {:num-iter 10 :lambda 0.1}
          training (-> (parse-ratings sc)
                       (training-ratings)
                       (spark/cache))
          ranks    (range 2 50 2)
          errors   (for [rank ranks]
                     (doto (-> (alternating-least-squares training
                                                          (assoc options :rank rank))
                               (rmse training))
                       (println "RMSE for rank" rank)))]
      (-> (c/scatter-plot ranks errors
                          :x-label "Rank"
                          :y-label "RMSE")
          (i/view)))))

(defn -main
  [& args]
  (do
    (println "===============ex-7-34")
    (ex-7-34)
    (println "===============ex-7-35")
    (ex-7-35)
    (println "===============ex-7-36")
    (ex-7-36)
    (println "===============ex-7-37")
    (ex-7-37)
    (println "===============ex-7-38")
    (ex-7-38)
    (println "===============ex-7-39")
    (ex-7-39)
    (println "===============ex-7-40")
    (ex-7-40)))
