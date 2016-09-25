(ns clojure-opencl-experiments.sql-functions
  (:require [clojure.string :as string]))

(defn sql-concat [& args]
  (string/join "" args))

(def sql-lower string/lower-case)
(def sql-upper string/upper-case)

(defn sql-count [accum & row-vals]
  (+ accum (if (not-every? nil? row-vals) 1 0)))

(defn sql-sum [accum val]
  (+ accum (if-not (nil? val) val 0)))

(defn sql-average [[sum-accum count-accum] val]
  [(sql-sum sum-accum val) (sql-count count-accum val)])

(defn sql-average-finalizer [[sum-accum, count-accum]]
  (/ (double sum-accum) count-accum))

(def aggregates #{:sum :avg :count})

(def exportable [:concat :lower :upper])

(def func-lkup
  (into {}
        (map
          (fn [func-name]
            [func-name, (resolve (symbol (str "sql-" (name func-name))))])
          exportable)))
