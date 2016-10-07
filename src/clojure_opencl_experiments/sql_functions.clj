(ns clojure-opencl-experiments.sql-functions
  (:require [clojure.string :as string]))

(defmacro allow-nulls [func]
  (let [func-arglists (-> func
                          resolve
                          meta
                          :arglists)
        forms (map (fn [arglist]
                     (if (some #{'&} arglist)
                       `(~arglist
                          (if (or
                                (or ~@(map (fn [a] `(nil? ~a))
                                           (drop-last 2 arglist)))
                                (some nil? ~(last arglist)))
                            nil
                            (apply ~func ~@(conj (drop-last 2 arglist)
                                                 (last arglist)))))
                       `(~arglist
                          (if (or ~@(map (fn [a] `(nil? ~a)) arglist))
                            nil
                            (~func ~@arglist)))))
                   func-arglists)]
    `(fn ~@forms)))


(defn sql-concat [& args]
  (if (some nil? args) nil (string/join "" args)))

(def sql-lower (allow-nulls string/lower-case))
(def sql-upper (allow-nulls string/upper-case))

(defn sql-count [accum & row-vals]
  (+ accum (if (not-every? nil? row-vals) 1 0)))

(defn sql-sum [accum val]
  (+ accum (if-not (nil? val) val 0)))

(defn sql-average [[sum-accum count-accum] val]
  [(sql-sum sum-accum val) (sql-count count-accum val)])

(defn sql-average-finalizer [[sum-accum, count-accum]]
  (/ (double sum-accum) count-accum))

(defn noop [x] x)

(defn zerof [& _] 0)
(defn zerof2 [& _] [0 0])

(def aggregates {:sum [`sql-sum `zerof `noop],
                 :avg [`sql-average `zerof2 `sql-average-finalizer],
                 :count [`sql-count `zerof `noop]})

(def exportable [:concat :lower :upper :sum])

(def func-lkup
  (into {}
        (map
          (fn [func-name]
            [func-name, (resolve (symbol (str "sql-" (name func-name))))])
          exportable)))
