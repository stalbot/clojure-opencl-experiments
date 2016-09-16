(ns clojure-opencl-experiments.core-test
  (:require [clojure.test :refer :all]
            [clojure-opencl-experiments.core :refer :all]))

(defn all-vals [sql]
  (-> sql sql-to-clojure eval))

(defn first-selected-val [sql]
  (-> sql all-vals first vals first))

(defn first-selected-field-name [sql]
  (-> sql all-vals first keys first))

(defn double= [^double x, ^double y]
  (< (Math/abs (- x y)) 0.0000000001))

(deftest simple-select-tests
  (is (= (first-selected-val "select 1") 1))
  (is (= (first-selected-val "select 1.12") 1.12))
  (is (= (first-selected-val "select 'hi'") "hi"))
  (is (= (first-selected-field-name "select 5242 as something") :something))
  (is (= (first-selected-field-name "select 5242 as \"foo.bar\"")
         (keyword "\"foo.bar\"")))
  (is (= (first-selected-val "select 1 + 1") 2))
  (is (= (first-selected-val "select 6 / 1.5 as something") 4.0))
  (is (= (first-selected-field-name "select 6 / 1.5 as something")
         :something))
  (is (double= (first-selected-val "select 3 * 5.1") 15.3)))


(swap! known-views assoc-in ["memory_test" "bar"]
       {:storage-type :memory-test,
        :name :bar,
        :binding {:id :integer, :name :string, :value :float}})


(swap! memory-test-views assoc :bar
       [{:id 1, :name "bob", :value 3.14}
        {:id 2, :name "george", :value -231.232}])


(deftest select-from-tests
  (is (= (all-vals "select 1 as val from memory_test.bar")
         [{:val 1} {:val 1}]))
  (is (= (all-vals "select name as thing, id + 6 as i from memory_test.bar")
         [{:thing "bob", :i 7} {:thing "george", :i 8}]))
  (is (= (all-vals "select baz.name as thing, baz.id + 6 as i
                    from memory_test.bar as baz")
         [{:thing "bob", :i 7} {:thing "george", :i 8}]))
  (is (= (all-vals "select name from memory_test.bar as baz")
         [{:name "bob"} {:name "george"}]))
  (is (= (all-vals "select name from memory_test.bar")
       [{:name "bob"} {:name "george"}])))

;(deftest blah
;  (is
;    (clojure.core/map
;      (clojure.core/fn [G__18558] {:val 1})
;      (clojure-opencl-experiments.core/load-qualified-view '("memory_test" "bar")))))