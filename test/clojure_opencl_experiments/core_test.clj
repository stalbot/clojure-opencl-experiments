(ns clojure-opencl-experiments.core-test
  (:require [clojure.test :refer :all]
            [clojure-opencl-experiments.core :refer :all]
            [clojure.string :as string]
            [criterium.core :refer [with-progress-reporting quick-bench]]))

(defmacro catch-fail [form]
  (let [e (symbol "e")]
    `(try
       ~form
       (catch RuntimeException ~e
         (let [~e
               (str (.getMessage ~e)
                    "\n"
                    (->> ~e .getStackTrace (map str) (string/join "\n")))]
           (println ~e)
           ~e)))))

(defn all-vals [sql]
  (catch-fail (-> sql sql-to-clojure eval)))

(defn first-selected-val [sql]
  (catch-fail (-> sql all-vals first vals first)))

(defn first-selected-field-name [sql]
  (catch-fail (-> sql all-vals first keys first)))

(defn double= [^double x, ^double y]
  (< (Math/abs (- x y)) 0.0000000001))

(deftest simple-select-tests
  (is (= (first-selected-val "select 1") 1))
  (is (= (first-selected-val "select 1.12") 1.12))
  (is (= (first-selected-val "select 'hi'") "hi"))
  (is (= (first-selected-val "select 1 = 1") true))
  (is (= (first-selected-val "select 1 = 4") false))
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

(swap! known-views assoc-in ["memory_test" "bar_child"]
       {:storage-type :memory-test,
        :name :bar_child,
        :binding {:id :integer, :bar_id :integer, :value :string}})

(swap! memory-test-views assoc :bar_child
         [{:id 1, :bar_id 1, :value "bob_val"}
          {:id 2, :bar_id 2, :value "george_val_1"}
          {:id 3, :bar_id 2, :value "george_val_2"}])

(deftest select-from-table-tests
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
         [{:name "bob"} {:name "george"}]))
  (is (= (all-vals "select * from memory_test.bar")
         [{:memory_test.bar.id 1, :memory_test.bar.name "bob", :memory_test.bar.value 3.14}
          {:memory_test.bar.id 2, :memory_test.bar.name "george", :memory_test.bar.value -231.232}]))
  (is (= (all-vals "select memory_test.bar.* from memory_test.bar")
         [{:memory_test.bar.id 1, :memory_test.bar.name "bob", :memory_test.bar.value 3.14}
          {:memory_test.bar.id 2, :memory_test.bar.name "george", :memory_test.bar.value -231.232}]))
  (is (= (all-vals "select bar.* from memory_test.bar")
         [{:memory_test.bar.id 1, :memory_test.bar.name "bob", :memory_test.bar.value 3.14}
          {:memory_test.bar.id 2, :memory_test.bar.name "george", :memory_test.bar.value -231.232}])))

(deftest select-from-subselect
  (is (= (all-vals "select * from (select 1 as foo) as baz")
         [{:baz.foo 1}]))
  ; TODO: this passing test is a lie, the second subselect ends up with
  ; a field named bar.baz.foo and it "just happens" to work
  (is (= (all-vals "select bar.foo from (select * from (select 1 as foo) as baz) as bar")
         [{:bar.foo 1}]))
  (is (= (all-vals "select baz.foo + 1.2 as bar from (select 2 as foo) as baz")
         [{:bar 3.2}]))
  (is (= (all-vals "select foo + 1 as bar, baz.t from (select 2 as foo, 'hi' as t) as baz")
         [{:bar 3, :baz.t "hi"}])))

(deftest select-from-join
  (is (= (all-vals "select bar.name, yo
                      from (select 'hi' as name, 1 as yo) as thing
                      join memory_test.bar on yo=id")
         [{:bar.name "bob", :yo 1}]))
  (is (= (all-vals "select name, bar_child.value
                      from memory_test.bar_child
                      join memory_test.bar on bar_id=bar.id")
         '({:name "bob", :bar_child.value "bob_val"}
           {:name "george", :bar_child.value "george_val_1"}
           {:name "george", :bar_child.value "george_val_2"})))
  ; TODO: note to self: problem here is with the join aliases,
  ; need to inject the alias into the row keys/binding directly
  (is (= (all-vals "select baz.name, bar.name from memory_test.bar
                                    left join memory_test.bar as baz
                                        on baz.name=bar.name")
         '({:baz.name "bob", :bar.name "bob"}
           {:baz.name "george", :bar.name "george"})))
  (is (= (all-vals "select right.foo, left.foo from (select 1 as id, 1 as foo) as right
                            left join (select 1 as id, 2 as foo) as left
                            on right.id = left.id")
         '({:right.foo 1, :left.foo 2}))))