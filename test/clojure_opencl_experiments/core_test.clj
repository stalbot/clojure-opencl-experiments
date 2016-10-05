(ns clojure-opencl-experiments.core-test
  (:require [clojure.test :refer :all]
            [clojure-opencl-experiments.core :refer :all]
            [clojure-opencl-experiments.memory-test :refer :all]
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
  (catch-fail (-> sql sql-to-clojure eval-sql-code)))

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
  (is (= (all-vals "select baz.name, bar.name from memory_test.bar
                                    left join memory_test.bar as baz
                                        on baz.name=bar.name")
         '({:baz.name "bob", :bar.name "bob"}
           {:baz.name "george", :bar.name "george"})))
  (is (= (all-vals "select * from
                      (select 1 as foo, 'hi' as bar union all
                      select 3 as foo, 'hi' as bar union all
                      select 6 as foo, 'not' as bar union all
                       select 2 as foo, 'why' as bar) as sub
                      left join
                      (select 1 as foo, 'HI' as bar union all
                       select 2 as foo, 'WHY' as bar) as other_sub
                      on UPPER(sub.bar) = other_sub.bar
                      order by 3")
         '({:other_sub.foo 1, :other_sub.bar "HI", :sub.foo 1, :sub.bar "hi"}
           {:other_sub.foo 2, :other_sub.bar "WHY", :sub.foo 2, :sub.bar "why"}
           {:other_sub.foo 1, :other_sub.bar "HI", :sub.foo 3, :sub.bar "hi"}
           {:other_sub.foo nil, :other_sub.bar nil, :sub.foo 6, :sub.bar "not"})))
  (is (= (all-vals "select right.foo, left.foo from (select 1 as id, 1 as foo) as right
                            left join (select 1 as id, 2 as foo) as left
                            on right.id = left.id")
         '({:right.foo 1, :left.foo 2}))))

(deftest test-union-all
  (is (= (all-vals "select 1 as foo union all select 2 as foo")
         '({:foo 1} {:foo 2})))
  (is (= (all-vals "select 1 as foo, 'hi' as bar union all
                      select 2 as foo, 'why' as bar")
         '({:foo 1, :bar "hi"} {:foo 2, :bar "why"}))))

(deftest group-by-test
  ; aka does it work at all?
  (is (= (all-vals "select name from memory_test.bar group by 1")
         '({:name "bob"} {:name "george"})))
  (is (= (all-vals "select name from memory_test.bar group by name")
         '({:name "bob"} {:name "george"})))
  (is (= (all-vals "select bar, foo from
                      (select 1 as foo, 'hi' as bar union all
                       select 1 as foo, 'hi' as bar union all
                       select 2 as foo, 'why' as bar) as sub
                      group by 2, bar")
         '({:foo 1, :bar "hi"} {:foo 2, :bar "why"}))))

(deftest group-by-with-aggs-test
  (is (= (all-vals "select count(id) as c, bar_id
                      from memory_test.bar_child group by 2")
         [{:c 1, :bar_id 1} {:c 2, :bar_id 2}]))
  (is (= (all-vals "select sum(id) as summy, bar_id
                      from memory_test.bar_child group by 2")
         [{:summy 1, :bar_id 1} {:summy 5, :bar_id 2}]))
  (is (= (all-vals "select sum(bar_id) + 100 as summy, bar_id
                        from memory_test.bar_child
                        group by 2")
         '({:summy 101, :bar_id 1} {:summy 104, :bar_id 2})))
  (is (= (all-vals "select sum(bar_id) + count(value) as count_summy,
                           bar_id
                        from memory_test.bar_child
                        group by 2")
         '({:count_summy 2, :bar_id 1} {:count_summy 6, :bar_id 2})))
  (is (= (all-vals "select avg(id) as average, bar_id from
                        memory_test.bar_child group by 2")
         '({:average 1.0, :bar_id 1} {:average 2.5, :bar_id 2}))))

(deftest raw-aggs-test
  (is (= (all-vals "select sum(id) as whatever
                     from memory_test.bar_child")
         [{:whatever 6}]))
  (is (= (all-vals "select count(id) as whatever
                       from memory_test.bar_child
                       order by whatever
                       limit 1")
         [{:whatever 3}]))
  (is (= (all-vals "select sum(id * 10 + 3) / (avg(id + 2) - 1) as meaningless
                      from memory_test.bar_child")
         [{:meaningless 23.0}])))

(deftest order-by-test
  (is (= (all-vals "select name from (select 'foo' as name
                                         union all select 'bob' as name) as baz
                              group by 1 order by 1")
         '({:name "bob"} {:name "foo"})))
  (is (= (all-vals "select name, id from
                          (select 'foo' as name, 2 as id
                             union all select 'foo' as name, 1 as id
                             union all select 'bar' as name, 10 as id) as baz
                     group by 1, 2 order by 1, 2")
         '({:name "bar", :id 10} {:name "foo", :id 1} {:name "foo", :id 2}))))

(deftest test-limit
  (is (= (all-vals "select id, value from memory_test.bar_child
                             order by 2
                             limit 2")
         '({:id 1, :value "bob_val"} {:id 2, :value "george_val_1"}))))

(deftest test-concat
  (is (= (all-vals "select concat('ab', 'cd') as val")
         [{:val "abcd"}])))

(deftest test-upper-lower
  (is (= (all-vals "select upper('cdAb') as val")
         [{:val "CDAB"}]))
  (is (= (all-vals "select lower('cDAb') as val")
         [{:val "cdab"}]))
  (is (= (all-vals "select upper(lower('cDAb')) as val")
         [{:val "CDAB"}])))
