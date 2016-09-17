(ns clojure-opencl-experiments.core
  (:require [instaparse.core :as insta]
    ;[uncomplicate.clojurecl [core :refer :all]
    ; [info :refer :all]]
            [clojure.string :as str]
            [clojure.string :as string]))

(def ^:const grammar-s
  (slurp "resources/grammar.txt"))

(defn- decap-grammar-s [grammar-s]
  ; convenience -> every keyword can be upper/lower, let's not
  ; write all that nonsense in the parser since we're rolling w/o a lexer
  (str/replace grammar-s #"'([A-Z_]+)'" "#'(?i)$1'"))

(def binding-map {})

(def parser (insta/parser (decap-grammar-s grammar-s)))

(defmulti visit #(-> % first first))

; convenience to do less nil-checking
(defmethod visit nil [[& _]] [nil nil])


(defn binding-from-context [context]
  ; here in case it needs to end up doing more
  (:binding context))

(defmethod visit :SELECT [[[_ & args] parse-context]]
  (let [parse-context (assoc parse-context
                        :statement-type :select
                        :context :select)
        by-key (into {} (for [[key & _ :as node] args] [key node]))

        [from-code from-context] (visit [(:FROM by-key) parse-context])
        bound-context from-context

        [field-code field-context] (visit [(:FIELD_LIST by-key) bound-context])
        new-binding (binding-from-context field-context)
        code `(map ~field-code ~(or from-code [{}]))
        bound-context (assoc parse-context :binding new-binding)

        code (reduce
               (fn [code key]
                 (let [[transform-func _]
                       (visit [(get by-key key) bound-context])]
                   (if transform-func
                     `(~transform-func ~code)
                     code)))
               code
               [:GROUP_BY, :ORDER_BY, :LIMIT])]

    [code bound-context]))

(let [number-type-lkup {[:integer :integer] :integer,
                        [:integer :float]   :float,
                        [:float :integer]   :float,
                        [:float :float]     :float}
      comparison-func
      (fn [[[t1 t2] :as types]]
        (if (and (= (count types) 2)
                 (or (= t1 t2) (contains? number-type-lkup types)))
          :bool))]
  (def func-type-lookup
    {:or {[:bool :bool] :bool}
     :and {[:bool :bool] :bool}
     := comparison-func
     :> comparison-func
     :>= comparison-func
     :< comparison-func
     :<= comparison-func
     :is (fn [args] (= (count args) 2))  ; any two types
     :+ number-type-lkup
     :* number-type-lkup
     :/ number-type-lkup
     :- number-type-lkup}))

(defn check-return-type! [func & arg-types]
  (let [type-val-getter (get func-type-lookup func)
        return-type (if type-val-getter
                      (or (get type-val-getter arg-types)
                          (type-val-getter arg-types)))]
    (if return-type
      return-type
      (throw (new
               RuntimeException
               (format
                 "Incompatible types %s for function '%s'"
                 (str "(" (str/join ", " (map str arg-types)) ")")
                 (str func)))))))


(defmethod visit :FIELD_LIST [[[_ & args] parse-context]]
  (let [{:keys [GLOB FIELD]} (group-by first args)
        row-sym (gensym)
        ctxt-with-row (assoc parse-context
                        :row-sym row-sym
                        :context :field-list)
        visited-globs (map #(visit [% ctxt-with-row]) GLOB)
        visited-fields (map #(visit [% ctxt-with-row]) FIELD)
        all-new-fields (concat visited-globs visited-fields)
        selected-field-code (into {}
                                  (map
                                    (fn [[code ctxt]]
                                      [(:field-name ctxt) code])
                                    all-new-fields))
        code `(fn [~row-sym] ~selected-field-code)
        new-binding (into binding-map
                          (map
                            (fn [[_ ctxt]]
                              [(:field-name ctxt) (:type ctxt)])
                            all-new-fields))]
    [code, (assoc parse-context :binding new-binding)]))

(defmethod visit :FROM [[[_ & args] parse-context]]
  (let [parse-context (assoc parse-context :context :from)]
    (visit [(first args) parse-context])))

(defmethod visit :FIELD [[[_ & args] parse-context]]
  (let [visit #(visit [% parse-context])
        [[expr expr-ctxt] [_ alias-ctxt]] (map visit args)
        field-name (if alias-ctxt
                     (:field-name alias-ctxt)
                     (:field-name expr-ctxt))]
    [expr (assoc parse-context :field-name field-name)]))

(defn update! [tmap key func & args]
  (assoc! tmap key (apply func (get tmap key) args)))

(defn make-aggregator [ctxt]
  (let [[accum row] [(gensym) (gensym)]
        updates (map (fn [[agg-col, agg-func]]
                       `(update! ~agg-col ~agg-func (get ~row ~agg-col)))
                     (:agg-colls ctxt))]
    `#(persistent!
       (reduce
         (fn [~accum ~row]
           (-> ~accum
               ~@updates))
         (transient (first %))
         (rest %)))))

(defmethod visit :GROUP_BY [[[_ & args] parse-context]]
  (let [arg-name (gensym)
        by-idx (:field-idx-lkup parse-context)
        group-vec (mapv
                    (fn [arg]
                      (let [[code _] (visit [arg parse-context])]
                        (if (integer? code)
                          (let [kw-for-idx (get by-idx code)]
                            (if-not (nil? code)
                              `(get ~arg-name kw-for-idx)
                              (throw
                                (RuntimeException. "TODO bad group idx"))))
                          `(~code ~arg-name))))
                    args)
        aggregator (make-aggregator parse-context)
        grouping-fn `(fn [~arg-name] group-vec)]
    [`#(->>
        %
        (group-by ~grouping-fn)
        vals
        (map ~aggregator))
     parse-context]))

(defmethod visit :ORDER_BY [[[_ & args] parse-context]])

(defmethod visit :LIMIT [[[_ & args] parse-context]])

(defmethod visit :VIEW [[[_ & args] parse-context]]
  (let [visit #(visit [% parse-context])
        [[data-code data-ctxt] [_ alias-ctxt]] (map visit args)
        alias-name (:view-name alias-ctxt)
        original-name (:view-name data-ctxt)
        parse-context (assoc parse-context :binding (:binding data-ctxt))]
    (when-not (or original-name alias-name)
      (throw (RuntimeException. "TODO every derived table blah blah alias")))
    [data-code (assoc-in parse-context
                       [:aliased-views alias-name]
                       original-name)]))

(defmethod visit :ALIAS [[[_ & args] parse-context]]
  ; deliberately do not worry about \"s in the parse ->
  ; they SHOULD become part of the column name keyword
  (let [key-name (if (= (:context parse-context) :from)
                   :view-name
                   :field-name)]
    [nil (assoc parse-context key-name (keyword (first args)))]))

(defmethod visit :JOIN [[[_ & args] parse-context]])

(defmethod visit :ON_JOIN [[[_ & args] parse-context]])

(def one-true-field-name-counter (atom -1))
(defn- gen-field-name []
  (keyword (str "field_" (swap! one-true-field-name-counter inc))))

(defmethod visit :EXPRESSION [[[_ & args] parse-context]]
  (let [[code ctxt] (visit [(first args) parse-context])]
    ; yeah, this should probably use the SQL like a real thing
    [code (update ctxt :field-name #(or % (gen-field-name)))]))

(defmethod visit :FUNCTION_CALL [[[_ & args] parse-context]]
  ; TODO
  [nil nil])

(defmethod visit :BIN_OP_CALL [[[_ & args] parse-context]]
  (let [visit #(visit [% parse-context])
        [[e1 e1-ctxt] [op op-ctxt] [e2 e2-ctxt]] (map visit args)
        ret-type (check-return-type!
                   (:function op-ctxt)
                   (:type e1-ctxt)
                   (:type e2-ctxt))]
    [`(~op ~e1 ~e2), (assoc parse-context :type ret-type)]))

(defmethod visit :BIN_OP [[[_ & args] parse-context]]
  (let [func-string (str/lower-case (first args))
        func-sym (condp = func-string
                    "is" (symbol "=")
                    ; we can nicely cheat -> every other SQL binary operation
                    ; happens to have the same name in clojure
                    (symbol func-string))]
    [func-sym, (assoc parse-context :function (keyword func-string))]))

(defmethod visit :LITERAL [[[_ & args] parse-context]]
  (visit [(first args) parse-context]))

(defmethod visit :FLOAT [[[_ & args] parse-context]]
  [(Double/parseDouble (first args)) (assoc parse-context :type :float)])

(defmethod visit :INTEGER [[[_ & args] parse-context]]
  [(Long/parseLong (first args)) (assoc parse-context :type :integer)])

(defmethod visit :NULL [[_ parse-context]]
  [nil (assoc parse-context :type :null)])

(defmethod visit :STRING [[[_ & args] parse-context]]
  (let [sval (first args)]
    [(subs sval 1 (- (count sval) 1)), (assoc parse-context :type :string)]))

(defmulti load-view :storage-type)

(def memory-test-views (atom {}))
(defmethod load-view :memory-test [{:keys [name]}]
  (get @memory-test-views name))

(def known-views (atom {}))
(defn lookup-view [view-path]
  (get-in @known-views view-path))

(defn load-qualified-view [view-path]
  (load-view (lookup-view view-path)))

(defn- qualify-keys-for-view [view-name row]
  (persistent!
    (reduce-kv
      (fn [new-row k v]
        (assoc! new-row (keyword (str view-name "." (name k))) v))
      (transient {})
      row)))

(defn qualify-binding-for-view [fully-qualified-view binding]
  (qualify-keys-for-view (name fully-qualified-view) binding))

(defn qualify-data-for-view [fully-qualified-view data]
  (let [view-name (name fully-qualified-view)]
    (mapv
      #(qualify-keys-for-view view-name %)
      data)))

(defn binding-from-qualified-view [view-path]
  (let [view (lookup-view view-path)]
    (if view
      (:binding view)
      (throw (RuntimeException. "TODO bad view")))))

(defn- binding-by-name* [binding]
  (reduce
    (fn [new-b [k _]]
      (let [split-key (string/split (name k) #"\.")]
        (reduce
          #(update %1 (string/join "." (take-last %2 split-key)) conj k)
          new-b
          (range 1 (+ 1 (count split-key))))))
    {}
    binding))

(def binding-by-name
  (memoize binding-by-name*))

(defn fully-qualified-name [field-name parse-context]
  (let [elements (-> field-name name (string/split #"\."))
        aliased-views (:aliased-views parse-context)
        name-lkup (binding-by-name (:binding parse-context))]
    (or
      (reduce
        (fn [_ idx]
          (let [front (str/join "." (take (+ 1 idx) elements))
                back (str/join "." (drop idx elements))
                resolved-alias (get aliased-views front)]
            (if resolved-alias
              (reduced
                (keyword (string/join "." [resolved-alias back])))
              (let [matching-cols (get name-lkup back)
                    matching-cols-count (count matching-cols)]
                (cond
                  (= matching-cols-count 1)
                  (reduced (first matching-cols))
                  (> matching-cols-count 1)
                  (throw (RuntimeException. "TODO ambigious cols"))
                  :else
                  nil)))))
        nil
        (range 0 (count elements)))
      (throw (RuntimeException. "no such col")))))

(defmethod visit :IDENTIFIER [[[_ & args] parse-context]]
  (if (= (:context parse-context) :field-list)
    (let [field-name (keyword (str/join "." args))
          row-sym (:row-sym parse-context)
          field-name-in-ctxt (fully-qualified-name field-name parse-context)
          _ (println field-name-in-ctxt)
          type (get-in parse-context [:binding field-name-in-ctxt])]
      [`(~field-name-in-ctxt ~row-sym),
       (assoc parse-context
         ; :field-name is the one displayed if no alias, so don't qualify
         :field-name field-name
         :type type)])
    ; assuming ctxt :from -> change if more types later
    [`(load-qualified-view ~(vec args))
     (assoc parse-context
       :binding (binding-from-qualified-view args)
       :view-name (str/join "." args))]))

(defmethod visit :NOT_EXPRESSION [[[_ & args] parse-context]]
  (let [[expr-code expr-ctxt] (visit [(first args) parse-context])]
    (if (= (:type expr-ctxt) :bool)
      [`(not ~expr-code), expr-ctxt]
      (throw (RuntimeException.
               (format "Bad type '%s' for NOT." (str (:type expr-ctxt))))))))

(defn to-clojure-data [parsed]
  (first (visit [(second parsed) {:binding binding-map}])))

(defn sql-to-clojure [sql]
  (let [parsed (parser sql)]
    (when (insta/failure? parsed)
      ; todo: can't figure out how to extract nice info w/o printing
      (println parsed)
      (throw (RuntimeException. "Bad parse")))
    (to-clojure-data (parser sql))))



