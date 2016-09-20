(ns clojure-opencl-experiments.core
  (:require [instaparse.core :as insta]
    ;[uncomplicate.clojurecl [core :refer :all]
    ; [info :refer :all]]
            [clojure.string :as str]
            [clojure.string :as string])
  (:import (java.util.regex Pattern)))

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
      (fn [[t1 t2 :as types]]
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
        visited-globs (mapcat #(visit [% ctxt-with-row]) GLOB)
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
    [expr (assoc expr-ctxt :field-name field-name)]))

(defmethod visit :GLOB [[[_ & qualifiers] parse-context]]
  ; this method violates the visit contract for convenience ->
  ; returns a sequence of the tuple you'd normally expect from visit()
  ; TODO maybe change this
  (let [qualification-reg (re-pattern
                            (str "^(?:\\w+\\.)*"
                                 (string/join "\\." qualifiers)))
        row-sym (:row-sym parse-context)
        binding (:binding parse-context)]
    (map
      (fn [[field-name field-type]]
        [`(~field-name ~row-sym), (assoc parse-context
                                    :field-name field-name
                                    :type field-type)])
      (filter #(re-find qualification-reg (name (first %))) binding))))

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
        parse-context (update parse-context
                              :binding
                              merge
                              (:binding data-ctxt))
        parse-context (assoc-in parse-context
                                [:aliased-views alias-name]
                                original-name)
        parse-context (assoc parse-context
                        :view-name
                        (or alias-name original-name))]
    (when-not (or original-name alias-name)
      (throw (RuntimeException. "TODO every derived table blah blah alias")))
    [data-code parse-context]))

(defmethod visit :ALIAS [[[_ & args] parse-context]]
  ; deliberately do not worry about \"s in the parse ->
  ; they SHOULD become part of the column name keyword
  (let [key-name (if (= (:context parse-context) :from)
                   :view-name
                   :field-name)]
    [nil (assoc parse-context key-name (keyword (first args)))]))

(defmethod visit :JOIN [[[_ & args] parse-context]]
  (let [data-sym (symbol "data")

        [join-component-forms, new-context]
        (reduce
          (fn [[forms context] arg]
            (let [[form ctxt] (visit [arg context])]
              [(conj forms form) ctxt]))
          [[], (assoc parse-context :data-sym data-sym)]
          args)

        forms-for-let (mapcat (fn [form] [data-sym form])
                              join-component-forms)]
    [`(let [~data-sym nil
            ~@forms-for-let]
        ~data-sym),
     new-context]))


(defmethod visit :ON_JOIN [[[_ & args] parse-context]]
  (let [by-key (into {} (for [[key & _ :as node] args] [key node]))
        [view-code view-ctxt] (visit [(:VIEW by-key) parse-context])

        new-data-sym (symbol "new-data")
        ctxt-for-on (assoc view-ctxt
                      ; could probably just use :view-ctxt directly,
                      ; but explicitlness better to avoid confusion later
                      :new-view-name (:view-name view-ctxt)
                      :new-data-sym new-data-sym
                      :join-strategy (-> by-key :JOIN_STRAT first (or :INNER))
                      :join-direction (-> by-key :JOIN_DIRECTION first))

        [on-code _] (visit [(:ON by-key) ctxt-for-on])
        on-code `(let [~new-data-sym ~view-code] ~on-code)]
    [on-code, view-ctxt]))

(defn- form-contains-view-name? [form view-name]
  (let [view-name-reg (if (= (type view-name) Pattern)
                        view-name
                        (re-pattern (str "^" (name view-name) "\\.")))]
    ; TODO: where the hell is `any?`
    (not (not-any?
           #(or (and (keyword? %) (re-find view-name-reg (name %)))
                (and (seq? %) (form-contains-view-name? % view-name)))
           form))))

(defn- subseq-vals [func]
  [#(reduce concat (vals (subseq %1 func %2))), true])

(def supported-split-funcs
  {`= [#(get %1 %2) false]
   `> (subseq-vals >)
   `>= (subseq-vals >=)
   `< (subseq-vals <)
   `<= (subseq-vals <=)})

(defn split-on-code-across-join [expr-code joining-view-name]
  (if-not (= 3 (count expr-code))
    ; form was not a binary function, we are screwed
    []
    (let [[func form1 form2] expr-code
          forms [form1 form2]
          forms-with-views (map
                             #(form-contains-view-name? % joining-view-name)
                             forms)]
      (if (or (not (contains? supported-split-funcs func))
              (every? true? forms-with-views))
        ; cannot split expression by views, screwed again
        ; TODO: this isn't quite right:
        ; "ON foo.bar + foo.baz = 2" aka
        ; (= (+ (:foo.bar row-sym) (:foo.baz row-sym)) 2) will get a
        ; false negative here and KABOOM down the line
        ; many more TODOs here: support trees of ORs/ANDs with splittable
        ; leaf conditions, optimize the above example to be splittable, etc.
        []
        [func forms (if (first forms-with-views) 1 0)]))))


(def func-swapper
  ; depending on who we're iterating over, will need to flip around the
  ; gt/lt -> this is 100% wrong I think, just need to remember to fix it
  {`>= `<=
   `> `<})


(defn join-with-grouping
  [func forms left-idx left-data-sym right-data-sym row-sym parse-context]
  (let [[lform rform] (if (= left-idx 0) forms (reverse forms))
        [iter-sym lkup-sym] (map symbol ["iter-data" "lkup-data"])
        ; reiterating, have not thought through the below line at all
        func (if (= left-idx 1) (get func-swapper func func) func)
        [transformed-func sort-required] (get supported-split-funcs func)
        [ksym vsym rsym] (map symbol ["k" "v" "r"])
        inner-only? (nil? (:join-direction parse-context))
        ; if inner join, which one we iterate over doesn't really matter
        vals-of-iters (if (= (:join-direction parse-context) :left)
                        [left-data-sym right-data-sym]
                        [right-data-sym left-data-sym])]

    `(let [~left-data-sym (group-by #(let [~row-sym %] ~lform)
                                    ~left-data-sym)
           ~right-data-sym (group-by #(let [~row-sym %] ~rform)
                                     ~right-data-sym)

           [~iter-sym ~lkup-sym] ~vals-of-iters
           ~@(if sort-required [lkup-sym, `(into (sorted-map) ~lkup-sym)])]
       ; TODO: in a FULL OUTER join, will need to add in all grouped keys
       ; from lkup-sym that weren't in iter-sym
       (mapcat
         (fn [~ksym ~vsym]
           (mapcat
             (fn [~rsym]
               (or
                 (not-empty
                   (map #(merge ~rsym %)
                        (~transformed-func ~lkup-sym ~ksym)))
                 ; if left/right, keep a row for every join 'miss'
                 ~(if inner-only? [] [rsym])))
             ~vsym))
         ~iter-sym))))


(defn join-with-scan
  ; TODO: there's some duplicate logic that could be eliminated from
  ; this and the above
  [expr-code left-data-sym right-data-sym row-sym parse-context]
  (let [inner-only? (nil? (:join-direction parse-context))
        ; if inner join, which one we iterate over doesn't really matter
        [iter-sym other-sym] (if (= (:join-direction parse-context) :left)
                               [left-data-sym right-data-sym]
                               [right-data-sym left-data-sym])]
    `(mapcat
       (fn [~row-sym]
         (->
           (filter
             #(-> % first nil? not)
             (map #(let [~row-sym (merge ~row-sym %)]
                     (if ~expr-code ~row-sym))
                  ~other-sym))
           ; need to get the row even if no match for left/right
           ~@(if-not inner-only? [`not-empty `(or [~row-sym])])))
       ~iter-sym)))


(defmethod visit :ON [[[_ & args] parse-context]]
  (let [row-sym (gensym)
        ctxt-for-expr (assoc parse-context :row-sym row-sym)
        [expr-code _] (visit [(first args) ctxt-for-expr])
        [func forms left-idx] (split-on-code-across-join
                                expr-code
                                (:new-view-name parse-context))
        code (if func
               (join-with-grouping func
                                   forms
                                   left-idx
                                   (:data-sym parse-context)
                                   (:new-data-sym parse-context)
                                   row-sym
                                   parse-context)
               (join-with-scan expr-code
                               (:data-sym parse-context)
                               (:new-data-sym parse-context)
                               row-sym
                               parse-context))]
    [code ctxt-for-expr]))


(def one-true-field-name-counter (atom -1))
(defn- gen-field-name []
  (keyword (str "field_" (swap! one-true-field-name-counter inc))))

(defmethod visit :EXPRESSION [[[_ & args] parse-context]]
  (let [ctxt-for-child (assoc parse-context :context :expression)
        [code ctxt] (visit [(first args) ctxt-for-child])]
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
      (throw (RuntimeException. (str "TODO bad view '" view "'"))))))

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
  (if (= (:context parse-context) :expression)
    (let [field-name (keyword (str/join "." args))
          row-sym (:row-sym parse-context)
          field-name-in-ctxt (fully-qualified-name field-name parse-context)
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



