(ns clojure-opencl-experiments.core
  (:require [clojure-opencl-experiments.sql-functions :as sql-functions]
            [clojure-opencl-experiments.memory-test :refer :all]
            [instaparse.core :as insta]
            ;[clojurewerkz.buffy.core :as buf]
    ;[uncomplicate.clojurecl [core :refer :all]
            [clojure.string :as str]
            [clojure.string :as string]
            [clojure.set :as set]))

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

(defn eval-sql-code [sql-as-clojure]
  ; have to evaluate in this ns
  (eval sql-as-clojure))

(defrecord AggFunctionInst [init-func, incr-func, agg-col, agg-tmp-dims])

(def ^:constant agg-row-sym (symbol "agg-row"))

(defn make-aggregator [ctxt group-cols]
  (let [[accum row] [(gensym) (gensym)]

        ; total hack to suppress arity ide warnings below
        assoc! assoc!

        agg-funcs (:agg-funcs ctxt)
        inititializer (into {}
                            (map
                              (fn [{:keys [init-func agg-col]}]
                                [agg-col `(~init-func)])
                              agg-funcs))
        updates (map
                  (fn [{:keys [incr-func agg-col agg-tmp-dims]}]
                    (let [agg-func-args (map (fn [dim] `(~dim ~row))
                                             agg-tmp-dims)]
                      `(assoc! ~agg-col (~incr-func
                                          (~agg-col ~accum)
                                          ~@agg-func-args))))
                  agg-funcs)
        final-row (into {} (:agg-cols ctxt))]
    `(fn [grouped-rows#]
       (let [first-row# (first grouped-rows#)
             grouped-kvs# (select-keys first-row# ~group-cols)
             ~agg-row-sym (reduce
                            (fn [~accum ~row]
                              (-> ~accum
                                  ~@updates))
                            (transient ~inititializer)
                            grouped-rows#)
             ~agg-row-sym (persistent! ~agg-row-sym)]
         (merge ~final-row grouped-kvs#)))))

(defn split-identifier [identifier]
  (re-seq #"(?:\".*?\"|[^.\"]+)" (name identifier)))

(defn qualify-keys-for-view [view-name row]
  (persistent!
    (reduce-kv
      (fn [new-row k v]
        (let [unqualified-name (-> k name split-identifier last)]
          (assoc! new-row
                  (keyword (str (name view-name) "." unqualified-name))
                  v)))
      (transient {})
      row)))

(defn qualify-data-for-view [fully-qualified-view data]
  (let [view-name (name fully-qualified-view)]
    (mapv
      #(qualify-keys-for-view view-name %)
      data)))

(defmulti load-view :storage-type)

(defmethod load-view :memory-test [{:keys [name view-name]}]
  (qualify-data-for-view view-name (get @memory-test-views name)))

(defn lookup-view [view-path]
  (get-in @known-views view-path))

(defn load-qualified-view [view-path view-name]
  ; view-name is underlying view name or alias
  (load-view (assoc (lookup-view view-path) :view-name view-name)))

(defn binding-from-qualified-view [view-path view-name]
  (let [view (lookup-view view-path)]
    (if view
      (qualify-keys-for-view view-name (:binding view))
      (throw (RuntimeException. (str "TODO bad view '" view "'"))))))

(defmethod visit :SELECT [[[_ & args] parse-context]]
  (visit [(first args) parse-context]))

(defn do-ungrouped-agg [code, bound-context]
  (let [unagged (->> bound-context
                     :agg-cols
                     (map first)
                     set
                     (set/difference
                       (set (keys (:binding bound-context))))
                     not-empty)]
    (when unagged
      (throw (RuntimeException. (str "Fields ("
                                     (string/join ", " unagged)
                                     ") must be grouped or aggregated")))))
  [`(~(make-aggregator bound-context []), ~code)])

(defmethod visit :SELECT_ [[[_ & args] parse-context]]
  (let [parse-context (assoc parse-context
                        :statement-type :select
                        :context :select)
        by-key (into {} (for [[key & _ :as node] args] [key node]))

        [from-code from-context] (visit [(:FROM by-key) parse-context])
        bound-context from-context

        [where-code _] (visit [(:WHERE by-key) from-context])
        code (if where-code `(~where-code ~from-code) from-code)

        [field-code field-context] (visit [(:FIELD_LIST by-key) bound-context])

        new-binding (:binding field-context)
        alias-name (:alias-name parse-context)
        new-binding (if alias-name
                      (qualify-keys-for-view alias-name new-binding)
                      new-binding)
        code `(map ~field-code ~(or code [{}]))
        bound-context (assoc parse-context
                        :binding new-binding
                        :field-idx-lkup (:field-idx-lkup field-context))
        bound-context (merge field-context bound-context)

        code (reduce
               (fn [code key]
                 (let [[transform-func _]
                       (visit [(get by-key key) bound-context])]
                   (if transform-func
                     `(~transform-func ~code)
                     (if (and (= key :GROUP_BY)
                              (not-empty (:agg-cols bound-context)))
                       (do-ungrouped-agg code bound-context)
                       code))))
               code
               [:GROUP_BY, :ORDER_BY, :LIMIT])
        code (if alias-name
               `(qualify-data-for-view ~alias-name ~code)
               code)]

    [code bound-context]))

(defmethod visit :UNION [[[_ & args] parse-context]]
  (when-not (= (string/upper-case (second args)) "ALL")
    (throw (RuntimeException. "TODO UNION not-ALL not implemented yet")))
  (let [[left-code left-ctxt] (visit [(first args) parse-context])
        [right-code right-ctxt] (visit [(last args) parse-context])]
    (when-not (= (:binding right-ctxt) (:binding left-ctxt))
      (throw (RuntimeException. "TODO cannot union different schemas")))
    [`(concat ~left-code ~right-code) right-ctxt]))

(let [number-type-lkup {[:integer :integer] :integer,
                        [:integer :float]   :float,
                        [:float :integer]   :float,
                        [:float :float]     :float}
      comparison-func
      (fn [[t1 t2 :as types]]
          (if (and (= (count types) 2)
                   (or (= t1 t2) (contains? number-type-lkup types)))
            :bool))
      string->string (fn [args] (when (and (= 1 (count args))
                                           (= (first args) :string))
                                      :string))]
  (def func-type-lookup
    {:or {[:bool :bool] :bool}
     :and {[:bool :bool] :bool}
     := comparison-func
     :> comparison-func
     :>= comparison-func
     :< comparison-func
     :<= comparison-func
     :is (fn [args] (when (= (count args) 2) :bool))  ; any two types
     :+ number-type-lkup
     :* number-type-lkup
     :/ number-type-lkup
     :- number-type-lkup

     :sum #(when (and (= (count %) 1) (#{:integer :float} (first %)))
            (first %))
     :avg #(when (#{:integer :float} (first %)) :float)
     :count (fn [& _] :integer) ; any combo of columns can be counted

     :concat (fn [args]
               (when (and (>= (count args) 2) (every? #{:string} args))
                 :string))
     :lower string->string
     :upper string->string}))

(defn check-return-type! [func & arg-types]
  (let [type-val-getter (get func-type-lookup func)
        return-type (if type-val-getter
                      ; if any literal NULLs in argument, just return NULL
                      ; TODO: this obviously not great
                      (or (some #{:null} arg-types)
                          (get type-val-getter arg-types)
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

        agg-tmp-dims (mapcat #(-> % second :agg-tmp-dims) all-new-fields)
        aggs-and-dims (group-by #(-> % second :agg? boolean) all-new-fields)
        agg-funcs (mapcat #(-> % second :agg-funcs) all-new-fields)

        all-dims (concat
                   agg-tmp-dims
                   (map
                     (fn [[code ctxt]]
                       [(:field-name ctxt) code])
                     (get aggs-and-dims false)))
        agg-cols (map
                   (fn [[code ctxt]]
                     [(:field-name ctxt) code])
                   (get aggs-and-dims true))

        selected-field-code (into {} all-dims)
        code `(fn [~row-sym] ~selected-field-code)

        field-idx-lkup (into
                         {}
                         (map-indexed
                           (fn [idx [_ ctxt]]
                             [(+ 1 idx) (:field-name ctxt)])
                           all-new-fields))
        new-binding (into binding-map
                          (map
                            (fn [[_ ctxt]]
                              [(:field-name ctxt) (:type ctxt)])
                            all-new-fields))]
    [code, (assoc parse-context
             :binding new-binding
             :field-idx-lkup field-idx-lkup
             :agg-cols agg-cols
             :agg-funcs agg-funcs)]))

(defmethod visit :FROM [[[_ & args] parse-context]]
  (let [parse-context (assoc parse-context :context :from)]
    (visit [(first args) parse-context])))

(defmethod visit :WHERE [[[_ & args] parse-context]]
  (let [row-sym (gensym)
        parse-context (assoc parse-context :row-sym row-sym)
        [expr-code expr-ctxt] (visit [(first args) parse-context])]
    (when-not (= (:type expr-ctxt) :bool)
      (throw (RuntimeException. "Must have boolean in WHERE clause")))
    [`(fn [rows#] (filter #(let [~row-sym %] ~expr-code) rows#)),
     expr-ctxt]))

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


(defn vec-for-grouping [row-sym args parse-context]
  (let [by-idx (:field-idx-lkup parse-context)
        ctxt-for-expr (assoc parse-context :row-sym row-sym)]
    (mapv
      (fn [arg]
        (let [[code _] (visit [arg ctxt-for-expr])]
          (if (integer? code)
            (let [kw-for-idx (get by-idx code)]
              (if-not (nil? kw-for-idx)
                `(~kw-for-idx ~row-sym)
                (throw
                  (RuntimeException. "TODO bad group idx"))))
            code)))
      args)))

(defmethod visit :GROUP_BY [[[_ & args] parse-context]]
  (let [row-sym (gensym)
        group-vec (vec-for-grouping row-sym args parse-context)
        ; a bit of an abuse of what this vector was intended for
        group-col-keys (mapv first group-vec)

        aggregator (make-aggregator parse-context group-col-keys)
        grouping-fn `(fn [~row-sym] ~group-vec)
        ungrouped-unagged (-> (:binding parse-context)
                              keys
                              set
                              (set/difference
                                (set (map first (:agg-cols parse-context)))
                                (set group-col-keys))
                              not-empty)]

    (when ungrouped-unagged
      (throw (RuntimeException. (str "Fields ("
                                     (string/join " " ungrouped-unagged)
                                     ") must be grouped or aggregated."))))
    [`#(->>
        %
        (group-by ~grouping-fn)
        vals
        (map ~aggregator))
     parse-context]))

(defmethod visit :ORDER_BY [[[_ & args] parse-context]]
  (let [row-sym (gensym)
        sort-vec (vec-for-grouping row-sym args parse-context)
        sorter-fn `(fn [~row-sym] ~sort-vec)]
    [`#(sort-by ~sorter-fn %)
     parse-context]))

(defmethod visit :LIMIT [[[_ & args] parse-context]]
  (let [limit-num (first (visit [(first args) parse-context]))]
    [`#(take ~limit-num %) parse-context]))

(defmethod visit :VIEW [[[_ & args] parse-context]]
  (let [parse-context (assoc parse-context :context :from) ; TODO: or :view?
        [_ alias-ctxt] (visit [(second args) parse-context])
        alias-name (:view-name alias-ctxt)
        [data-code data-ctxt] (visit [(first args) (assoc parse-context
                                                     :alias-name alias-name)])
        data-ctxt (update data-ctxt :binding merge (:binding parse-context))
        view-name (or alias-name (:view-name data-ctxt))]

    (when-not view-name
      (throw (RuntimeException. "TODO every derived table blah blah alias")))
    [data-code, (assoc data-ctxt :view-name view-name)]))

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
                      ; but explicitness better to avoid confusion later
                      :new-view-name (:view-name view-ctxt)
                      :new-data-sym new-data-sym
                      :join-strategy (-> by-key :JOIN_STRAT first (or :INNER))
                      :join-direction (-> by-key
                                          :JOIN_DIRECTION
                                          second
                                          first))

        [on-code _] (visit [(:ON by-key) ctxt-for-on])
        on-code `(let [~new-data-sym ~view-code] ~on-code)]
    [on-code, view-ctxt]))

(defn- form-only-has-view-name? [form view-name]
  (let [views-used (-> form meta :views-used)]
    ; this 'works' if views-used in nil -> should it?
    (empty? (set/difference views-used #{view-name}))))

(defn- subseq-vals [func]
  [#(reduce concat (vals (subseq %1 func %2))), true])

(def supported-split-funcs
  {:= [`get false]
   :> `(subseq-vals >)
   :>= `(subseq-vals >=)
   :< `(subseq-vals <)
   :<= `(subseq-vals <=)})

(defn split-on-code-across-join [expr-code joining-view-name]
  (if-not (= 3 (count expr-code))
    ; form was not a binary function, we are screwed
    []
    (let [[func form1 form2] expr-code
          forms [form1 form2]
          func-kw (-> func str (string/split #"/") last keyword)
          single-view-form (map
                             #(form-only-has-view-name? % joining-view-name)
                             forms)]
      (if (or (not (contains? supported-split-funcs func-kw))
              (every? true? single-view-form))
        ; cannot split expression by views, screwed again
        ; TODO: this isn't quite right:
        ; "ON foo.bar + foo.baz = 2" aka
        ; (= (+ (:foo.bar row-sym) (:foo.baz row-sym)) 2) will get a
        ; false negative here and KABOOM down the line
        ; many more TODOs here: support trees of ORs/ANDs with splittable
        ; leaf conditions, optimize the above example to be splittable, etc.
        []
        [func-kw forms (if (first single-view-form) 1 0)]))))


(def func-swapper
  ; depending on who we're iterating over, will need to flip around the
  ; gt/lt -> this is 100% wrong I think, just need to remember to fix it
  {:>= :<=
   :> :<})


(defn join-with-grouping
  [func-kw forms left-idx left-data-sym right-data-sym row-sym parse-context]
  (let [[lform rform] (if (= left-idx 0) forms (reverse forms))
        [iter-sym lkup-sym] (map symbol ["iter-data" "lkup-data"])
        ; reiterating, have not thought through the below line at all
        func-kw (if (= left-idx 1) (get func-swapper func-kw func-kw) func-kw)
        [transformed-func sort-required] (get supported-split-funcs func-kw)
        [ksym vsym rsym] (map symbol ["k" "v" "r"])
        inner-only? (nil? (:join-direction parse-context))
        ; if inner join, which one we iterate over doesn't really matter
        vals-of-iters (if (= (:join-direction parse-context) :LEFT)
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
         (fn [[~ksym ~vsym]]
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
        [iter-sym other-sym] (if (= (:join-direction parse-context) :LEFT)
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
        [code ctxt] (visit [(first args) ctxt-for-child])

        ; TODO: this may be a suboptimal way of passing around context
        ; although, per comment below, if we do do this there's another
        ; todo to add in the line numbers to the meta for more error reporting
        views-used (:views-used ctxt)
        code (if (and (coll? code) views-used)
               (with-meta code {:views-used views-used})
               code)]
    ; yeah, this should probably use the SQL like a real thing
    [code (update ctxt :field-name #(or % (gen-field-name)))]))

(defn do-visit-function [func-code func-ctxt arg-code arg-ctxts]
  (let [agg? (:agg? func-ctxt)
        tmp-dim-names (when agg?
                        (->> (repeatedly gensym)
                             (take (count arg-code))
                             (map str)
                             (map keyword)))
        agg-col-name (-> (gensym) str keyword)
        agg-tmp-dims (when tmp-dim-names
                       (map vector tmp-dim-names arg-code))
        agg-funcs (when tmp-dim-names
                    [(->AggFunctionInst
                       (:agg-init-func func-ctxt)
                       (:agg-incr-func func-ctxt)
                       agg-col-name
                       tmp-dim-names)])
        arg-code (if tmp-dim-names
                   [`(~agg-col-name ~agg-row-sym)]
                   arg-code)

        type (apply check-return-type!
                    (:function func-ctxt)
                    (map :type arg-ctxts))
        any-child-aggs? (some :agg? arg-ctxts)]

    (when (and agg? any-child-aggs?)
          (throw (RuntimeException. "TODO Can't nest aggregates")))
    [`(~func-code ~@arg-code),
     (assoc func-ctxt
       :type type
       :agg? (or agg? any-child-aggs?)
       :agg-tmp-dims (concat agg-tmp-dims (mapcat :agg-tmp-dims arg-ctxts))
       :agg-funcs (concat agg-funcs (mapcat :agg-funcs arg-ctxts)))]))

(defmethod visit :FUNCTION_CALL [[[_ & args] parse-context]]
  (let [[func-code func-ctxt] (visit [(first args) parse-context])

        arg-info (map #(visit [% parse-context]) (rest args))
        arg-code (map first arg-info)
        arg-ctxts (map second arg-info)]
    (do-visit-function func-code func-ctxt arg-code arg-ctxts)))

(defmethod visit :FUNCTION [[[_ & args] parse-context]]
  (let [func-name-raw (first args)
        func-name (keyword (string/lower-case func-name-raw))
        agg? (contains? sql-functions/aggregates func-name)
        [agg-incr-func agg-init-func func-sym]
        (if agg?
          (get sql-functions/aggregates func-name)
          [nil, nil, (get sql-functions/func-lkup func-name)])]

    (when-not func-sym
      (throw (RuntimeException.
               (format "Unknown function '%s'" func-name-raw))))
    [func-sym (assoc parse-context
                :function func-name
                :agg? (boolean agg-init-func)
                :agg-init-func agg-init-func
                :agg-incr-func agg-incr-func)]))

(defmethod visit :BIN_OP_CALL [[[_ & args] parse-context]]
  (let [visit #(visit [% parse-context])
        [[e1 e1-ctxt] [op op-ctxt] [e2 e2-ctxt]] (map visit args)]
    (do-visit-function op op-ctxt [e1 e2] [e1-ctxt e2-ctxt])))

(defmethod visit :BIN_OP [[[_ & args] parse-context]]
  (let [func-string (str/lower-case (first args))
        func-sym (condp = func-string
                    "is" (symbol "=")
                    ; we can nicely cheat -> every other SQL binary operation
                    ; happens to have the same name in clojure
                    ; TODO: can't actually cheat, need to make them nullable
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
  (let [elements (-> field-name name split-identifier)
        name-lkup (binding-by-name (:binding parse-context))]
    (or
      (reduce
        (fn [_ idx]
          (let [back (str/join "." (drop idx elements))
                matching-cols (get name-lkup back)
                matching-cols-count (count matching-cols)]
            (cond
              (= matching-cols-count 1)
              (reduced (first matching-cols))
              (> matching-cols-count 1)
              (throw (RuntimeException. "TODO ambigious cols"))
              :else
              nil)))
        nil
        (range 0 (count elements)))
      (throw (RuntimeException. (format "no such col '%s'" field-name))))))

(defmethod visit :IDENTIFIER [[[_ & args] parse-context]]
  (if (= (:context parse-context) :expression)
    (let [field-name (keyword (str/join "." args))
          row-sym (:row-sym parse-context)
          field-name-in-ctxt (fully-qualified-name field-name parse-context)

          view-name (->> field-name-in-ctxt
                         split-identifier
                         (drop-last 1)
                         (string/join ".")
                         keyword)
          parse-context (update parse-context
                                :views-used
                                #(conj (or % #{}) view-name))

          type (get-in parse-context [:binding field-name-in-ctxt])]
      [`(~field-name-in-ctxt ~row-sym)
       (assoc parse-context
         ; :field-name is the one displayed if no alias, so don't qualify
         :field-name field-name
         :type type)])
    ; assuming ctxt :from -> change if more types later
    (let [view-name (or (:alias-name parse-context) (string/join "." args))]
      [`(load-qualified-view ~(vec args) ~view-name)
       (assoc parse-context
         :binding (binding-from-qualified-view args view-name)
         :view-name view-name)])))

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


