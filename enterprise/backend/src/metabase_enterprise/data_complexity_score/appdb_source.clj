(ns metabase-enterprise.data-complexity-score.appdb-source
  "Raw-JDBC entity loader and score writer for the Data Complexity CLI's `--source appdb` mode.

  Why a parallel path? The cron and API run against an appdb at the same Metabase version, so they
  can trust Toucan model hooks (`:after-select`, transforms, settings cache fill-ins, etc.). The
  CLI must stay safe even when a newer Metabase binary is pointed at an older appdb — e.g. a v73
  CLI sampling a v54 appdb to compute a comparative score. In that mismatched-versions scenario
  any v73 model hook that quietly normalizes a JSON column on read, warms a setting cache and
  writes back a defaulted value, or runs an encryption migration would silently mutate v54 data
  in ways the v54 binary doesn't expect.

  This namespace's contract: at most one INSERT, into `data_complexity_score`. Nothing else
  touches the appdb. We use `next.jdbc/execute!` against `(mdb/data-source)`, never call any
  `t2/select`/`t2/insert!`/`t2/update!`, and don't `finish-db-setup!` (so the app DB's `:status`
  stays at `::not-finished` and any accidental Toucan call fails loudly rather than silently).

  Scope reductions vs the cron path:
    - No synonym-axis (`embedder` is always nil). Synonym scoring needs the settings cache and
      semantic-search state; both are no-go.
    - No `:metabot` catalog. It would need the Metabot Toucan row + a premium-feature flag, both
      of which read settings. `score-from-entities` falls back to `:universe` when
      `:metabot-entities` is nil, which is correct for an admin who hasn't constrained Metabot.
    - No Snowplow emission. CLI is an operator diagnostic, not telemetry.
    - No fingerprint advance. The cron's `last-fingerprint` setting is its skip-when-unchanged
      gate — irrelevant to a manual run, and writing it would mutate `setting`.

  Table-name constants are inlined to avoid loading model namespaces (where the
  `t2/table-name` defmethods live alongside `:before-select`/`:after-select` hooks)."
  (:require
   [metabase.app-db.core :as mdb]
   [metabase.util.json :as json]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as jdbc.rs])
  (:import
   (java.sql Timestamp)))

(set! *warn-on-reflection* true)

;;; ----------------------------------- inlined constants -----------------------------------
;;;
;;; All of these match the corresponding Toucan model's `t2/table-name` / typed constants. Inlining
;;; (rather than requiring the model namespaces) keeps `:before-select`/`:after-select` hooks out
;;; of the loaded set.

(def ^:private audit-db-id              13371337) ; metabase.audit-app.impl/audit-db-id
(def ^:private library-collection-type  "library") ; metabase.collections.models.collection/library-collection-type
(def ^:private card-table               :report_card)
(def ^:private table-table              :metabase_table)
(def ^:private field-table              :metabase_field)
(def ^:private database-table           :metabase_database)
(def ^:private collection-table         :collection)
(def ^:private measure-table            :measure)
(def ^:private moderation-review-table  :moderation_review)
(def ^:private score-table              :data_complexity_score)

;;; ----------------------------------- query helper -----------------------------------

(defn- query
  "Run a HoneySQL map against the appdb data source. Returns rows as plain maps with unqualified,
  lowercase keyword keys (e.g. `:id`, `:collection_id`) regardless of how the JDBC driver cases
  column names — matches what the cron's Toucan path produces, so downstream entity-shaping is
  identical between the two."
  [data-source honey-map]
  (jdbc/execute! data-source (mdb/compile honey-map)
                 {:builder-fn jdbc.rs/as-unqualified-lower-maps}))

;;; ----------------------------------- entity shaping -----------------------------------

(defn- ->card-entity
  "Mirror of the private `complexity/->card-entity`. Cards don't contribute to `:field-count` or
  `:measure-names` — those are Table-side, see `complexity/->card-entity` for the reasoning."
  [{:keys [id name type]}]
  {:id            id
   :name          name
   :kind          (keyword type)
   :field-count   0
   :measure-names []})

(defn- ->table-entity
  "Mirror of the private `complexity/->table-entity`."
  [field-counts measure-names {:keys [id name]}]
  {:id            id
   :name          name
   :kind          :table
   :field-count   (get field-counts id 0)
   :measure-names (get measure-names id [])})

(defn- pick-by-row
  "Index-parallel filter mirroring `complexity/pick-by-row`. Preserves entity-map identity across
  the universe and library projections so a Card/Table that appears in both is one map in memory.
  Nil `row-pred` short-circuits — used when the library collection doesn't exist yet."
  [row-pred rows entities]
  (if row-pred
    (into []
          (keep-indexed (fn [i row] (when (row-pred row) (nth entities i))))
          rows)
    []))

;;; ----------------------------------- raw-SQL reads -----------------------------------

(defn- library-collection-id
  "Id of the (singleton) library collection, or nil if the instance has no Library yet. The cron's
  `collections/library-collection` uses `t2/select-one` with model post-select hooks; we just need
  the bare id, which `:type = 'library'` uniquely identifies."
  [ds]
  (-> (query ds {:select [:id]
                 :from   [collection-table]
                 :where  [:= :type library-collection-type]
                 :limit  1})
      first
      :id))

(defn- descendant-collection-ids
  "Ids of all collections beneath `root-id` via the `location` materialized path. Matches what
  `collections/descendant-ids` produces but skips the model layer entirely."
  [ds root-id]
  (when root-id
    (into #{}
          (map :id)
          (query ds {:select [:id]
                     :from   [collection-table]
                     ;; Collection `location` is a `/`-delimited path of ancestor ids — a descendant
                     ;; of root 42 has `location LIKE '/42/%'`.
                     :where  [:like :location (str "/" root-id "/%")]}))))

(defn- library-scope-collection-ids [ds]
  (when-let [root (library-collection-id ds)]
    (conj (descendant-collection-ids ds root) root)))

(defn- universe-cards [ds]
  (query ds {:select [:id :name :type :collection_id]
             :from   [card-table]
             :where  [:and
                      [:in :type ["metric" "model"]]
                      [:= :archived false]
                      [:not= :database_id audit-db-id]]}))

(defn- universe-tables [ds]
  (query ds {:select [:id :name :collection_id :is_published]
             :from   [table-table]
             :where  [:and
                      [:= :active true]
                      [:not= :db_id audit-db-id]]}))

(defn- table-field-counts
  "Active-field counts grouped by `table_id`. Returns `{table-id field-count}`."
  [ds table-ids]
  (if (empty? table-ids)
    {}
    (into {}
          (map (juxt :table_id :field_count))
          (query ds {:select   [:table_id [[:count :*] :field_count]]
                     :from     [field-table]
                     :where    [:and
                                [:= :active true]
                                [:in :table_id table-ids]]
                     :group-by [:table_id]}))))

(defn- table-measure-names
  "Non-archived measure names grouped by `table_id`. Returns `{table-id [name ...]}`."
  [ds table-ids]
  (if (empty? table-ids)
    {}
    (reduce (fn [acc {:keys [table_id name]}]
              (update acc table_id (fnil conj []) name))
            {}
            (query ds {:select [:table_id :name]
                       :from   [measure-table]
                       :where  [:and
                                [:= :archived false]
                                [:in :table_id table-ids]]}))))

;;; ----------------------------------- public loader -----------------------------------

(defn load-from-jdbc
  "Read the universe + library entities the scorer needs, returning
  `{:library-entities [...] :universe-entities [...]}` with the same per-entity shape that
  `complexity/score-from-entities` consumes. No Toucan, no settings, no Snowplow."
  []
  (let [ds                (mdb/data-source)
        ;; Universe rows first; library is an in-memory projection over the same rows so a Card
        ;; or Table that appears in both is shared by reference (matches `enumerate-catalogs`).
        cards             (universe-cards ds)
        tables            (universe-tables ds)
        table-ids         (mapv :id tables)
        field-counts      (table-field-counts  ds table-ids)
        measure-names     (table-measure-names ds table-ids)
        card-entities     (mapv ->card-entity cards)
        table-entities    (mapv #(->table-entity field-counts measure-names %) tables)
        library-cids      (library-scope-collection-ids ds)
        in-library-card?  (when (seq library-cids)
                            (fn [{:keys [collection_id]}]
                              (contains? library-cids collection_id)))
        in-library-table? (when (seq library-cids)
                            (fn [{:keys [collection_id is_published]}]
                              (and is_published
                                   (contains? library-cids collection_id))))]
    {:universe-entities (into card-entities table-entities)
     :library-entities  (into (pick-by-row in-library-card?  cards  card-entities)
                              (pick-by-row in-library-table? tables table-entities))}))

;;; ----------------------------------- raw-SQL write -----------------------------------

(defn record-score!
  "Insert one row into `data_complexity_score` via raw JDBC. No Toucan transforms, no model
  hooks. `score-data` is serialized to JSON here so we don't depend on `mi/transform-json`.

  Returns the row's `id`, or nil if the driver doesn't surface it."
  [fingerprint source score-data]
  (let [ds  (mdb/data-source)
        row {:fingerprint fingerprint
             :source      source
             :score_data  (json/encode score-data)
             :created_at  (Timestamp. (System/currentTimeMillis))}
        ;; `execute-one!` returns the inserted row when the driver supports `RETURN_GENERATED_KEYS`
        ;; (postgres/h2/mysql all do for an autoincrement pk); we surface the id for the CLI's
        ;; printed result so the operator can correlate with rows in the table.
        result (jdbc/execute-one! ds (mdb/compile {:insert-into [score-table]
                                                   :values      [row]})
                                  {:return-keys true
                                   :builder-fn  jdbc.rs/as-unqualified-lower-maps})]
    (:id result)))
