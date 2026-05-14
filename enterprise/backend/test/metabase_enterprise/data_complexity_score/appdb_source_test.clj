(ns metabase-enterprise.data-complexity-score.appdb-source-test
  "Integration tests for the raw-JDBC entity loader. Cross-checks the new path against the cron's
  Toucan-based `complexity/enumerate-catalogs` to prove they agree on the same appdb.

  These tests run under `mt/test-helpers-set-global-values!` so `mt/with-temp` actually commits
  + cleans up — the default rollback-on-exit mode binds rows to the Toucan transaction
  connection, where a raw-JDBC reader hitting `(mdb/data-source)` from a fresh pool connection
  can't see them. Without committed writes, the cross-source comparison is degenerate (both
  paths returning empty isn't an agreement, it's a missed bug)."
  (:require
   [clojure.test :refer :all]
   [metabase-enterprise.data-complexity-score.appdb-source :as appdb-source]
   [metabase-enterprise.data-complexity-score.complexity :as complexity]
   [metabase.app-db.core :as mdb]
   [metabase.collections.core :as collections]
   [metabase.test :as mt]
   [metabase.util.json :as json]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as jdbc.rs]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

(defn- entity-keys
  "Project an entity map to the keys that identify it for cross-source comparison. Ignores
  ordering; only what's in the entity matters, not whether it shares identity with a sibling
  vector. We deliberately leave `:measure-names` as a set (the JDBC path's `GROUP BY` doesn't
  promise an order)."
  [{:keys [id name kind field-count measure-names]}]
  {:id            id
   :name          name
   :kind          kind
   :field-count   field-count
   :measure-names (set measure-names)})

(defn- entity-set [entities]
  (set (map entity-keys entities)))

(deftest ^:sequential jdbc-loader-matches-toucan-enumerate-test
  (testing "appdb-source/load-from-jdbc returns the same library + universe entities as complexity/enumerate-catalogs"
    ;; This is the key safety net: if the raw-SQL queries drift from what Toucan's model layer
    ;; produces (missing column, wrong WHERE clause, misnamed table), this assertion catches it.
    ;; Fixtures intentionally exercise:
    ;;   - a Library collection with a descendant Data collection (location-path traversal)
    ;;   - an out-of-library collection (negative path — must not leak into :library)
    ;;   - an inactive Table and an archived Card (negative path — must not appear in :universe)
    ;;   - a Table outside the Library (positive in :universe, negative in :library)
    (mt/test-helpers-set-global-values!
      (mt/with-temp
        [:model/Collection {lib-id :id}      {:type     collections/library-collection-type
                                              :name     "Library"
                                              :location "/"}
         :model/Collection {data-id :id}     {:type     collections/library-data-collection-type
                                              :name     "Data"
                                              :location (format "/%d/" lib-id)}
         :model/Collection {mets-id :id}     {:type     collections/library-metrics-collection-type
                                              :name     "Metrics"
                                              :location (format "/%d/" lib-id)}
         :model/Collection {outside-id :id}  {:name "Outside" :location "/"}
         :model/Database   {db-id :id}       {:name "JDBC loader test DB"}
       ;; Library Table (active, published, inside Library tree) — counts in both catalogs.
         :model/Table      {orders-id :id}   {:db_id db-id :name "orders" :active true
                                              :is_published true :collection_id data-id}
       ;; Library Card (metric in Library Metrics collection) — counts in both.
         :model/Card       _                 {:database_id db-id :type :metric :name "Revenue"
                                              :archived false :collection_id mets-id}
       ;; Universe-only Table (active, NOT in Library) — counts only in :universe.
         :model/Table      {events-id :id}   {:db_id db-id :name "events" :active true
                                              :is_published true :collection_id outside-id}
       ;; Inactive Table — must not appear anywhere.
         :model/Table      _                 {:db_id db-id :name "inactive_table" :active false}
       ;; Archived Card — must not appear anywhere.
         :model/Card       _                 {:database_id db-id :type :metric :name "Old Metric"
                                              :archived true :collection_id mets-id}
       ;; Active fields on `orders` (2) and `events` (3) — drive :field-count.
         :model/Field      _                 {:table_id orders-id :name "id"     :active true}
         :model/Field      _                 {:table_id orders-id :name "total"  :active true}
         :model/Field      _                 {:table_id orders-id :name "legacy" :active false}
         :model/Field      _                 {:table_id events-id :name "id"     :active true}
         :model/Field      _                 {:table_id events-id :name "type"   :active true}
         :model/Field      _                 {:table_id events-id :name "ts"     :active true}
       ;; A Measure on orders (non-archived) and an archived one that must not appear.
         :model/Measure    _                 {:table_id orders-id :name "revenue" :archived false}
         :model/Measure    _                 {:table_id orders-id :name "stale"   :archived true}]
        (let [via-jdbc                       (appdb-source/load-from-jdbc)
              {:keys [library universe]}     (#'complexity/enumerate-catalogs nil)]
          (testing "universe entity set matches between the two readers"
            (is (= (entity-set universe)
                   (entity-set (:universe-entities via-jdbc)))
                "the raw-JDBC reader must surface the same Universe entities as the Toucan reader"))
          (testing "library entity set matches between the two readers"
            (is (= (entity-set library)
                   (entity-set (:library-entities via-jdbc)))
                "the raw-JDBC reader must surface the same Library entities as the Toucan reader"))
          (testing "orders Table picks up its 2 active fields and its 1 non-archived measure"
            (let [orders (first (filter #(= "orders" (:name %))
                                        (:library-entities via-jdbc)))]
              (is (some? orders))
              (is (= 2 (:field-count orders))     "inactive field must be excluded")
              (is (= ["revenue"] (:measure-names orders)) "archived measure must be excluded"))))))))

(deftest ^:sequential record-score-roundtrips-json-via-raw-jdbc-test
  (testing "record-score! inserts a row whose score_data round-trips back through raw SQL as the expected JSON"
    (let [fp     (str "appdb-source-test/fp-" (random-uuid))
          source "appdb-cli-test"
          score  {:library  {:total 1 :components {:entity-count {:measurement 1.0 :score 10}}}
                  :universe {:total 1 :components {:entity-count {:measurement 1.0 :score 10}}}
                  :metabot  {:total 1 :components {:entity-count {:measurement 1.0 :score 10}}}
                  :meta     {:formula-version   1
                             :synonym-threshold 0.8
                             :weights           {:entity 10}}}
          inserted-id (appdb-source/record-score! fp source score)]
      (try
        (is (some? inserted-id) "raw INSERT should surface the generated id")
        (let [row (jdbc/execute-one!
                   (mdb/data-source)
                   [(str "SELECT fingerprint, source, score_data "
                         "FROM data_complexity_score WHERE id = ?") inserted-id]
                   {:builder-fn jdbc.rs/as-unqualified-lower-maps})]
          (is (=? {:fingerprint fp
                   :source      source}
                  row))
          (testing "score_data is stored as the JSON we serialized; round-trips via `json/decode`"
            ;; The column is `${text.type}` which maps to `CLOB` on H2 and TEXT on Postgres/MySQL,
            ;; so the JDBC driver hands us either a `String` or a `Clob` depending on the appdb —
            ;; `clob->str` normalizes both.
            (let [raw     (mdb/clob->str (:score_data row))
                  decoded (json/decode raw true)]
              (is (= 1 (get-in decoded [:library :total])))
              (is (= 1 (get-in decoded [:meta :formula-version]))))))
        (finally
          ;; Append-only table — clean up manually so the test doesn't accumulate rows.
          (t2/delete! :model/DataComplexityScore :id inserted-id))))))
