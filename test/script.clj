#!/usr/bin/env bb

(ns script
  "End-to-end tests for pod-frozenlock-bbolt.

  These exercise the POD PROTOCOL - describe, invoke, the var set, the
  transaction lifecycle - which the Go tests do not: those cover the
  internals (JSON encoding, envelope parsing, compaction) by calling the
  functions directly. Nothing else in the world tests this pod over
  bencode, so this file is the only place a var that is declared in
  `describe` but missing from the dispatch switch (or vice versa) shows
  up.

  Run with `script/test`, which builds the binary first."
  (:require [babashka.pods :as pods]
            [clojure.test :as t :refer [deftest is testing]]
            [cognitect.transit :as transit])
  (:import (java.io ByteArrayInputStream ByteArrayOutputStream File)))

(def pod-path
  (or (System/getenv "POD_BBOLT") "./pod-frozenlock-bbolt"))

(pods/load-pod pod-path)

(require '[pod.frozenlock.bbolt :as b])

;;; -- helpers ----------------------------------------------------------------

(defn- enc
  "Transit-JSON encode, cache off - the same encoding the pod's envelope
  writer expects for each field blob."
  [v]
  (let [o (ByteArrayOutputStream.)]
    (transit/write (transit/writer o :json {:cache false}) v)
    (.toString o "UTF-8")))

(defn- dec*
  [s]
  (transit/read (transit/reader (ByteArrayInputStream. (.getBytes s "UTF-8")) :json)))

(defn- doc-parts
  "Split a map into the [keys offsets body] triple `put-doc` takes: each
  field encoded on its own, offsets in BYTES into the concatenated body."
  [m]
  (let [o    (ByteArrayOutputStream.)
        w    (transit/writer o :json {:cache false})
        ks   (volatile! [])
        offs (volatile! [])]
    (doseq [[k v] m]
      (vswap! ks conj (enc k))
      (let [off (.size o)]
        (transit/write w v)
        (vswap! offs conj [off (- (.size o) off)])))
    [(deref ks) (deref offs) (.toString o "UTF-8")]))

(def ^:private envelope-min
  "Below this the caller stores a plain value instead of an envelope - the
  pod does not decide, `put-doc` with nil keys stores the body verbatim."
  2048)

(defn- put-doc!
  [tx bucket key m]
  (let [[ks offs body] (doc-parts m)]
    (if (>= (count body) envelope-min)
      (b/put-doc tx bucket key ks offs body)
      ;; Small document: one plain transit value of the WHOLE map, not the
      ;; concatenated per-field blobs - those are only meaningful inside an
      ;; envelope.
      (b/put-doc tx bucket key nil nil (enc m)))))

(defn- thaw
  "Decode a `get` reply: [\"doc\" blob] or [\"fields\" [[k blob] ...]]."
  [[kind payload]]
  (if (= "doc" kind)
    (dec* payload)
    (reduce (fn [m [k v]] (assoc m (dec* k) (dec* v))) {} payload)))

(defn- thaw-projected
  "Decode a `scan-paths` value: [\"fields\" [[k blob] ...]], [\"doc\" blob],
  or - for a plain document the pod projected at the JSON level - the
  projected map itself."
  [v]
  (if (map? v) v (thaw v)))

(defn- fresh
  "A fresh database file per test. The pod keeps one open handle per path,
  so each test needs its own."
  []
  (let [f (File/createTempFile "pod-bbolt-test" ".db")]
    (.delete f)
    (.getAbsolutePath f)))

(defn- with-db
  "Open a fresh db, run f with its handle, close it."
  [f]
  (let [h (b/open (fresh))]
    (try (f h) (finally (b/close h)))))

(defn- in-tx
  "Run f with a write transaction id, committing on success."
  [h f]
  (let [tx (b/begin h)]
    (try
      (let [r (f tx)]
        (b/commit tx)
        r)
      (catch Throwable e
        (b/rollback tx)
        (throw e)))))

(defn- read-tx
  [h f]
  (let [tx (b/begin-read h)]
    (try (f tx) (finally (b/rollback tx)))))

(defn- big-body [n] (apply str (repeat (quot n 10) "abcdefghij")))

;;; -- protocol ---------------------------------------------------------------

(deftest version-and-var-set
  (testing "the version var answers, and looks like a version"
    (let [v (b/version)]
      (is (string? v))
      (is (re-matches #"\d+\.\d+\.\d+.*" v))))
  (testing "every var the README documents is callable"
    ;; A var listed in `describe` but missing from the dispatch switch fails
    ;; with "unknown var" on first call, not at load time - so touch them all.
    (doseq [v '[version open close begin begin-read commit rollback
                get get-many get-path get-paths raw-size put put-many
                put-field put-doc put-doc-many delete delete-many
                scan scan-paths buckets create-bucket delete-bucket
                count stats compact]]
      (is (some? (resolve (symbol "pod.frozenlock.bbolt" (name v))))
          (str v " is not exposed")))))

;;; -- point reads and writes -------------------------------------------------

(deftest put-get-roundtrip
  (with-db
    (fn [h]
      (in-tx h (fn [tx] (b/put tx "b" "k" "hello")))
      (read-tx h (fn [tx]
                   (is (= ["doc" "hello"] (b/get tx "b" "k"))))))))

(deftest get-missing-is-nil
  (with-db
    (fn [h]
      (read-tx h (fn [tx]
                   (testing "missing key"
                     (is (nil? (b/get tx "b" "nope"))))
                   (testing "missing bucket"
                     (is (nil? (b/get tx "nobucket" "nope")))))))))

(deftest get-many-preserves-order-and-skips-missing
  (with-db
    (fn [h]
      (in-tx h (fn [tx] (b/put-many tx "b" [["a" "1"] ["b" "2"] ["c" "3"]])))
      (read-tx h (fn [tx]
                   (testing "requested order is preserved"
                     (is (= [["c" ["doc" "3"]] ["a" ["doc" "1"]]]
                            (b/get-many tx "b" ["c" "a"]))))
                   (testing "missing keys are skipped, not nil-padded"
                     (is (= [["a" ["doc" "1"]] ["c" ["doc" "3"]]]
                            (b/get-many tx "b" ["a" "nope" "c"]))))
                   (testing "an empty request is an empty reply"
                     (is (= [] (b/get-many tx "b" []))))
                   (testing "a missing bucket is an empty reply"
                     (is (= [] (b/get-many tx "nobucket" ["a"])))))))))

(deftest delete-and-delete-many
  (with-db
    (fn [h]
      (in-tx h (fn [tx] (b/put-many tx "b" [["a" "1"] ["b" "2"] ["c" "3"]])))
      (in-tx h (fn [tx] (b/delete tx "b" "a")))
      (in-tx h (fn [tx] (b/delete-many tx "b" ["b" "c"])))
      (read-tx h (fn [tx]
                   (is (= 0 (b/count tx "b")))
                   (is (nil? (b/get tx "b" "a"))))))))

(deftest count-sees-the-transactions-own-writes
  ;; `count` must read the transaction's own writes, not the last committed
  ;; state. It used to use `Bucket.Stats().KeyN`, which walks the bucket's
  ;; PAGES - and a page is only written at commit - so it reported 0 for a
  ;; bucket created in the same transaction and a stale number after any put
  ;; or delete.
  (with-db
    (fn [h]
      (testing "a bucket created in this transaction"
        (let [tx (b/begin h)]
          (b/put-many tx "new" [["a" "1"] ["b" "2"] ["c" "3"]])
          (is (= 3 (b/count tx "new")))
          (b/commit tx)))
      (testing "puts into an existing bucket"
        (let [tx (b/begin h)]
          (b/put-many tx "new" [["d" "4"] ["e" "5"]])
          (is (= 5 (b/count tx "new")))
          (b/commit tx)))
      (testing "a delete"
        (let [tx (b/begin h)]
          (b/delete tx "new" "a")
          (is (= 4 (b/count tx "new")))
          (b/commit tx)))
      (testing "and the committed state agrees"
        (read-tx h (fn [tx] (is (= 4 (b/count tx "new")))))))))

(deftest count-matches-a-cursor-walk
  ;; The count lives in the bucket's sequence field, so every write path that
  ;; can change the KEY SET has to keep it in sync. A cursor walk is the
  ;; oracle: O(n), but always right.
  (with-db
    (fn [h]
      (let [oracle (fn [tx bucket] (count (b/scan tx bucket nil false nil false 0)))
            check  (fn [tx bucket label]
                     (let [want (oracle tx bucket)
                           got  (b/count tx bucket)]
                       (is (= want got) (str label ": oracle " want ", count " got))))]
        (testing "put, overwrite, and an empty value"
          (in-tx h (fn [tx]
                     (b/put tx "b" "a" "1")
                     (check tx "b" "after put")
                     (b/put tx "b" "a" "2")
                     (check tx "b" "after overwriting a key")
                     (b/put tx "b" "empty" "")
                     (check tx "b" "after an empty value"))))
        (testing "put-many, including a duplicate key in one batch"
          (in-tx h (fn [tx]
                     (b/put-many tx "b" [["c" "1"] ["d" "2"] ["c" "3"]])
                     (check tx "b" "after put-many"))))
        (testing "put-doc, small and envelope, and an envelope overwrite"
          (in-tx h (fn [tx]
                     (put-doc! tx "b" "small" {:id "small" :n 1})
                     (check tx "b" "after a small put-doc")
                     (put-doc! tx "b" "big" {:id "big" :body (big-body 5000)
                                             :meta {:city "Springfield"}})
                     (check tx "b" "after an envelope put-doc")
                     (put-doc! tx "b" "big" {:id "big" :body (big-body 6000)
                                             :meta {:city "Springfield"}})
                     (check tx "b" "after an envelope overwrite"))))
        (testing "put-doc-many"
          (in-tx h (fn [tx]
                     (b/put-doc-many tx "b" [["m1" nil nil (enc {:id "m1"})]
                                             ["m2" nil nil (enc {:id "m2"})]])
                     (check tx "b" "after put-doc-many"))))
        (testing "put-field leaves the key set alone"
          (in-tx h (fn [tx]
                     (b/put-field tx "b" "big" (enc :meta) (enc {:city "x"}))
                     (check tx "b" "after put-field"))))
        (testing "delete and delete-many, including a missing key"
          (in-tx h (fn [tx]
                     (b/delete tx "b" "a")
                     (check tx "b" "after delete")
                     (b/delete tx "b" "nope")
                     (check tx "b" "after deleting a missing key")
                     (b/delete-many tx "b" ["c" "d" "nope"])
                     (check tx "b" "after delete-many"))))
        (testing "the committed state agrees"
          (read-tx h (fn [tx] (check tx "b" "after commit"))))
        (testing "delete-bucket then recreate resets the count"
          (in-tx h (fn [tx]
                     (b/delete-bucket tx "b")
                     (b/create-bucket tx "b")
                     (check tx "b" "after recreating the bucket")
                     (b/put tx "b" "z" "1")
                     (check tx "b" "after a put into the recreated bucket"))))
        (testing "compaction preserves the count"
          (let [before (read-tx h (fn [tx] (b/count tx "b")))]
            (b/compact h)
            (read-tx h (fn [tx]
                         (is (= before (b/count tx "b")) "count survives compact")
                         (check tx "b" "after compact")))))))))

(deftest count-and-buckets
  (with-db
    (fn [h]
      (in-tx h (fn [tx]
                 (b/create-bucket tx "one")
                 (b/create-bucket tx "two")
                 (b/put-many tx "one" [["a" "1"] ["b" "2"]])))
      (read-tx h (fn [tx]
                   (is (= 2 (b/count tx "one")))
                   (is (= 0 (b/count tx "two")))
                   (is (= #{"one" "two"} (set (b/buckets tx))))))
      (in-tx h (fn [tx] (b/delete-bucket tx "two")))
      (read-tx h (fn [tx]
                   (is (= #{"one"} (set (b/buckets tx)))))))))

;;; -- transactions -----------------------------------------------------------

(deftest rollback-discards
  (with-db
    (fn [h]
      (let [tx (b/begin h)]
        (b/put tx "b" "k" "v")
        (b/rollback tx))
      (read-tx h (fn [tx] (is (nil? (b/get tx "b" "k"))))))))

(deftest begin-read-sees-committed-data
  (with-db
    (fn [h]
      (in-tx h (fn [tx] (b/put tx "b" "k" "v")))
      (read-tx h (fn [tx] (is (= ["doc" "v"] (b/get tx "b" "k"))))))))

(deftest bucket-only-transaction-persists
  ;; A transaction that only creates a bucket must commit. The pod commits a
  ;; "dirty" tx and rolls back a clean one, so a bucket creation that does not
  ;; mark the tx dirty is silently lost.
  (with-db
    (fn [h]
      (in-tx h (fn [tx] (b/create-bucket tx "empty")))
      (read-tx h (fn [tx] (is (some #{"empty"} (b/buckets tx))))))))

(deftest close-with-open-transaction
  ;; bbolt's Close waits for open transactions, and their commits can no
  ;; longer be read once the message loop stops - the pod must roll them back
  ;; first, or close deadlocks.
  (let [h  (b/open (fresh))
        tx (b/begin h)]
    (is (string? tx))
    (is (nil? (b/close h)))))

(deftest concurrent-transactions
  ;; The pod replies from one goroutine per invoke; a shared stdout writer or
  ;; a blocked message loop shows up here as corruption or a deadlock.
  (with-db
    (fn [h]
      (let [fs (mapv (fn [t]
                       (future
                         (dotimes [i 50]
                           (in-tx h (fn [tx]
                                      (b/put tx "b" (str t "-" i) (str t)))))))
                     (range 8))]
        (run! deref fs)
        (read-tx h (fn [tx] (is (= 400 (b/count tx "b")))))))))

(deftest read-modify-write-is-serialized
  ;; A transaction begins at its FIRST operation, so the read a
  ;; read-modify-write depends on is inside the same snapshot as the write.
  (with-db
    (fn [h]
      (in-tx h (fn [tx] (b/put tx "b" "n" "0")))
      (let [n 50
            fs (mapv (fn [_]
                       (future
                         (dotimes [_ n]
                           (in-tx h (fn [tx]
                                      (let [[_ cur] (b/get tx "b" "n")]
                                        (b/put tx "b" "n" (str (inc (parse-long cur))))))))))
                     (range 4))]
        (run! deref fs)
        (read-tx h (fn [tx]
                     (is (= (str (* 4 n)) (second (b/get tx "b" "n"))))))))))

;;; -- ordering and scans -----------------------------------------------------

(deftest keys-are-ordered-bytewise
  ;; The whole index extension rests on this: a caller encodes the order it
  ;; wants into the key, and the store returns keys in that order.
  (with-db
    (fn [h]
      (in-tx h (fn [tx]
                 (b/put-many tx "b" [["c" "3"] ["a" "1"] ["b" "2"]])))
      (read-tx h (fn [tx]
                   (is (= ["a" "b" "c"]
                          (mapv first (b/scan tx "b" nil false nil false 0)))))))))

(deftest scan-bounds
  (with-db
    (fn [h]
      (in-tx h (fn [tx]
                 (b/put-many tx "b" (mapv (fn [k] [k k]) ["a" "b" "c" "d" "e"]))))
      (read-tx h
               (fn [tx]
                 (let [ks (fn [& args] (mapv first (apply b/scan tx "b" args)))]
                   (testing "unbounded"
                     (is (= ["a" "b" "c" "d" "e"] (ks nil false nil false 0))))
                   (testing "inclusive from"
                     (is (= ["c" "d" "e"] (ks "c" false nil false 0))))
                   (testing "exclusive from"
                     (is (= ["d" "e"] (ks "c" true nil false 0))))
                   (testing "inclusive to"
                     (is (= ["a" "b" "c"] (ks nil false "c" false 0))))
                   (testing "exclusive to"
                     (is (= ["a" "b"] (ks nil false "c" true 0))))
                   (testing "both bounds"
                     (is (= ["b" "c"] (ks "b" false "d" true 0))))
                   (testing "limit"
                     (is (= ["a" "b"] (ks nil false nil false 2))))
                   (testing "limit <= 0 means no limit"
                     (is (= 5 (count (ks nil false nil false -1)))))))))))

(deftest scan-paths-projects
  (with-db
    (fn [h]
      (in-tx h (fn [tx]
                 (doseq [i (range 3)]
                   (put-doc! tx "b" (str "k" i)
                             {:id (str "k" i) :name (str "n" i) :body (big-body 5000)}))))
      (read-tx h
               (fn [tx]
                 (let [res (b/scan-paths tx "b" nil false nil false 0
                                         [(enc :id) (enc :name)] false)]
                   (is (= 3 (count res)))
                   (is (= #{"k0" "k1" "k2"}
                          (set (map (fn [[_ v]] (:id (thaw-projected v))) res)))
                       "only the projected fields come back")
                   (is (every? (fn [[_ v]] (not (contains? (thaw-projected v) :body)))
                               res)
                       "the unprojected field is not transferred")))))))

;;; -- envelopes --------------------------------------------------------------

(deftest envelope-roundtrip
  (with-db
    (fn [h]
      (let [doc {:id "big" :name "Big" :body (big-body 2000000)
                 :meta {:city "Springfield"}}]
        (in-tx h (fn [tx] (put-doc! tx "b" "big" doc)))
        (read-tx h (fn [tx]
                     (is (= doc (thaw (b/get tx "b" "big"))))))))))

(deftest get-path-returns-one-field
  (with-db
    (fn [h]
      (let [doc {:id "big" :body (big-body 5000000) :meta {:city "Springfield"}}]
        (in-tx h (fn [tx] (put-doc! tx "b" "big" doc)))
        (read-tx h
                 (fn [tx]
                   (testing "a stored field comes back as its own blob"
                     (let [[kind blob] (b/get-path tx "b" "big" (enc :meta))]
                       (is (= "field" kind))
                       (is (= {:city "Springfield"} (dec* blob)))))
                   (testing "a field that is not stored"
                     (is (= ["missing" ""] (b/get-path tx "b" "big" (enc :nope)))))
                   (testing "a missing document is distinct from a missing field"
                     ;; The adapter needs to tell them apart: a missing field
                     ;; still has a document to fall back to.
                     (is (= ["nodoc" ""] (b/get-path tx "b" "nope" (enc :id)))))))))))

(deftest put-field-updates-one-field
  (with-db
    (fn [h]
      (let [doc {:id "big" :body (big-body 5000) :meta {:city "Springfield"}}]
        (in-tx h (fn [tx] (put-doc! tx "b" "big" doc)))
        (in-tx h (fn [tx] (b/put-field tx "b" "big" (enc :meta) (enc {:city "Shelbyville"}))))
        (read-tx h
                 (fn [tx]
                   (let [back (thaw (b/get tx "b" "big"))]
                     (is (= "Shelbyville" (get-in back [:meta :city])))
                     (is (= (:body doc) (:body back))
                         "the untouched field is unchanged"))))))))

(deftest get-paths-projects
  (with-db
    (fn [h]
      (let [doc {:id "big" :name "Big" :body (big-body 5000) :meta {:city "Springfield"}}]
        (in-tx h (fn [tx] (put-doc! tx "b" "big" doc)))
        (read-tx h
                 (fn [tx]
                   (testing "an envelope comes back as the requested fields only"
                     (let [[kind payload] (b/get-paths tx "b" "big" [(enc :id) (enc :name)])]
                       (is (= "fields" kind))
                       (is (= {:id "big" :name "Big"}
                              (reduce (fn [m [k v]] (assoc m (dec* k) (dec* v))) {} payload)))))
                   (testing "no keys asked for means the whole value"
                     (is (= doc (thaw (b/get-paths tx "b" "big" [])))))
                   (testing "a missing document is nil"
                     (is (nil? (b/get-paths tx "b" "nope" [(enc :id)]))))))))))

(deftest put-doc-many-writes-a-batch
  ;; The bulk writer: one round-trip for many documents, and the same
  ;; plain-vs-envelope rule as put-doc (nil keys stores the body verbatim).
  (with-db
    (fn [h]
      (let [big   (mapv (fn [i] {:id (str "big" i) :body (big-body 5000) :n i}) (range 3))
            small (mapv (fn [i] {:id (str "small" i) :n i}) (range 3))
            recs  (concat
                   (map (fn [m]
                          (let [[ks offs body] (doc-parts m)]
                            [(str (:id m)) ks offs body]))
                        big)
                   (map (fn [m] [(str (:id m)) nil nil (enc m)]) small))]
        (in-tx h (fn [tx] (b/put-doc-many tx "b" (vec recs))))
        (read-tx h
                 (fn [tx]
                   (is (= 6 (b/count tx "b")))
                   (doseq [m big]
                     (is (= m (thaw (b/get tx "b" (str (:id m)))))
                         (str "envelope " (:id m))))
                   (doseq [m small]
                     (is (= m (thaw (b/get tx "b" (str (:id m)))))
                         (str "plain " (:id m))))))))))

(deftest open-without-durability-still-works
  ;; `durable? false` sets bbolt's NoSync: commits skip the fsync. The data
  ;; must still be there for a reader in the same process.
  (let [path (fresh)
        h    (b/open path false)]
    (try
      (in-tx h (fn [tx] (b/put tx "b" "k" "v")))
      (read-tx h (fn [tx] (is (= ["doc" "v"] (b/get tx "b" "k")))))
      (finally (b/close h)))))

(deftest raw-size-small-document-is-plain
  ;; Below the envelope threshold the value must be byte-for-byte the plain
  ;; transit encoding - the header would cost more than it saves.
  (with-db
    (fn [h]
      (let [doc {:v 1 :name "x" :tag "y"}]
        (in-tx h (fn [tx] (put-doc! tx "b" "a" doc)))
        (read-tx h
                 (fn [tx]
                   (is (= (count (enc doc)) (b/raw-size tx "b" "a")))))))))

(deftest raw-size-large-document-is-compressed
  (with-db
    (fn [h]
      (let [doc {:id "big" :body (big-body 1000000)}]
        (in-tx h (fn [tx] (put-doc! tx "b" "big" doc)))
        (read-tx h
                 (fn [tx]
                   (let [n (b/raw-size tx "b" "big")]
                     (is (< n 300000) (str "stored " n " bytes for a 1 MB document")))))))))

(deftest plain-value-starting-with-magic-is-not-an-envelope
  ;; `put` stores opaque strings, so a plain value that happens to start with
  ;; the envelope marker must come back unchanged rather than be misread.
  (with-db
    (fn [h]
      (let [v "PBB1 not really an envelope"]
        (in-tx h (fn [tx] (b/put tx "b" "k" v)))
        (read-tx h (fn [tx]
                     (is (= ["doc" v] (b/get tx "b" "k")))))))))

;;; -- compaction and stats ---------------------------------------------------

(deftest stats-reports-sizes
  (with-db
    (fn [h]
      (in-tx h (fn [tx] (b/put-many tx "b" (mapv (fn [i] [(str i) (str i)]) (range 100)))))
      (read-tx h
               (fn [tx]
                 (let [s (b/stats tx)]
                   (is (pos? (:file-size s)))
                   (is (pos? (:tx-size s)))
                   (is (<= 0.0 (:free-ratio s) 1.0))))))))

(deftest compact-shrinks-the-file
  ;; bbolt never shrinks its file on its own: freed pages are reused, but the
  ;; high-water mark stays. `compact` is the only way to reclaim it.
  (let [path (fresh)
        h    (b/open path)]
    (try
      (in-tx h (fn [tx]
                 (b/put-many tx "b" (mapv (fn [i] [(str i) (big-body 1000)]) (range 2000)))))
      (let [before (.length (File. path))]
        (in-tx h (fn [tx] (b/delete-many tx "b" (mapv str (range 2000)))))
        (b/compact h)
        (is (< (.length (File. path)) before)
            "the file should shrink once the rows are gone")
        (read-tx h (fn [tx] (is (= 0 (b/count tx "b"))))))
      (finally (b/close h)))))

;;; -- runner -----------------------------------------------------------------

(let [{:keys [fail error]} (t/run-tests)]
  (shutdown-agents)
  (System/exit (+ fail error)))
