(ns movies.download
  (:use [seabass core])
  (:use [clojure.java.io :only (file)])
  (:use [incanter core stats charts io datasets])
  (:require [clojure string]))

(def prefix
  (str "prefix : <http://data.linkedmdb.org/resource/movie/> "
       "prefix foaf: <http://xmlns.com/foaf/0.1/> "
       "prefix dc: <http://purl.org/dc/terms/> "))

(def lmdb "http://data.linkedmdb.org/sparql")

(def class-query "select distinct ?cs { ?s a ?cs } ")

(defn get-classes []  ($ :cs (bounce class-query lmdb)))

(defn class-download [c]
  (let [class-name (last (clojure.string/split c #"/"))
        query (str "construct {?s ?p ?o} { ?s ?p ?o . ?s a <" c "> }")
        graph (pull query lmdb)]
    (Thread/sleep 200)
    (pr ".")
    (stash graph (str "data-lmdb/lmdb-" class-name ".nt"))))

(defn download-data [L] (doseq [i L] (class-download i)))
; (-> (get-classes) download-data)

(defn dbpedia [x] (str "http://dbpedia.org/data/" x ".ntriples"))

(def dbp-links (str prefix "
select ?dbp
{ ?f owl:sameAs ?dbp . filter regex(str(?dbp), '^http://dbpedia.org/') }
"))

(defn get-links [m] ($ :dbp (bounce dbp-links m)))

(defn link-download [l]
  (let [linkname (last (clojure.string/split l #"/"))
        link (clojure.string/replace linkname "\"" "")
        data (slurp (dbpedia link))
        ]
    (Thread/sleep 1000)
    (spit (str "data-dbpedia/dbp-" link ".nt") data)
    (pr "")
    ))

(defn get-files [] (filter #(.isFile %) (file-seq (file "data-lmdb"))))
(defn get-graph [] (reduce build (get-files)))
(defn download-link-data [L] (doseq [i L]
                               (try (link-download i)
                                    (catch Exception e (prn "!")))))
; (def m (get-graph))
; (-> (get-links m) download-link-data)