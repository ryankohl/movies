(ns movies.download
  (:use [seabass core])
  (:use [incanter core stats charts io datasets])
  (:require [clojure string]))

(def lmdb "http://data.linkedmdb.org/sparql")

(def class-query "select distinct ?cs { ?s a ?cs } ")

(defn get-classes []  ($ :cs (bounce class-query lmdb)))

(defn class-download [c]
  (let [class-name (last (clojure.string/split c #"/"))
        query (str "construct {?s ?p ?o} { ?s ?p ?o . ?s a <" c "> }")
        graph (pull query lmdb)]
    (Thread/sleep 200)
    (pr ".")
    (stash graph (str "raw/file-" class-name ".nt"))))

(defn download-data [L] (doseq [i L] (class-download i)))

; (-> (get-classes) download-data)

(def dbpedia "http://dbpedia.org/sparql")

(def dbp-links (str prefix "
select ?dbp
{ ?f owl:sameAs ?dbp . filter regex(str(?dbp), '^http://dbpedia.org/') . }
order by ?f
"))

(defn get-links [m] ($ :dbp (bounce dbp-links m)))

(defn download-links [l]
  (let [linkname (last (clojure.string/split l #"/"))
        query (str "construct {?s ?p ?o} {<"l"> ?p ?o")
        graph (pull query dbpedia)]
    (Thread/sleep 200)
    (pr ".")
    (stash graph (str "raw/link-" linkname ".nt"))))

; (defn get-files [] (filter #(.isFile %) (file-seq (file "raw"))))
; (defn get-graph [] (reduce build (get-files)))
; (def m (get-graph))
; (-> (get-links) download-links)