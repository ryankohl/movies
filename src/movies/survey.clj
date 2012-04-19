(ns movies.survey
  (:use [seabass core])
  (:use [clojure.java.io :only (file)])
  (:use [incanter core stats charts io datasets]))


(def prefix
  (str "prefix : <http://data.linkedmdb.org/resource/movie/> "
       "prefix foaf: <http://xmlns.com/foaf/0.1/> "
       "prefix dc: <http://purl.org/dc/terms/> "))

(def details "
select ?class ?pred (count(?subj) as ?cnt)
{ ?subj a ?class . ?subj ?pred ?obj }
group by ?class ?pred
order by ?class ?cnt
")

(def pred-details "
select ?pred (count(?subj) as ?cnt)
{ ?subj ?pred ?obj }
group by ?pred
order by ?pred
")

(defn subj-cnt [x] (str prefix "
select distinct (count(?s) as ?cnt) 
{ ?s a :" x "}") )

(def locations (str prefix "
select ?film ?place {
?f a :film . ?f rdfs:label ?film .
?f :featured_film_location ?p . ?p rdfs:label ?place
}"))

(defn get-files [x] (filter #(.isFile %) (file-seq (file x))))

(defn get-graph [x] (reduce build (get-files (str "data-" x))))

; (def m (get-graph "lmdb"))
; (def n (get-graph "dbpedia"))
; hey, why not push all 4000ish dbpedia nt files into a single file
