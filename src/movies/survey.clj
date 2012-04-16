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

(defn get-files [] (filter #(.isFile %) (file-seq (file "raw"))))

(defn get-graph [] (reduce build (get-files)))

