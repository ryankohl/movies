(ns movies.examine
  (:use [seabass core])
  (:use [clojure.java.io :only (file)])
  (:use [incanter core stats charts io datasets]))


(def prefix
  (str "prefix : <http://tutori.al/> "
       "prefix lmdb: <http://data.linkedmdb.org/resource/movie/> "
       "prefix dbc: <http://dbpedia.org/class/> "
       "prefix dbp: <http://dbpedia.org/property/> "
       "prefix dbr: <http://dbpedia.org/resource/> "
       "prefix foaf: <http://xmlns.com/foaf/0.1/> "
       "prefix dc: <http://purl.org/dc/terms/> "))

(def lmdb "http://localhost:3030/ds/query")
(def dbp "http://localhost:3031/ds/query")

;; who are the top 100 most active producers (by # films)?
;; what is each producer's
;;   name, # films, gravity, street cred, experience,
;;   avg film rating,
;;   associated production companies (w/ # films)

;; for each producer,
;;   which films did they produce?
(def lmdb-0 (str prefix
                 "construct {?x :produced ?f . ?f :title ?t . ?f :date ?d} "
                 "{?f lmdb:producer ?x . ?f dc:title ?t . ?f dc:date ?d}"))
;;   which production companies have they worked with?
(def lmdb-1 (str prefix
                 "construct {?x :worked-with ?y . ?y a :Production-Company} "
                 "{?f lmdb:producer ?x . ?f lmdb:production_company ?y}"))
;;   which writers have they worked with?
(def lmdb-2 (str prefix
                 "construct {?x :worked-with ?y . ?y a :Writer} "
                 "{?f lmdb:producer ?x . ?f lmdb:writer ?y}"))
;;   which directors have they worked with?
(def lmdb-3 (str prefix
                 "construct {?x :worked-with ?y . ?y a :Director} "
                 "{?f lmdb:producer ?x . ?f lmdb:director ?y}"))
;;   how often have they directed?
(def lmdb-4 (str prefix
                 "construct {?x :directed ?f} "
                 "{?x a lmdb:producer . ?f lmdb:director ?x}"))
;;   how often have they been executive producers?
(def lmdb-5 (str prefix
                 "construct {?x :executive-produced ?f} "
                 "{?x a lmdb:producer . ?f lmdb:executive_producer ?x}"))
;;   what ratings have their films gotten?
(def lmdb-6 (str prefix
                 "construct {?f :rating ?r} "
                 "{?f lmdb:producer ?x . ?f lmdb:rating ?r}"))
;;   when were their films released?
(def lmdb-7 (str prefix
                 "construct {?f :released ?d} "
                 "{?f lmdb:producer ?x . ?f lmdb:initial_release_date ?d}"))
;;   get the interlink facts
(def lmdb-8 (str prefix
                 "construct {?x owl:sameAs ?y} "
                 "{ ?x a lmdb:film . ?x owl:sameAs ?y}"))
;;  get the names for people
(def lmdb-9 (str prefix
                 "construct {?x :name ?name} "
                 "{ "
                 "  { ?x rdfs:label ?name . ?x a lmdb:producer} union "
                 "  { ?x rdfs:label ?name . ?x a lmdb:director} union "
                 "  { ?x rdfs:label ?name . ?x a lmdb:writer} union "
                 "  { ?x rdfs:label ?name . ?x a lmdb:actor}  "
                 "}"))

;;   what were these films' budgets? (dbp)
(def dbp-0 (str prefix
                "construct {?f :dbp-budget ?b} "
                "{?f dbp:budget ?b}"))
;;   what were these films' grosses? (dbp)
(def dbp-1 (str prefix
                "construct {?f :dbp-gross ?g} "
                "{?f dbp:gross ?g}"))
;;   who were these films' stars? (dbp)
(def dbp-2 (str prefix
                "construct {?f :dbp-starring ?a . ?a :name ?name} "
                "{?f dbp:starring ?a . ?a rdfs:label ?name}"))

;;   the integration queries clean up the ties betw the sources
(def int-0 (str prefix
                "construct {?f :budget ?v} "
                "{ select ?f ?v "
                "  { ?f owl:sameAs/:dbp-budget ?b "
                "    bind (xsd:float(?b) as ?v) "
                "  } "
                "}"))
(def int-1 (str prefix
                "construct {?f :gross ?v} "
                "{ select ?f ?v "
                "  { ?f owl:sameAs/:dbp-gross ?g "
                "    bind (xsd:float(?g) as ?v) "
                "  } "
                "}"))
(def int-2 (str prefix
                "construct {?f :starring ?a} "
                "{?f owl:sameAs/:dbp-starring ?a}"))

;;   the analysis queries add addtional content over the integration graph
(def ana-A-0 (str prefix
                "construct {?f :expValue ?xp} "
                "{ select ?f (coalesce(?ratio, 1) as ?xp) "
                "  { ?f :gross ?g . ?f :budget ?b "
                "    bind (?g/?b as ?ratio) "
                "  }"
                "}"))
(def ana-A-1 (str prefix
                "construct {?p :numWriters ?nw} "
                "{ select ?p (count(?w) as ?nw) "
                "  { select distinct ?p ?w "
                "    { ?p :worked-with ?w . ?w a :Writer } "
                "  } group by ?p "
                "}"))
(def ana-A-2 (str prefix
                "construct {?p :numDirectors ?nd} "
                "{ select ?p (count(?d) as ?nd) "
                "  { select distinct ?p ?d "
                "    { ?p :worked-with ?d . ?d a :Director } "
                "  } group by ?p "
                "}"))
;;  infer: each producer's gravity
;;  -> the number of different stars in his movies
(def ana-B-0 (str prefix
                "construct {?p :gravity ?g} "
                "{ select ?p (count(?a) as ?g) "
                "  { select distinct ?p ?a "
                "    { ?p :produced/:starring ?a } "
                "  } group by ?p "
                "}"))
;;  infer: each producer's experience
;;  -> the number of films produced, exec produced, or directed
;;  * if available, replace the film by the ratio of
;;  * the gross over the budget (so blockbusters count for more)
(def ana-B-1 (str prefix
                  "construct {?p :experience ?e} "
                  "{ select ?p (sum(?xp) as ?e) "
                  "  { ?p :produced|:directed|:executive-produced ?f . "
                  "    ?f :expValue ?xp "
                  "  } group by ?p ?f"
                  "}"))
;;  infer: each producer's street cred
;;  -> the number of different writers and directors worked w/
(def ana-B-2 (str prefix
                  "construct {?p :street-cred ?cred} "
                  "{ ?p :numWriters ?nw . ?p :numDirectors ?nd . "
                  "  bind (?nw + ?nd as ?cred) "
                  "}"))

(def lmdb-queries [lmdb-0 lmdb-1 lmdb-2 lmdb-3 lmdb-4
                   lmdb-5 lmdb-6 lmdb-7 lmdb-8 lmdb-9])
(def dbp-queries [dbp-0 dbp-1 dbp-2])
(def int-queries [int-0 int-1 int-2])
(def ana-A-queries [ana-A-0 ana-A-1 ana-A-2])
(def ana-B-queries [ana-B-0 ana-B-1 ana-B-2])

(defn extract [queries source] (reduce build (map #(pull % source) queries)))

(defn get-i-graph []
  (let [lmdb-extract (extract lmdb-queries lmdb)
        dbp-extract (extract dbp-queries dbp)]
    (build lmdb-extract
           (extract int-queries (build lmdb-extract dbp-extract)))))

(defn get-a-graph []
  (let [i-graph (get-i-graph)
        analytic-A (build i-graph (extract ana-A-queries i-graph))
        analytic-B (build i-graph (extract ana-B-queries analytic-A))]
    (build i-graph
           analytic-B
           "data/movies.ttl"
           "data/movies.rules")))

(defn grab-new-data []
  (stash (get-a-graph) "data/the-producers.nt"))

;; startup directions:
;;  fuseki-server --port 3030 --update --mem /ds &
;;  fuseki-server --port 3031 --update --mem /ds &
;;  each control panel - load RDF file

;;  (grab-new-data)
;;  fuseki-server --port 3032 --update --mem /ds &
;;  load 'the-producers.nt' into :3032