from pymongo import MongoClient
from pprint import pprint
import re

client = MongoClient(
    host="127.0.0.1",
    port = 27017,
    username = "admin",
    password = "pass"
)
#(b) Afficher la liste des bases de données disponibles.
print("*_*_*_*_*_*_*_*_*_*")
print(client.list_database_names())

#(c) Afficher la liste des collections disponibles dans cette base de données.
print("*_*_*_*_*_*_*_*_*_*")
print(client["sample"].list_collection_names())

#(d) Afficher un des documents de cette collection.
print("*_*_*_*_*_*_*_*_*_*")
sample = client["sample"]
c_books = sample["books"]

pprint(list(c_books.find_one()))

#(e) Afficher le nombre de documents dans cette collection*
print("*_*_*_*_*_*_*_*_*_*")
print(c_books.count_documents({}))

print("*_*_*_*_*_*_*_*_*_*""Exploration de la base""*_*_*_*_*_*_*_*_*_*")
#(a) Afficher le nombre de livres avec de plus de 400 pages, affichez ensuite le nombre de livres ayant plus de 400 pages ET qui sont publiés 
print("*_*_*_*_*_*_*_*_*_*")
print(
    c_books.count_documents({"pageCount": {"$gte": 400}})
)
print(
    c_books.count_documents({"pageCount": {"$gte": 400},'status': 'PUBLISH'})
)
#(b) Afficher le nombre de livres ayant le mot-clé Android dans leur description (brève ou longue). 
exp = re.compile("Android")
print(
    c_books.count_documents({"$or": [{"shortDescription": exp}, {"longDescription": exp}]})
)

#(c) Afficher les 2 listes des categories distinctes
print("*_*_*_*_*_*_*_*_*_*")
#pprint(list(c_books.find({},{'_id':0,"categories":1})))#.distinct('Catégorie1')))

result = c_books.find({}, {'_id': 0, 'categories': 1})
categories = list(result)

first_column = list(set(category['categories'][0] for category in categories if len(category['categories']) > 0))
second_column = list(set(category['categories'][1] for category in categories if len(category['categories']) > 1))

print("\nCatégorie1 :")
for value in first_column:
    print(value)

print("\nCatégorie12 :")
for value in second_column:
    print(value)


#(d) Afficher le nombre de livres qui contiennent des noms de langages suivant dans leur description longue : Python, Java, C++, Scala. On pourra s'appuyer sur des expressions régulières et une condition or.
print("*_*_*_*_*_*_*_*_*_*")

langages = ['Python', 'Java', 'C\+\+', 'Scala']
pattern = '|'.join(langages)
regex = re.compile(pattern)
print(c_books.count_documents({'longDescription': {'$regex': regex}}))


#(e) Afficher diverses informations statistiques sur notre bases de données : nombre maximal, minimal, et moyen de pages par catégorie. On utilisera une pipeline d'aggregation, le mot clef $group, ainsi que les accumulateurs appropriés.
print("*_*_*_*_*_*_*_*_*_*")

agg = [
    {
        '$group': {
            '_id': '$categories',
            'maxPages': {'$max': '$pageCount'},
            'minPages': {'$min': '$pageCount'},
            'avgPages': {'$avg': '$pageCount'}
        }
    }
]

result = list(c_books.aggregate(agg))

for i in result:
    category = i['_id']
    max_pages = i['maxPages']
    min_pages = i['minPages']
    avg_pages = i['avgPages']
    print(f"Categorie: {category}")
    print(f"Nombre maximal de pages: {max_pages}")
    print(f"Nombre minimal de pages: {min_pages}")
    print(f"Nombre moyen de pages: {avg_pages}")
    print("------")

#(f) Via une pipeline d'aggrégation, Créer de nouvelles variables en extrayant info depuis l'attribut dates : année, mois, jour. On rajoutera une condition pour filtrer seulement les livres publiés après 2009.
print("*_*_*_*_*_*_*_*_*_*")

aggr = [
    {
        '$match': {
            'publishedDate': {'$gte': '2010-01-01'}  # Filtrer les livres publiés après 2009
        }
    },
    {
        '$project': {
            'year': {'$year': {'$toDate': '$publishedDate'}},  # Extraire l'année de la date
            'month': {'$month': {'$toDate': '$publishedDate'}},  # Extraire le mois de la date
            'day': {'$dayOfMonth': {'$toDate': '$publishedDate'}}  # Extraire le jour de la date
        }
    }
]

pprint(list(c_books.aggregate(aggr)))


#(g) Créer une nouvelle colonne à partir de la liste des auteurs. Vous devez créer de nouveaux attributs (author1, author2 ... author_n). Observez le comportement de "$arrayElemAt". N'affichez que les 20 premiers dans l'ordre chronologique.
print("*_*_*_*_*_*_*_*_*_*")

pipeline = [
    {
        '$addFields': {
            'authorsCount': {'$size': '$authors'},  # Nombre total d'auteurs
            'authors': {
                '$slice': ['$authors', 20]  # Obtenir les 20 premiers auteurs
            }
        }
    },
    {
        '$project': {
            'authors': {
                '$map': {
                    'input': {'$range': [0, '$authorsCount']},  # Générer une liste de nombres
                    'in': {'$arrayElemAt': ['$authors', '$$this']}  # Récupérer l'auteur correspondant
                }
            }
        }
    },
    {
        '$limit': 20
    }
]


pprint(list(c_books.aggregate(pipeline)))


#(h) En s'inspirant de la requête précédente, créer une colonne contenant le nom du premier auteur, puis agréger selon cette colonne pour obtenir le nombre d'articles pour chaque premier auteur. Afficher le nombre de publications pour les 10 premiers auteurs les plus prolifiques. On pourra utiliser un pipeline d'agrégation avec les mots-clefs $group, $sort, $limit.
print("*_*_*_*_*_*_*_*_*_*")

pipeline = [
    {
        '$addFields': {
            'firstAuthor': {'$arrayElemAt': ['$authors', 0]}  # Récupérer le premier auteur
        }
    },
    {
        '$group': {
            '_id': '$firstAuthor',
            'publications': {'$sum': 1}  # Compter le nombre de publications pour chaque premier auteur
        }
    },
    {
        '$sort': {'publications': -1}  # Trier par ordre décroissant du nombre de publications
    },
    {
        '$limit': 10  # Limiter à 10 résultats
    }
]

pprint(list(c_books.aggregate(pipeline)))

