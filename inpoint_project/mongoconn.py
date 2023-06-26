import pymongo


def Jaccard_Similarity(doc1, doc2):
    # List the unique words in a document
    words_doc1 = set(doc1.lower().split())
    words_doc2 = set(doc2.lower().split())

    # Find the intersection of words list of doc1 & doc2
    intersection = words_doc1.intersection(words_doc2)

    # Find the union of words list of doc1 & doc2
    union = words_doc1.union(words_doc2)

    # Calculate Jaccard similarity score
    # using length of intersection set divided by length of union set
    return float(len(intersection)) / len(union)


myclient = pymongo.MongoClient("mongodb://localhost:27017/")

mydb = myclient["inpoint"]

#dblist = myclient.list_database_names()
#if "inpoint" in dblist:
#    print("The database exists.")

mycollection = mydb["https://en.wikipedia.org/wiki/Category:Coffee_brands"];

#collist = mydb.list_collection_names()
#if "coffee" in collist:
#    print("The collection exists.")

#mydict = { "name": "John", "address": "Highway 37" }
#x = mycollection.insert_one(mydict)
#print(x)

mybasetext = ' I am building a new coffee supply chain.';
cursor = mycollection.find({})
for document in cursor:
    print(Jaccard_Similarity(mybasetext, document['content']), document['url']);
