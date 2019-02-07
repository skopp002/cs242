import lucene, json
from org.apache.lucene.document import Document, Field, FieldType, StringField, TextField
from org.apache.lucene.index import IndexWriterConfig, IndexWriter
from org.apache.lucene.index import IndexReader
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.store import SimpleFSDirectory, FSDirectory
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.util import Version
from org.apache.lucene.store import IOContext
from org.apache.lucene.index import DirectoryReader
from java.io import File
from java.nio.file import Paths
from org.apache.lucene.store import RAMDirectory
import org.apache.lucene.store
from org.apache.lucene import analysis, document, index, queryparser, search, store, util


import pdb
import glob

#awk '{ sub("\r$", ""); print }' tweets.json > tweets_tweaked.json
#specify the directory in which the tweets_tweaked.json exists

def retrieving():
    # indexdir = input("Enter index output dir: ")
    path = Paths.get('indexOut')
    indexPath = File("indexOut/").toPath()
    indexDir = FSDirectory.open(indexPath)
    reader = DirectoryReader.open(indexDir)
    searcher = IndexSearcher(reader)
    analyzer = StandardAnalyzer()
    query = QueryParser("text", analyzer).parse(input("enter search:"))
    MAX = 1000
    hits = searcher.search(query, MAX)
    print
    "Found %d document(s) that matched query '%s':" % (hits.totalHits, query)
    for hit in hits.scoreDocs:
        print
        hit.score, hit.doc, hit.toString()
        doc = searcher.doc(hit.doc)
        print
        doc.get("text").encode("utf-8")


def indexing():
    tweets=[]
    doc = Document()
    json_direc = input("Enter the relative path of the JSON files: ")
    #index_outdir = str(input("Enter index output dir: "))
    path = Paths.get('indexOut')
    indexOut = SimpleFSDirectory(path)
    analyzer = StandardAnalyzer()
    config = IndexWriterConfig(analyzer)
    writer = IndexWriter(indexOut, config)
    for filename in glob.iglob(json_direc + '**/*.json', recursive=True):
        try:
            print("Filename is", filename)
            with open(filename) as f:
                for line in f:
                    tweet=json.loads(line)
                    if(tweet['lang']=='en'):
                        tweets.append(tweet)
                        doc.add(Field(tweet['id_str'],f))
                        doc.add(Field(tweet['user.screen_name'], f))
                        doc.add(Field(tweet['user.location'], f))
                        doc.add(Field(tweet['text'], f))
                        writer.addDocument(doc)
                        writer.commit()
        except:
            continue

    writer.close()

    #indexOut = SimpleFSDirectory(File("/index_output"))
  #  writer = IndexWriter(store.RAMDirectory(),analyzer)
    #directory = store.RAMDirectory()
    # To store an index on disk, use this instead:
    # Directory directory = FSDirectory.open(File("/tmp/testindex"))

   # try:

    #except Exception:
     #   print("Failed while writing to index")


if __name__=='__main__':
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    choice=int(input("Choose 1 to index and 2 to query: "))
    if(choice == 1):
        # pdb.set_trace()
        indexing()
    elif (choice == 2):
        retrieving()






#####################################################Workspace#####################################

# for tweet in tweets:
#     print(tweet)
#     ids = [tweet['id_str'] for tweet in tweets if 'id_str' in tweet]
#     text = [tweet['text'] for tweet in tweets if 'text' in tweet]
#     lang = [tweet['lang'] for tweet in tweets if 'lang' in tweet]
#     geo = [tweet['geo'] for tweet in tweets if 'geo' in tweet]
#     place = [tweet['place'] for tweet in tweets if 'place' in tweet]
    #print(ids, text, lang, geo, place)


# this was just what I was using to access the .json file from my desktop, a direct input of the directory may not
# really be the best idea. The open() and json.load functions output a dictionary value, test_dict a direct copy paste
# of the output of these functions from a small test .json file I downloaded to use for testing
#


# test_dict = {'error_message': 'You must use an API key to authenticate each request to Google Maps Platform APIs. For additional information, please refer to http://g.co/dev/maps-no-account', 'results': [], 'status': 'REQUEST_DENIED'}
# lucene.initVM(vmargs=['-Djava.awt.headless=true'])
# index = Document()
# index_fields = ["error_message", "status"]
# # index_fields can be set to pick out the parts of the tweets we care about, this area would have code to further parse
# # or narrow down results in a more specific way if we wanted/needed.
# for i in index_fields:
#     index.add(Field("error_message", test_dict[i], StringField.TYPE_STORED))
# index_config = IndexWriterConfig(StandardAnalyzer())
# index_direc = SimpleFSDirectory(Paths.get(sys.argv[1]))
# not sure what the formula for index_direc is doing, can this directory be set up with a prompt or
# just set to be the same value as json_direc? Once that is done the below code should work to commit changes to
# the index? once index_direc is set the rest of the code below should do the finishing steps for the index
# index_writer = IndexWriter(index_direc,index_config)
# index_writer.addDocument(index)
# index.writer.commit()
# index.writer.close()
