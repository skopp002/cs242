import lucene, json
from org.apache.lucene.document import Document, Field, FieldType, StringField, TextField, IntPoint
from org.apache.lucene.index import IndexWriterConfig, IndexWriter
from org.apache.lucene.index import IndexReader
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.store import SimpleFSDirectory, FSDirectory
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.util import Version
from org.apache.lucene.store import IOContext
from org.apache.lucene.index import DirectoryReader,  MultiFields
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
    #path = Paths.get('indexOut')
    #pdb.set_trace()
    indexPath = File("indexOut/").toPath()
    indexDir = FSDirectory.open(indexPath)
    reader = DirectoryReader.open(indexDir)
    searcher = IndexSearcher(reader)
    analyzer = StandardAnalyzer()
    #Search for the input term in field stored as text
    # To look into multiple fields, try  MultiFieldQueryParser, but it is not recommended. Its best to club everything we want to search into a single search field and try WildCard matching on it
    query = QueryParser("text", analyzer).parse("School")#(input("enter search:"))
    MAX = 1000
    hits = searcher.search(query, MAX)
    print ("Found %d document(s) that matched query '%s':" % (hits.totalHits, query))
    try:
        for hit in hits.scoreDocs:
            print (hit.score, hit.doc, hit.toString())
            doc = searcher.doc(hit.doc)
            print (doc.get("text").encode("utf-8"))
    except:
        print("Could not find the word")

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
    for filename in glob.iglob(json_direc + '/*.json', recursive=True):
        try:
            print("Filename is", filename)
            #pdb.set_trace()
            with open(filename) as f:
                for line in f:
                    tweet=json.loads(line)
                    print("tweet ",tweet)
                    if(tweet['lang']=='en'):
                        doc.add(StringField("id", tweet['id_str'], Field.Store.YES))

                    # doc.add(Field("screen_name", tweet['user.screen_name']))
                    # print(tweet['user.screen_name'])
                    # doc.add(Field("name", tweet['user.name']))
                    #doc.add(Field("location", tweet['user.location']))
                    #print(tweet['user.location'])
                        doc.add(StringField("text",tweet['text'],Field.Store.YES))
                    #doc.add(Field("created_at", DateTools.stringToDate(tweet['created_at']),Field.Store.YES))
                        doc.add(StringField("created_at", tweet['created_at'], Field.Store.YES))
                    # doc.add(IntPoint("followers", tweet['user.followers_count'],Field.Store.YES))
                    # doc.add(IntPoint("friends", tweet['friends_count'],Field.Store.YES))
                        print("Indexed ... "+ doc.toString())
                        writer.addDocument(doc)
                        writer.commit()
        except:
            continue


    writer.close()



def index_scan():
    print("Scanning the index")
    #pdb.set_trace()
    indexPath = File("indexOut/").toPath()
    indexDir = FSDirectory.open(indexPath)
    reader = DirectoryReader.open(indexDir)
    fields = MultiFields.getFields(reader)
    for field in fields:
        term = MultiFields.getTerms(reader,field)
        print(term)


if __name__=='__main__':
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    choice=int(input("Choose 1 to index, 2 to query and 3 to scan the index: "))
    if(choice == 1):
        # pdb.set_trace()
        indexing()
    elif (choice == 2):
        retrieving()
    elif(choice == 3):
        index_scan()









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