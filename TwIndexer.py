import lucene, json
from org.apache.lucene.document import Document, Field, FieldType, StringField, TextField, IntPoint
from org.apache.lucene.index import IndexWriterConfig, IndexWriter
from org.apache.lucene.index import IndexReader
#from org.apache.lucene.analysis.snowball import SnowballAnalyzer
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.analysis.core import WhitespaceAnalyzer, KeywordAnalyzer, SimpleAnalyzer
from org.apache.lucene.analysis.en import EnglishAnalyzer
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
import sys

import pdb
import glob

#awk '{ sub("\r$", ""); print }' tweets.json > tweets_tweaked.json
#specify the directory in which the tweets_tweaked.json exists

def retrieving(searchword):
    indexPath = File("indexOut/").toPath()
    indexDir = FSDirectory.open(indexPath)
    reader = DirectoryReader.open(indexDir)
    idxDocs = reader.maxDoc()
    print("We have ", idxDocs, " indexed documents")
    searcher = IndexSearcher(reader)
    idx_analyzer = StandardAnalyzer()
    #Search for the input term in field stored as text
    # To look into multiple fields, try  MultiFieldQueryParser, but it is not recommended.
    # Its best to club everything we want to search into a single search field and try WildCard matching on it
    query = QueryParser("text", idx_analyzer).parse(searchword)
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





def indexing(datadir):
    indexedDocs = 0
    doc = Document()
    #index_outdir = str(input("Enter index output dir: "))
    path = Paths.get('indexOut')
    indexOut = SimpleFSDirectory(path)
    analyzer = StandardAnalyzer()
    config = IndexWriterConfig(analyzer)
    writer = IndexWriter(indexOut, config)
    for filename in glob.iglob(datadir + '/*.json*', recursive=True):
        try:
            print("Filename is", filename)
            #pdb.set_trace()
            with open(filename) as f:
                for line in f:
                    tweet=json.loads(line)
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
                        writer.addDocument(doc)
                        writer.commit()
                        indexedDocs+=1
        except:
            continue


    writer.close()
    print("Indexed ", indexedDocs, " documents")



def index_scan():
    print("Scanning the index")
    #pdb.set_trace()
    indexPath = File("indexOut/").toPath()
    indexDir = FSDirectory.open(indexPath)
    reader = DirectoryReader.open(indexDir)
    fields = MultiFields.getFields(reader)
    for field in fields:
        term = MultiFields.getTerms(reader,field)
        print(field, "->" , term)



if __name__=='__main__':
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    if(len(sys.argv) < 3) :
        print ("Incorrect Usage !!")
        print("Usage : python TwIndexer.py <dir with json data files> <search word>")
    else:
        indexing(sys.argv[1])
        print("Indexing Complete")
        print("................................................")
        print("Lets try searching the index for -> "+sys.argv[2])
        retrieving(sys.argv[2])
        print("................................................")
        index_scan()
        print("................................................")

