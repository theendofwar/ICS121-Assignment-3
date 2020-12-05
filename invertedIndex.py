from collections import defaultdict
import json
from bs4 import BeautifulSoup
import nltk
import sys
from urllib.parse import urlparse
import re
from pathlib import Path

'''
ZODB is a library to store the index into 
'''
import ZODB
from ZODB import FileStorage, DB
from persistent import Persistent
import transaction
import datetime
from BTrees._IOBTree import IOBTree

'''import math'''
from math import log10
from math import  sqrt

from simhash import Simhash 

# a class to record various information about the 
class Posting(Persistent):
    def __init__(self,frequency = 1,position= list()):

        self.frequency:int = frequency # store a words' frequency in a document
        self.position:list = position   # store a words' position in a document
        self.totalWord:int = 0 # store the doc length 
        self.weight:int = 0 # a score that based on importance (score higher if the word is more important)
    

    def incrementFrequency(self)->None:
        ''' increment the frequency by 1 '''
        self.frequency+=1
        self._p_changed = True

    def updatePosition(self,pos:int)->None:
        ''' update the position list '''
        self.position.append(pos)
        self.position.sort()
        self._p_changed = True
    
    def incrementWeight(self,score:int)->None:
        ''' increment the score by the input '''
        self.weight +=score 

    def tf(self)->float:
        ''' calculate the term frequency of document '''
        tf = log10(float(self.frequency))
        return 1 + tf

    def __repr__(self):
        return f"{self.frequency,self.weight,self.totalWord}"

# a class to represent a posting dictionary
class PostingDict(Persistent):
    def __init__(self,totalWebPage:int,page:int,pos:int):
        self.pageTotal:int = 1 # record the frequency of a term appear in all document
        self.postDict:IOBTree = IOBTree() # a dictionary that the key is docID and value is a Posting object
        self.postDict[page] = Posting(position = [pos])
        self.totalWebPage:int = totalWebPage # record the totalWebpage that crawler crawl
    
    def updatePostDict(self,page:int,pos:int)->None:
        ''' update the postingDict '''
        if page in self.postDict:
            self.postDict[page].incrementFrequency()
            self.postDict[page].updatePosition(pos)
            
        else:
            self.postDict[page] = Posting(position = [pos])
            self.pageTotal+=1
        self._p_changed = True

    def merge(self,postD):
        ''' simple merge function '''
        self.postDict.update(postD.postDict)
        self._p_changed = True

    def getPostKey(self):
        return self.postDict.keys()

     
    def rankdictFilter(self,champList:list,queryPosting:int):
        ''' return a dictionary with the key is docId and value is the rank score, but only iterate through the champList '''
        rankdict = dict()
        threshold = 0
        for key in champList:
            if threshold == 200:
                break
            # compute the socre based on the model g(d) + tf-idf(normalized), g(d) is the weight of a word inside a document
            rankdict[key] = (self.postDict[key].weight + 2*(self.postDict[key].tf() * log10(float(self.totalWebPage/self.pageTotal)/sqrt(self.postDict[key].totalWord)))) * queryPosting
            threshold+=1
        return rankdict
    
    def rankdict(self):
        ''' return a dictionary with the key is docId and value is the rank score '''
        rankd = IOBTree()
        for key,value in self.postDict.items():
            rankd[key] = value.weight + 2*((value.tf() * log10(float(self.totalWebPage/self.pageTotal)))/sqrt(value.totalWord))
        return rankd

    def __repr__(self):
        return f"frequency: {self.pageTotal},  {list(self.postDict.items())}"
    
# a simple class to represent the only champList
class ChampList(Persistent):
    def __init__(self):
        self.chaList = [] # type: List[int]
    
    def addElement(self,ele):
        self.chaList.append(ele)
        self._p_changed = True

    def sortchamp(self,func,reverse:bool):
        self.chaList.sort(key=func,reverse=reverse)

    def cut(self):
        if len(self.chaList) > 3000:
            self.chaList = self.chaList[:3000]
        
    def __repr__(self):
        return f"{self.chaList}"

    def __iter__(self):
        return iter(self.chaList)

class InvertedIndex():
    def __init__(self,name="DEV"):
        self.name: str = name # name of the crawled document directory
        self.index = dict() # type Dict[str,PostingDict], temporary store some of the index and later dump into disk
        self.page:int  = 1
        self.map:IOBTree = IOBTree() #type: Dict[docId, url]
        self.invertmap = dict() # type: Dict[url, docId]

        # the next section is initialize three file that would store the map between url and docId, champlist and inverted Index
        '''ZODB Opening'''
        indexStorage = FileStorage.FileStorage(f'IndexInvertIndex{name}.fs')
        indexdb = DB(indexStorage)
        self.indexconnection = indexdb.open()
        self.indexRoot = self.indexconnection.root()
        mapStorage = FileStorage.FileStorage(f'map{name}.fs')
        mapdb = DB(mapStorage)
        self.mapconnection = mapdb.open()
        self.mapRoot = self.mapconnection.root()
        champStorage = FileStorage.FileStorage(f'champList{self.name}.fs')
        champdb = DB(champStorage)
        self.champListConnection = champdb.open()
        self.champListRoot = self.champListConnection.root()
 
        self.threshold:int = 0 # when reach a threshold, dump the inverted index to disk
        self.countTotalWebPages(name) # count how many webpage the crawler crawl

        self.url = set() # store the url crawled, to avoid duplication

    def updateIndex(self,totalPage,word:str,pos:int):
        ''' update the inverted Index  '''
        if word in self.index.keys():
            self.index[word].updatePostDict(self.page,pos)
        else:
            self.index[word] = PostingDict(self.count,self.page,pos)
            

    def dumpIntoDisk(self):
        ''' dump the tempory inverted index to the disk  '''
        print("size before:" + str(sys.getsizeof(self.index)))
        print("dumping file")
        x = datetime.datetime.now()
        for key,value in self.index.items():
            if key in self.indexRoot:
                self.indexRoot[key].merge(value)
            else:
                self.indexRoot[key] = value
        
        for key,value in self.map.items():
            self.mapRoot[key] = value
        transaction.commit()
        self.index.clear()
        y = datetime.datetime.now()
        print(y-x)

    def readJson(self,jsonFile):
        ''' read a single json file  '''
        with open(jsonFile) as f:
            jsonFile = json.load(f)
        jsonFile["url"] = jsonFile["url"].split('#')[0]
        if jsonFile["url"] in self.url:
            return
        self.url.add(jsonFile["url"])
        totalWord = len(jsonFile["content"])
        if InvertedIndex.is_valid(jsonFile["url"]) == False or totalWord>100000 or totalWord == 0:
            return
        print(jsonFile["url"])
        # parse the jsonfile content
        soup = BeautifulSoup(jsonFile["content"],features = "lxml")
        all_text = soup.get_text()
        words = nltk.tokenize.word_tokenize(all_text)
        position = 0

        tempdict = defaultdict(int)
        for word in words:
            word = word.lower()
            if word.isalnum():
                self.updateIndex(self.count,word,position)
                position +=1
                tempdict[word]+=1
        
        totalfreq = sum([i**2 for i in tempdict.values()])
        for key in tempdict:
            self.index[key].postDict[self.page].totalWord = totalfreq
        
        
        #find the text that is more important than other text
        weights = soup.find_all(['h1', 'h2', 'h3', 'b', "strong","p","a"], text=True)
        for tag in weights:
            score = 0
            
            if tag.name == "a":
                score = 1
                parsed = nltk.tokenize.word_tokenize(tag.string)
                if "href" in tag.attrs:
                    url = tag.attrs["href"]
                    if url in self.invertmap:
                        for word in parsed:
                            word = word.lower()
                            if word in self.index and self.invertmap[url] in self.index[word].postDict:
                                self.index[word].postDict[self.invertmap[url]].incrementWeight(score)
            else:
                if tag.name == "p":
                    score = 0.2
                else:
                    score = 1
                parsed = nltk.tokenize.word_tokenize(tag.string)
                for word in parsed:
                    word = word.lower()
                    if word in self.index and word.isalnum():
                        try:
                            self.index[word].postDict[self.page].incrementWeight(score)
                        except KeyError:
                            pass
        

        self.map[self.page] = jsonFile["url"]
        self.invertmap[jsonFile["url"]] = self.page
        self.page+=1
        self.threshold+=1

        # reach threshold, dump the tempory inverted index to disk
        if self.threshold>5000:
            x = datetime.datetime.now()
            self.dumpIntoDisk()
            y = datetime.datetime.now()
            print(f"Execute the function time: {y-x}")
            self.threshold = 0
        
    def readIndex(self,path):
        ''' read a directory  '''
        x = datetime.datetime.now()
        p = Path(path)
        for path in p.iterdir():
            if path.is_dir():
                for pt in path.iterdir():
                    print(pt)
                    self.readJson(pt)
            else:
                self.readJson(path)
        self.dumpIntoDisk()
        # create a champlist based on inverted index
        self.champList()
        y = datetime.datetime.now()
        print(y-x)
    
    def countTotalWebPages(self,path):
        ''' count how many webpages the target directory have  '''
        self.count = 0
        p = Path(path)
        for path in p.iterdir():
            if path.is_dir():
                for pt in path.iterdir():
                    self.count+=1
            else:
                self.count+=1

    def champList(self):
        ''' create a champlist '''
        threshold = 0
        x = datetime.datetime.now()
        for key,value in self.indexRoot.items():
            print(key)
            rankdict = value.rankdict()
            chaList = ChampList()
            for k in rankdict:
                chaList.addElement(k)
            chaList.sortchamp(func=lambda x:rankdict[x],reverse=True)
            chaList.cut()
            self.champListRoot[key] = chaList
            if threshold == 1000:
                threshold = 0
                transaction.commit()
            threshold +=1
            rankdict.clear()
        transaction.commit()
        y = datetime.datetime.now()
        print(y-x)

    def writeReport(self,path:str):
        infile = open(f"report{path}.txt","w")
        infile.write(f"Number of web pages stored in {path}: {str(self.page)}\n")
        infile.write(f"Number of unique words store in {path}: {str(len(self.index.keys()))}\n")
        infile.write(f"The size of inverted index: {str(sys.getsizeof(self.index))}\n")
        '''
        x = [(key,v.frequency) for key,val in self.index.items() for v in val.values()]
        x.sort(key=lambda x:x[1],reverse=True)
        infile.write(f"The most common word in index is: {x[0][0]} and its frequency is {x[0][1]}")
        '''
        infile.close()


    @staticmethod
    def is_valid(url):
        ''' avoid trash websites parsed into inverted index '''
        try:
            parsed = urlparse(url)
            if "reply" and "wics.ics.uci.edu" in parsed.geturl():
                return False
            elif "pdf" in parsed.geturl():
                return False
            elif "zip" in parsed.geturl():
                return False
            elif "ppsx" in parsed.geturl():
                return False
            elif "CollabCom" in parsed.geturl():
                return False
            elif "ps.Z" in parsed.geturl():
                return False
            elif "MjolsnessCunhaPMAV24Oct2012" in parsed.geturl():
                return False
            elif ".npy" in parsed.geturl() or ".opt" in parsed.geturl() or ".sql" in parsed.geturl() or "txt" in parsed.geturl() or ".htm" in parsed.geturl() or ".html" in parsed.geturl() or ".shtml" in parsed.geturl():
                return False
            elif "calender" and "date" not in parsed.geturl():
                    if "?replytocom" not in parsed.geturl():
                        return not re.match(
                            r".*\.(css|js|bmp|gif|jpe?g|ico"
                                + r"|png|tiff?|mid|mp2|mp3|mp4"
                                + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
                                + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|ppsx"
                                + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
                                + r"|epub|dll|cnf|tgz|sha1"
                                + r"|thmx|mso|arff|rtf|jar|csv"
                                + r"|rm|smil|wmv|swf|wma|zip|rar|gz|odc|ps.Z|.r|replytocom)$", parsed.path.lower())
                    else:
                        return False
            
        except TypeError:
            print ("TypeError for ", parsed)
            raise
        