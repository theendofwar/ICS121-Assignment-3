from invertedIndex import InvertedIndex
import datetime
import nltk
from collections import  defaultdict
from math import log10
from math import sqrt

class SearchEngine():
    def __init__(self,name="DEV"):
        self.invertIndex = InvertedIndex(name)
        self.cache = dict()
        #target query should look exactly like an inverted index
        self.targetQuery = dict()
        self.webMap = dict()
        self.queryPosting = defaultdict(int)
        self.champList = dict()
    
    #rank the target queries and return a list of url for display
    #TODO: Adapt the ranking to fit in champList
    def ranking(self)->list:
        x = datetime.datetime.now()
        rankScore = dict()
        for key,value in self.targetQuery.items():
            #filter out the valid document for ranking
            rankScore[key] = value.rankdictFilter(self.invertIndex.champListRoot[key],self.queryPosting[key])
        y = datetime.datetime.now()
        print("filter time is", (y-x).total_seconds())
        rankURL = dict()

        # add up all the score from words in the document
        for value in rankScore.values():
            SearchEngine.updateDict(rankURL,value)

        #sorted based on the score
        webList = sorted(rankURL.keys(),key=lambda x:rankURL[x],reverse=True)
        if len(webList)>10:
            y = datetime.datetime.now()
            print("ranking time is", (y-x).total_seconds())
            self.queryPosting.clear()
            return webList[:10]
        else:
            y = datetime.datetime.now()
            print("ranking time is", (y-x).total_seconds())
            self.queryPosting.clear()
            return webList
            

    @staticmethod
    def updateDict(dict1,dict2)->None:
        for key,value in dict2.items():
            if key in dict1:
                dict1[key] += dict2[key]
            else:
                dict1[key] = value
    
    def startSearchEngine(self):
        '''  pre-load some important information to the search engine '''
        for key,value in self.invertIndex.mapRoot.items():
            self.webMap[key] = value
        for key,value in self.invertIndex.champListRoot.items():
            self.champList[key] = value
        for key,value in self.invertIndex.indexRoot.items():
            if value.pageTotal > 5000:
                self.cache[key] = value

    def analyzeQuery(self,inputstr:str):
        '''  analyze the query before ranking, ex. load the term from inverted index '''
        x = datetime.datetime.now()
        words = nltk.tokenize.word_tokenize(inputstr)
        for word in words:
            word = word.lower()
            self.queryPosting[word]+=1
        doclength = sum([i**2 for i in self.queryPosting.values()])
        for key in self.queryPosting.keys():
            self.queryPosting[key] = (1 + log10(self.queryPosting[key]))/doclength
            if key in self.cache:
                self.targetQuery[key] = self.cache[key]
                self.queryPosting[key] *=(self.targetQuery[key].totalWebPage/self.targetQuery[key].pageTotal)
            elif key in self.invertIndex.indexRoot:
                self.targetQuery[key] = self.invertIndex.indexRoot[key]
                self.queryPosting[key] *=log10(self.targetQuery[key].totalWebPage/self.targetQuery[key].pageTotal)
        y = datetime.datetime.now()
        print("analyze query time is", (y-x).total_seconds())

    def searchInterface(self,userinput:str):
        ''' this function is specificly for GUI '''
        x = datetime.datetime.now()
        self.analyzeQuery(userinput)
        useroutput = self.ranking()
        y = datetime.datetime.now()
        print("Search time is", (y-x).total_seconds())
        if useroutput == []:
            print("Doesn't find any Results")
        else:
            print("your 10 URLs are: ")
            for i in useroutput:
                print(self.webMap[i])

    
    def processPosition(self,list1,list2)->int:
        ''' compute the minimum difference from two sorted list '''
        len1 = len(list1)
        len2 = len(list2)
        i = 0
        j = 0
        min = -1
        while i < len1-1 and j <len2-1:
            min = abs(list1[i]-list2[i])
            if min == 1 or min == 2:
                return min
            if list1[i] < list2[j]:
                i+=1
            else:
                j+=1
        return min

    def searchInterfaceCommandLine(self):
        ''' commandline version of search engine '''
        userinput = input("StartSearchEngine Y/N: \n")
        if userinput.lower() == "y":
            self.startSearchEngine()
            while True:
                userinput = input("Your search query(type quit to quit): \n")
                if userinput.lower() == "quit":
                    break
                x = datetime.datetime.now()
                self.analyzeQuery(userinput)
                useroutput = self.ranking()
                y = datetime.datetime.now()
                print("Search time is", (y-x).total_seconds())
                print(useroutput)
                if useroutput == []:
                    print("Doesn't find any Results")
                else:
                    print("your 10 URLs are: ")
                    for i in useroutput:
                        print(self.webMap[i])
        self.closeConnection()

    def closeConnection(self):
        self.invertIndex.indexconnection.close()
        self.invertIndex.mapconnection.close()
        self.invertIndex.champListConnection.close()


'''
boolean search version

def search(self):
        newlist = []
        for word in words:
            if word in self.indexRoot:
                newlist.append(self.indexRoot[word])
            else:
                return []
        newlist.sort(key=lambda x:x.pageTotal)
        resultset = set(newlist[0].getPostKey())
        for i in newlist[1:]:
            x = set(i.getPostKey())
            resultset = resultset.intersection(x)
        resultList = []
        if len(resultset) <= 5:
            return resultset
        count = 0
        for i in resultset:
            if count == 5:
                return resultList
            else:
                resultList.append(self.mapRoot[i])
            count+=1
'''

