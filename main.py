
from invertedIndex import InvertedIndex
import datetime
from searchEngine import SearchEngine
from pathlib import Path
import json

#x = datetime.datetime.now()

'''
invertIndex = InvertedIndex("DEV")
invertIndex.readIndex(r"DEV")
'''
#y = datetime.datetime.now()


search = SearchEngine("DEV")
search.searchInterfaceCommandLine()

