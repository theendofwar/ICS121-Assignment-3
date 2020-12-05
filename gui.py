import PySimpleGUI as sg
from searchEngine import SearchEngine

sg.theme("LightGrey3")
layout = [
            [sg.Text("Enter your query"), sg.Input(key="IN"),sg.Button("search",bind_return_key=True,key="search")],
            [sg.Output(size=(100,30))]]

def main():
    searchEn = SearchEngine()
    searchEn.startSearchEngine()
    window = sg.Window("My Search Engine",layout)
    while True:
        event,values = window.read()
        if event is None:
            searchEn.closeConnection()
            break
        if event == "search":
            searchEn.searchInterface(values["IN"])
    window.close()

main()
