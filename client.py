import pandas as pd
import socket
import pickle
import numpy as np

def loadDataset():
    df = pd.read_csv("iris.csv")
    return df

def openConnection(s,host,port):
    #open connection to server
    # get number of working nodes and their IP @
    # connection to hostname on the port.
    s.connect((host, port))
    data = s.recv(1024)
    ips = pickle.loads(data) #list of @ ip of working nodes sent by the server
    nbNodes=len(ips) #number of working nodes
    return ips , nbNodes

def splitDataset(df , nbNodes):
    #split the dataset depending on how many working nodes we have
    # return a list of the datasets
    splitDf= np.array_split(df, nbNodes)
    return splitDf

def sendDatasetsNods(s,ips, splitDf):
    test=splitDf[0]
    s.send(test)
    return

def closeConnection(s):
    #close connection to server
    s.close()
    return

def main():
    print("Starting the client connection ...")
    # load the dataset
    df = loadDataset()
    # create a socket object
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # get local machine name
    host = socket.gethostname()
    port = 9999
    ips , nbNodes = openConnection(s,host,port)
    splitDf = splitDataset(df,nbNodes)
    sendDatasetsNods(s,ips,splitDf)
    closeConnection(s)


if __name__ == '__main__':
    main()
