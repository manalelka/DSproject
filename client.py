import pandas as pd
import socket
import pickle
import numpy as np
from sklearn import datasets
from include import *


dataset = datasets.load_iris() #We'll work with the iris dataset --> we can change later if not suitable

host = '127.0.0.1'
serverPort = SERVER_PORT_WORKERS

def startConnectionToServer():
    while True:
            try:
                # Connecting to server port
                connectServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                connectServer.connect((host, serverPort))

                #Send how many nodes the client needs to the server
                msg = 'Send 3'  #TODO : agree on one message ( always ask for 3 nodes ? )
                connectServer.send(msg.encode('ascii'))

                #We wait for tthe server to tell us how many workers it gives
                #us and split the data accordingly
                nbNodes = connectServer.recv(1024)
                datasets=splitDataset(nbNodes) #list of split datasets

                #TODO while until finished
                #Get list of working nodes (ip,port)
                data = connectServer.recv(1024)
                ips , ports = pickle.loads(data) #list of @ ips and ports of working nodes sent by the server
                                                #TODO : agree on one format to send the ips and ports and test it --> we should use pickle on server side too
                nbNodes=len(ips) #number of working nodes


                for i in range(nbNodes):
                    datasetToSend = datasets[i]
                    workerIp= ips[i]
                    workerPort = ports[i]
                    listenWorker(datasetToSend,workerIp,workerPort)

                #TODO : (more like a problem !!) if the server sends a new available worker node we would have already split the dataset and sent it
                # to the available nodes...

                #We don't close the connection to the server in case the server sends a new working node
            except:
                print("An error occurred!")
                connectServer.close()
                break

def listenWorker(df,workerIp,workerPort):
        while True:
            try:
                #Connect to the worker node
                workerSocket =socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                workerSocket.connect((workerIp,workerPort))

                #Serialize the dataset with pickle
                df_pickled= pickle.dumps(df)

                #send the serialized dataset with pickle
                workerSocket.send(df_pickled)

                #close the connection with the worker
                workerSocket.close()

            except:
                print("An error occurred!")
                workerSocket.close()
                break

def splitDataset(nbNodes):
    # split the dataset depending on how many working nodes we have
    #output : list of the datasets
    splitDf= np.array_split(dataset, nbNodes)
    return splitDf

def main():
    print("Starting the client connection ...")
    startConnectionToServer()


if __name__ == '__main__':
    main()
