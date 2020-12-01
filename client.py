import pandas as pd
import socket
import pickle
import numpy as np
from sklearn import datasets
from include import *


#dataset = datasets.load_iris() #We'll work with the iris dataset --> we can change later if not suitable
dataset = np.arange(9.0)

host = '127.0.0.1'
serverPort = SERVER_PORT_CLIENTS

def startConnectionToServer():
        try:
            #Connecting to server port
            connectServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connectServer.connect((host, serverPort))

            nbNodes = 2
            #Send how many nodes the client needs to the server
            msg = 'Send ' + str(nbNodes)  #TODO : agree on one message
            connectServer.send(msg.encode('ascii'))
            print('Asked for ' + str(nbNodes) + ' nodes')

            #We wait for the server to tell us how many workers it gives us and split the data accordingly
            nbNodes = connectServer.recv(1024)
            print('Permission for ' + str(int(nbNodes)) + ' nodes')
            datasets=splitDataset(int(nbNodes)) #list of split datasets
           
            #TODO while until finished check
            finished = False
            while not finished:

                #### First part : get list of IP @and ports of nodes and send the splitted data accordingly to the working nodes
                wrkNodescounter = 0 #working nodes counter
                while(wrkNodescounter <nbNodes and wrkNodescounter<=MAX_WORKERS): #while we don't have the number of nodes we asked for
                    data = []
                    while len(data) <= 0: #Why this while ?!
                        data = connectServer.recv(1024) #TODO why is it receiveng??        
                    flag, addrs = pickle.loads(data) #@ ips and ports of working nodes sent by the server

                    if flag == NEW_WORKERS: #We send data to these new workers
                        wrkNodescounter+=1
                        workerIp=addrs[0][0]
                        workerPort=addrs[0][1]
                        sendDataToWorker(datasets[wrkNodescounter],workerIp,workerPort)

                    elif flag == DEAD_WORKERS: #These workers are dead
                        #we decrease the number of the "good" nodes that way we know that the server is going to send another worker
                        if(wrkNodescounter!=0):
                            wrkNodescounter-=1 
                        workerIp=addrs[0][0]
                        workerPort=addrs[0][1]
                        #We check which data was sent to this dead node
                        try:
                            workerSocket =socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            workerSocket.connect((workerIp,workerPort))
                            msg= 'Which data was sent to this node ?' #TODO : agree on message
                            data = pickle.loads(workerSocket.recv(1024))
                            #TODO : what to do with this part of the data ?
                        except:
                            print("An error occurred!")
                            workerSocket.close()
                
                #### Second part : get the computation result and see if check that it's correct
                #TODO : get the result of the computation from the workers and mark finished as correct

            #We don't close the connection to the server in case the server sends a new working node
    
        except:
            print("An error occurred!")
            connectServer.close()

def sendDataToWorker(df,workerIp,workerPort):
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


            

def splitDataset(nbNodes):
    # split the dataset depending on how many working nodes we have
    #output : list of the datasets
    try:
        splitDf = np.array_split(dataset, nbNodes)
    except:
        print('Error splitting the data')
    return splitDf

def main():
    print("Starting the client connection ...")
    startConnectionToServer()


if __name__ == '__main__':
    main()
