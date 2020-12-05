import pandas as pd
import socket
import pickle
import numpy as np
import threading
from sklearn import datasets
from include import *
import sys


#dataset = datasets.load_iris() #We'll work with the iris dataset --> we can change later if not suitable
dataset = np.arange(9.0)
host = HOST
serverPort = SERVER_PORT_CLIENTS
workersJob = {} #dictionary with the workers and the part of the dataset they are working on
jobsToGetDone = None
mutex = threading.Lock()

def startConnectionToServer():
        #Connecting to server port
        global connectServer
        global result
        global mutex
        result = None

        #We ask the client how many workers he would want
        nbNodes = input("How many nodes do you want?:\n")
        nbNodes = int(nbNodes)

        try:
            connectServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connectServer.connect((host, serverPort))
            #Send how many nodes the client needs to the server
            msg = pickle.dumps(['Send', nbNodes])
            connectServer.send(msg)
        except Exception as e:
            print("Error with socket connection to server: " + str(e))
            connectServer.close()
            sys.exit(0)

        print('Asked for ' + str(nbNodes) + ' nodes')

        #We wait for the server to tell us how many workers it gives us and split the data accordingly
        try:
            nbNodesPickle = connectServer.recv(1024)
        except Exception as e:
            print("Error receiving from server: " + str(e))
            connectServer.close()
            sys.exit(0)

        nbNodes = pickle.loads(nbNodesPickle)
        print('Permission for ' + str(int(nbNodes)) + ' nodes')
        datasets = splitDataset(int(nbNodes)) #list of split datasets

        #We iniciate the thread listening for results.
        listenThread = threading.Thread(target = listenResult, args=(nbNodes,))
        listenThread.start()

        #List of numbers of jobs left to be done in inverse order to pop them in order
        jobsToGetDone = list(range(int(nbNodes)-1,-1,-1))
        #### First part : get list of IP @and ports of nodes and send the splitted data accordingly to the working nodes

        while(result == None): #while we don't have the number of nodes we asked for
            data = []

            try:
                data = connectServer.recv(1024)
            except:
                #This is in case the socket is closed
                pass

            if(result == None and len(data) != 0):
                flag, addrs = pickle.loads(data) #@ ips and ports of working nodes sent by the server

                if flag == NEW_WORKERS: #We send data to these new workers
                    for i in range(len(addrs)):
                        workerIp = addrs[i][0]
                        workerPort = addrs[i][1]

                        jobNumber = jobsToGetDone.pop()
                        sendDataToWorker(datasets[jobNumber],workerIp,workerPort)

                        #We add to workersJob what job is this worker going to do
                        mutex.acquire()
                        workersJob[(workerIp,workerPort)] = jobNumber
                        mutex.release()

                elif flag == DEAD_WORKERS: #These workers are dead
                    #we decrease the number of the "good" nodes that way we know that the server is going to send another worker
                    for i in range(len(addrs)):
                        workerIp = addrs[i][0]
                        workerPort = addrs[i][1]

                        #Recover what data needs to be computed again because its worker die
                        mutex.acquire()
                        jobNumber = workersJob[(workerIp,workerPort)]
                        mutex.release()
                        jobsToGetDone.append(jobNumber)

                #### Second part : get the computation result and see if check that it's correct
                #The program wont finish until he receives the result into the socket in the listenThread. If it receives it will close gently I thinkl


def sendDataToWorker(df,workerIp,workerPort):
    #Connect to the worker node
    try:
        workerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        workerSocket.connect((workerIp,workerPort))
    except Exception as e:
        print("Error with the connection socket to worker: " + str(e))
        sys.exit(0)

    #Serialize the dataset with pickle
    df_pickled = pickle.dumps([clientPort,df])

    #send the serialized dataset with pickle
    try:
        workerSocket.send(df_pickled)
    except:
        print("Error sending data to worker: " + str(e))

    #close the connection with the worker
    workerSocket.close()


def splitDataset(nbNodes):
    # split the dataset depending on how many working nodes we have
    #output : list of the datasets
    try:
        splitDf = np.array_split(dataset, nbNodes)
    except:
        print('Error splitting the data')
    return splitDf

def listenResult(nbNodes):
    global connectServer
    global result
    global clientPort
    global mutex

    #We open listening socket on the client
    try:
        clientSocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        clientSocket.bind((host,clientPort))
        clientSocket.listen()
    except Exception as e:
        print("Error with listening socket: " + str(e))
        sys.exit(0)

    #We wait for the result to come
    partialResult = 0
    numberResultsReceived = 0
    while True:
        try:
            workerSocket,address = clientSocket.accept() #TODO not sure try except
            data = workerSocket.recv(1024)
        except Exception as e:
            print("Error with worker socket: " + str(e))
            clientSocket.close()
            connectServer.close()
            break

        workerPort,data = pickle.loads(data)

        partialResult += data
        mutex.acquire()
        print("I received the result (" + str(data) + ") from data partition: " + str(workersJob[(address[0],workerPort)]))
        mutex.release()
        numberResultsReceived += 1

        if(numberResultsReceived == nbNodes):
            result = partialResult/len(dataset)
            #We close both sockets letting connectServer socket to get out of the recv blocking call when all data is processed
            clientSocket.close()
            connectServer.shutdown(socket.SHUT_RDWR)
            connectServer.close()
            print("The result of the mean asked is: " + str(result))
            break

print("Starting the client connection ...")

#We ask for the client port to be used
clientPort = input("Input client port number:\n")
clientPort = int(clientPort)

startConnectionToServer()
