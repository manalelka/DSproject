import pandas as pd
import socket
import pickle
import numpy as np
import threading
from sklearn import datasets
from include import *
import sys
import signal
import os
import json

# dataset = datasets.load_iris() #We'll work with the iris dataset --> we can change later if not suitable
dataset = np.arange(100.0)
len_dataset = len(dataset)
host = HOST
serverPort = SERVER_PORT_CLIENTS
workersJob = {}  # dictionary with the workers and the part of the dataset they are working on
jobsToGetDone = None
mutex = threading.Lock()

def signalHandler(sig, frame):
    print('Closing the client...')
    sys.exit(0)

def main():
    # Connecting to server port
    global connectServer
    global result
    global mutex
    result = None

    # We ask the client how many workers he would want
    nbNodes = input("How many nodes do you want?:\n")
    nbNodes = int(nbNodes)

    try:
        connectServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connectServer.connect((host, serverPort))
        # Send how many nodes the client needs to the server
        msg = pickle.dumps(['Send', nbNodes])
        connectServer.send(msg)
    except Exception as e:
        print("Error with socket connection to server: " + str(e))
        connectServer.close()
        sys.exit(0)

    print('Asked for ' + str(nbNodes) + ' nodes')

    # We wait for the server to tell us how many workers it gives us and split the data accordingly
    try:
        nbNodesPickle = connectServer.recv(1024)
    except Exception as e:
        print("Error receiving from server: " + str(e))
        connectServer.close()
        sys.exit(0)

    nbNodes = pickle.loads(nbNodesPickle)
    print('Permission for ' + str(int(nbNodes)) + ' nodes')
    datasets = splitDataset(int(nbNodes))  # list of split datasets

    # We initiate the thread listening for results.
    listenThread = threading.Thread(target=listenResult, args=(nbNodes,))
    listenThread.start()

    # List of numbers of jobs left to be done in inverse order to pop them in order
    jobsToGetDone = list(range(int(nbNodes)-1, -1, -1))

    while(result == None):  # while we dont have a result
        data = []

        try:
            data = connectServer.recv(1024)
        except Exception as e:
            # This is in case the socket is closed
            print(e)
            pass


        if(result == None and len(data) != 0):


            data = data.decode("utf-8")
            jsons = data.split("][")   

            if(len(jsons) > 1): 
                jsons[0] += "]"
                for item in range(1,len(jsons)-1):
                    item = "[" + item + "]"
                jsons[len(jsons)-1] = "[" + jsons[len(jsons)-1]


            for data in jsons:
                data = json.loads(data)
                # @ ips and ports of working nodes sent by the server
                flag, addrs = data

                if flag == NEW_WORKERS:  # We send data to these new workers
                    for i in range(len(addrs)):
                        workerIp = addrs[i][0]
                        workerPort = addrs[i][1]

                        jobNumber = jobsToGetDone.pop()
                        sendDataToWorker(datasets[jobNumber], workerIp, workerPort)

                        # We add to workersJob what job is this worker going to do
                        mutex.acquire()
                        workersJob[(workerIp, workerPort)] = jobNumber
                        mutex.release()

                elif flag == DEAD_WORKERS:  # These workers are dead
                    for i in range(len(addrs)):
                        workerIp = addrs[i][0]
                        workerPort = addrs[i][1]

                        # Recover what data needs to be computed again because its worker died
                        mutex.acquire()
                        jobNumber = workersJob[(workerIp, workerPort)]
                        mutex.release()
                        jobsToGetDone.append(jobNumber)

    #We have received the result so we close the program
    sys.exit(0)


def sendDataToWorker(df, workerIp, workerPort):
    # Connect to the worker node
    try:
        workerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        workerSocket.connect((workerIp, workerPort))
    except Exception as e:
        print("Warning: connection not established with worker " + str((workerPort)) + ' ' + str(e))
        workerSocket.close()
        return

    # Serialize the dataset with pickle
    df_pickled = pickle.dumps([clientPort, df])

    print("Sending to worker: " + str(df))
    # send the serialized dataset with pickle
    try:
        workerSocket.send(df_pickled)
    except:
        print("Error sending data to worker: " + str(e))

    # close the connection with the worker
    workerSocket.close()


def splitDataset(nbNodes):
    # split the dataset depending on how many working nodes we have
    # output : list of the datasets
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
    global clientSocket

    #We activate the socket for listening
    clientSocket.listen()

    # We wait for the result to come
    partialResult = 0
    numberResultsReceived = 0
    while True:

        data = None

        try:
            workerSocket, address = clientSocket.accept() 
        except Exception as e:
            print("Error at accept in listening workers socket: " + str(e))
            continue

        try:
            data = workerSocket.recv(1024)
        except Exception as e:
            print("Error at receiving partial results: " + str(e))
            workerSocket.close()
            continue

        workerPort, data = pickle.loads(data)

        num, mean = data
        partialResult += mean * (num / len_dataset)
        mutex.acquire()
        print("I received the result (" + str(data) + ") from data partition: " +
              str(workersJob[(address[0], workerPort)]))
        mutex.release()
        numberResultsReceived += 1

        if(numberResultsReceived == nbNodes):
            result = round(partialResult,meanDecimals)

            # We close both sockets letting connectServer socket to get out of the recv blocking call when all data is processed
            workerSocket.close()
            clientSocket.close()
            connectServer.shutdown(socket.SHUT_RDWR)
            connectServer.close()
            print("The result of the mean asked is: " + str(result))
            break


print("Starting the client connection ...")

#We capture SIGINT to end gracefully
signal.signal(signal.SIGINT, signalHandler)  

# We ask for the client port to be used
clientPort = input("Input client port number:\n")
clientPort = int(clientPort)

# We open listening socket on the client
try:
    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientSocket.bind((host, clientPort))
except Exception as e:
    print("Error with listening socket: " + str(e))
    sys.exit(1)

main()
