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
import time
import openpyxl
import random

host = HOST
serverPort = SERVER_PORT_CLIENTS
workersJob = {}  # dictionary with the workers and the part of the dataset they are working on
jobsToGetDone = None
mutex = threading.Lock()

def signalHandler(sig, frame):
    print('Closing the client...')

def evaluate(nodes,size):
    # Connecting to server port
    global connectServer
    global result
    global mutex
    result = None


    # We ask the client how many workers he would want
    nbNodes = nodes

    try:
        connectServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connectServer.connect((host, serverPort))
        # Send how many nodes the client needs to the server
        msg = pickle.dumps(['Send', nbNodes])
        connectServer.send(msg)
    except Exception as e:
        print("Error with socket connection to server: " + str(e))
        connectServer.close()
        #sys.exit(0)

    print('Asked for ' + str(nbNodes) + ' nodes')

    # We wait for the server to tell us how many workers it gives us and split the data accordingly
    try:
        nbNodesPickle = connectServer.recv(1024)
    except Exception as e:
        print("Error receiving from server: " + str(e))
        connectServer.close()
        #sys.exit(0)

    nbNodes = pickle.loads(nbNodesPickle)
    print('Permission for ' + str(int(nbNodes)) + ' nodes')
    datasets = splitDataset(int(nbNodes),size)  # list of split datasets

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
            #print(e)
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
    #exit()


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


def splitDataset(nbNodes,size):
    # split the dataset depending on how many working nodes we have
    # output : list of the datasets
    dataset = [10]
    dataset = dataset*(10**size)
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
    #global clientSocket

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
            #clientSocket.close()
            #connectServer.shutdown(socket.SHUT_RDWR)
            connectServer.close()
            print("The result of the mean asked is: " + str(result))
            break


print("Starting the client connection ...")

#We capture SIGINT to end gracefully
signal.signal(signal.SIGINT, signalHandler)

clientPort = input("Input client port number:\n")
clientPort = int(clientPort)
# We open listening socket on the client
try:
    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientSocket.bind((host, clientPort))
except Exception as e:
    print("Error with listening socket: " + str(e))
    #sys.exit(1)

# How to test the effect of (1 - n) number of workers with this script
# Create an excel file called 'LoggingData.xlsx' in the same file
# Run the server.py
# Run the worker.py n amount of times
# Change the numberOfWorkers variable below to n
# Change the size variable below to test for array of size (10**1 - 10**size) of the dataset


numberOfWorkers = 10
wb = openpyxl.load_workbook('LoggingData.xlsx')
columns = ['A','B','C','D','E','F','G','H','I','J']
len_dataset = 0

sheet = wb.get_sheet_by_name('Sheet1')
totalRuns = 1

#Logging into excel sheet called LoggingData.xlsx
for size in range (1,8):
    print(size)
    len_dataset = 10**size
    for i in range(numberOfWorkers):
        totalTimeTaken = 0
        for j in range (totalRuns):
            start = time.time()
            evaluate(i+1,size)
            end = time.time() - start
            totalTimeTaken += end
            time.sleep(1)
        sheet[columns[i]+str(size)] = totalTimeTaken/totalRuns


# size = 7
# # Running them separately
# len_dataset = 10**size
# for i in range(9,6,-1):
#     totalTimeTaken = 0
#     for j in range (totalRuns):
#         start = time.time()
#         evaluate(i+1,size)
#         end = time.time() - start
#         totalTimeTaken += end
#         time.sleep(1)
#     sheet[columns[i]+str(size)] = totalTimeTaken/totalRuns
wb.save('LoggingData.xlsx')

