import socket
import pickle
import numpy as np
import threading
from include import *
import sys
import os
import json
import logging

dataset = np.arange(100.0)
len_dataset = len(dataset)
host = HOST
serverPort = SERVER_PORT_CLIENTS
workersJob = {}  # dictionary with the workers and the part of the dataset they are working on
jobsToGetDone = None
mutex = threading.Lock() # mutex to protect workersJob

def main():
    """
        Name: main
        Description: main function of the client.
        Arguments: None.
        Return: None.
    """
    global connectServer
    global result
    global mutex
    result = None

    # We ask the client how many workers he would want
    nbNodes = input("How many nodes do you want?:\n")
    nbNodes = int(nbNodes)

    # Connecting to server port
    try:
        connectServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connectServer.connect((host, serverPort))
        # Send how many nodes the client needs to the server
        msg = pickle.dumps(['Send', nbNodes])
        connectServer.send(msg)
    except Exception as e:
        msgInfo = "Error with socket connection to server: " + str(e)
        if(printMode):
            print(msgInfo)
        logging.error(msgInfo)
        connectServer.close()
        sys.exit(0)
    msgInfo = 'Asked for ' + str(nbNodes) + ' nodes'
    print(msgInfo)
    logging.info(msgInfo)

    # We wait for the server to tell us how many workers it gives us and split
    # the data accordingly
    try:
        nbNodesPickle = connectServer.recv(1024)
    except Exception as e:
        msgInfo = "Error receiving from server: " + str(e)
        if(printMode):
            print(msgInfo)
        logging.error(msgInfo)
        connectServer.close()
        sys.exit(0)

    nbNodes = pickle.loads(nbNodesPickle)
    msgInfo = 'Permission for ' + str(int(nbNodes)) + ' nodes'
    print(msgInfo)
    logging.info(msgInfo)

    datasets = splitDataset(int(nbNodes))  # list of split datasets

    try:
        # We initiate the thread listening for results.
        listenThread = threading.Thread(target=listenResult, args=(nbNodes,))
        listenThread.start()
    except(KeyboardInterrupt, SystemExit):
        #In case the client hungs, to kill it with SIGINT
        sys.exit(1)

    # List of numbers of jobs left to be done in inverse order to pop them in order
    jobsToGetDone = list(range(int(nbNodes)-1, -1, -1))


    while(result == None):  # while we dont have a result
        data = []

        #Timeout of 1 second to the server connection socket
        connectServer.settimeout(1)

        try:
            data = connectServer.recv(1024)
        except Exception as e:
            # This is in case the socket is closed from the server side
            pass

        if(result == None and len(data) != 0):

            # We parse the json data in case more than one json is sent together
            data = data.decode("utf-8")
            jsons = data.split("][")

            if(len(jsons) > 1):
                jsons[0] += "]"
                for item in range(1, len(jsons)-1):
                    item = "[" + item + "]"
                jsons[len(jsons)-1] = "[" + jsons[len(jsons)-1]

            # We get the data from each json
            for data in jsons:
                data = json.loads(data)
                # @ ips and ports of working/dead nodes sent by the server
                flag, addrs = data

                if flag == NEW_WORKERS:  # We send data to these new workers
                    for i in range(len(addrs)):
                        workerIp = addrs[i][0]
                        workerPort = addrs[i][1]

                        jobNumber = jobsToGetDone.pop()
                        sendDataToWorker(
                            datasets[jobNumber], workerIp, workerPort)

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

    # We have received the result so we close the program
    sys.exit(0)


def sendDataToWorker(df, workerIp, workerPort):
    """
        Name: sendDataToWorker
        Description: Function to send the data to the worker we want.
        Arguments:
            -df: data we want to send.
            -workerIp: string, worker IP we want to connect to.
            -workerPort: int, worker port we want to connect to.
        Return: None
    """
    # Connect to the worker node
    try:
        workerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        workerSocket.connect((workerIp, workerPort))
    except Exception as e:
        msgInfo = "Warning: connection not established with worker " + str((workerPort)) + ' ' + str(e)
        if(printMode):
            print(msgInfo)
        logging.warning(msgInfo)
        workerSocket.close()
        return

    # Serialize the dataset with pickle
    df_pickled = pickle.dumps([clientPort, df])

    msgInfo = "Sending to worker: " + str(df)
    print(msgInfo)
    logging.info(msgInfo)

    # send the serialized dataset with pickle
    try:
        workerSocket.send(df_pickled)
    except:
        msgInfo = "Error sending data to worker: " + str(e)
        if(printMode):
            print(msgInfo)
        logging.error(msgInfo)

    # close the connection with the worker
    workerSocket.close()


def splitDataset(nbNodes):
    """
        Name: splitDataset
        Description: Function that divides a dataset depending on the number of
            we are going to get from the server.
        Arguments:
            -nbNodes: number of nodes we will get from the server (number of
                parts in which the dataset will be divided).
            -workerIp: string, worker IP we want to connect to.
            -workerPort: int, worker port we want to connect to.
        Return: list of the datasets.
    """
    # split the dataset depending on how many working nodes we have
    try:
        splitDf = np.array_split(dataset, nbNodes)
    except:
        msgInfo = 'Error splitting the data'
        if(printMode):
            print(msgInfo)
        logging.error(msgInfo)
    return splitDf


def listenResult(nbNodes):
    """
        Name: listenResult
        Description: Function that starts a listening socket to receive the
            result from the workers.
        Arguments:
            -nbNodes: number of nodes we will get from the server (number of
                workers we will receive a result from).
        Return: None.
    """
    global connectServer
    global clientSocket
    global clientPort
    global result
    global mutex

    # We activate the socket for listening
    clientSocket.listen()

    # We wait for the result to come
    partialResult = 0
    numberResultsReceived = 0
    while True:
        data = None

        # We accept the connection from the client and receive its data
        try:
            workerSocket, address = clientSocket.accept()
        except Exception as e:
            msgInfo = "Error at accept in listening workers socket: " + str(e)
            if(printMode):
                print(msgInfo)
            logging.error(msgInfo)
            continue

        try:
            data = workerSocket.recv(1024)
        except Exception as e:
            msgInfo = "Error at receiving partial results: " + str(e)
            if(printMode):
                print(msgInfo)
            logging.error(msgInfo)
            workerSocket.close()
            continue

        # We unpickle the data received
        workerPort, data = pickle.loads(data)

        # Data received must be composed of the length of the data the worker
        # received and the result of the computation
        if len(data) == 2:
            num, mean = data
            # We calculated the weighted mean with the other received results
            partialResult += mean * (num / len_dataset)
            mutex.acquire()
            msgInfo = "I received the result (" + str(data) + ") from data partition: " + str(workersJob[(address[0], workerPort)])
            print(msgInfo)
            logging.info(msgInfo)
            mutex.release()
            numberResultsReceived += 1


            if(numberResultsReceived == nbNodes): # when we have received everything
                result = round(partialResult, meanDecimals)

                # We close both sockets letting connectServer socket  get out
                # of the recv blocking call when all data is processed
                workerSocket.close()
                clientSocket.close()
                connectServer.close()
                msgInfo = "The result of the mean asked is: " + str(result)
                print(msgInfo)
                logging.info(msgInfo)
                break


# We ask for the client port to be used
clientPort = input("Input client port number:\n")
logging.basicConfig(filename='client' + clientPort + '.log', level=logging.DEBUG)
clientPort = int(clientPort)

msgInfo = "Starting the client connection..."
logging.info(msgInfo)
print(msgInfo)

# We open listening socket on the client
try:
    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientSocket.bind((host, clientPort))
except Exception as e:

    msgInfo = "Error with listening socket: " + str(e)
    if(printMode):
        print(msgInfo)
    logging.error(msgInfo)

    sys.exit(1)

main()
