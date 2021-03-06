import threading
import socket
import time
import pickle
import numpy as np
from include import *
import sys
import os
import logging

host = HOST
serverPort = SERVER_PORT_WORKERS


def pingServer():
    """
        Name: pingServer
        Description: Function to ping the server periodically.
        Arguments: None.
        Return: None.
    """
    while True:
        if msgServer("PING"):
            time.sleep(PING_SLEEP)
        else:
            os._exit(1)


def msgServer(msg):
    """
        Name: msgServer
        Description: Function to send a message to the server.
        Arguments:
            -msg: message to send.
        Return: False if anything goes wrong, else True.
    """
    # Connecting to server port and sending a message
    try:
        connectServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connectServer.connect((host, serverPort))
        dumpMsg = pickle.dumps([msg, workerPort])
        connectServer.send(dumpMsg)
    except Exception as e:
        msgInfo = "Error with connection socket with server: " + str(e)
        if(printMode):
            print(msgInfo)
        logging.error(msgInfo)
        connectServer.close()
        return False

    connectServer.close()
    return True


def handleDataFromClient():
    """
        Name: handleDataFromClient
        Description: Function to handle data received from a client.
        Arguments: None.
        Return: None.
    """
    while True:
        try:
            client, address = workerServer.accept()
        except Exception as e:
            msgInfo = "Error accepting client connection: " + str(e)
            if(printMode):
                print(msgInfo)
            logging.error(msgInfo)
            client.close()
            continue

        # We tell the server we are not available for other works
        if not msgServer("No"):
            client.close()
            os._exit(1)

        # We expect a dataset from the client
        recv = []
        while True:
            try:
                packet = client.recv(1024)
                if not packet:
                    break
                recv.append(packet)
            except Exception as e:
                msgInfo = "Error receiving data from client: " + str(e)
                if(printMode):
                    print(msgInfo)
                logging.error(msgInfo)
                client.close()
                os._exit(1)
        # We already received all the information from the client
        client.close()

        clientPort, data = pickle.loads(b"".join(recv))

        msg = "Received the following data: " + str(data)
        logging.info(msg)
        print(msg)

        # We process the data received and send it back to the client
        result = processData(data)

        msg = 'Sending result: ' + str(result)
        logging.info(msg)
        print(msg)
        sendResult(address[0], clientPort, result)

        # We tell the server we are available from now on
        if not msgServer("Yes"):
            os._exit(1)


def sendResult(addressIp, port, result):
    """
        Name: sendResult
        Description: Function to send the partial result to the client.
        Arguments:
            -addressIp: string address IP of the client.
            -port: integer port of the client.
            -result: result of the partial computation.
        Return: None.
    """
    # We connect to the server
    try:
        connectClient = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connectClient.connect((addressIp, port))
    except Exception as e:
        msgInfo = "Error connection socket with client: " + str(e)
        if(printMode):
            print(msgInfo)
        logging.error(msgInfo)
        connectClient.close()
        return

    # We send the result we got
    try:
        resultPickle = pickle.dumps([workerPort, result])
        connectClient.send(resultPickle)
    except Exception as e:
        msgInfo = "Error sending result to client: " + str(e)
        if(printMode):
            print(msgInfo)
        logging.error(msgInfo)


def meanData(ary):
    """
        Name: meanData
        Description: Function that computes the mean of an array of numbers.
        Arguments:
            -ary: array of numbers to calculate its mean.
        Return: Mean of the data.
    """
    avg = 0
    t = 1
    for x in ary:
        avg += (x - avg) / t
        t += 1
    return avg


def processData(data):
    """
        Name: processData
        Description: Function that processes the data imputed. In this case,
            it calculates its average.
        Arguments:
            -data: data to process.
        Return: list with the length of the data received and the result of the
            computation.
    """
    result = meanData(data)
    #In case you want to kill workers and see the fault tolerance
    #time.sleep(1)
    return [len(data), result]


# Creating a worker port to listen to client
workerPort = input("Introduce worker port number:\n")
logging.basicConfig(filename='client' + workerPort + '.log', level=logging.DEBUG)
workerPort = int(workerPort)

# We create the socket to listen to the server
try:
    workerServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    workerServer.bind((host, workerPort))
    workerServer.listen()
except Exception as e:
    msgInfo = "Error connection socket to listen to the client: " + str(e)
    if(printMode):
        print(msgInfo)
    logging.error(msgInfo)
    workerServer.close()
    sys.exit(1)

# We tell the server we are ready
if not msgServer("Yes"):
    workerServer.close()
    sys.exit(1)


# We ping the server from time to time so that it knows we are alive
pingThread = threading.Thread(target=pingServer)
pingThread.start()

# We start a thread to handle the data sent from the client
handleDataThread = threading.Thread(target=handleDataFromClient)
handleDataThread.start()
