import threading
import socket
import time
import pickle
import numpy as np
from include import *
import sys
import os

host = HOST
serverPort = SERVER_PORT_WORKERS

def pingServer():
    while True:
        if msgServer("PING"):
            time.sleep(PING_SLEEP)
        else:
            os._exit(1)

def msgServer(msg):
    #Connecting to server port and sending a message
    try:
        connectServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connectServer.connect((host, serverPort))
        dumpMsg = pickle.dumps([msg,workerPort])
        connectServer.send(dumpMsg)
    except Exception as e:
        print("Error with connection socket with server: " + str(e))
        connectServer.close()
        return False

    connectServer.close()
    return True


def handleDataFromClient():
    while True:
        try:
            client,address = workerServer.accept()
        except Exception as e:
            print("Error accepting client connection: " + str(e))
            client.close()
            continue

        #We tell the server we are not ready
        if not msgServer("No"):
            client.close()
            os._exit(1)

        #We expect a dataset from the client
        try:
            data = client.recv(1024)
        except Exception as e:
            print("Error receiving data from client: " + str(e))
            client.close()
            sys.exit(0)
            #TODO kill completely or tell server im up again
            #We tell the server we are ready again
            if not msgServer("Yes"):
                sys.exit(0)

        #client.close() #TODO not 100 sure
        clientPort,data = pickle.loads(data)
        print("Received the following data: " + str(data))

        #We process the data received and send it back to the client
        result = processData(data)
        print('Sending result: ' + str(result))
        sendResult(address[0],clientPort,result)

        #We tell the server we are available from now on
        if not msgServer("Yes"):
            sys.exit(0)


def sendResult(addressIp,port,result):
    #We connect to the server to send back the result
    try:
        connectClient = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connectClient.connect((addressIp, port))
    except Exception as e:
        print("Error connection socket with client: " + str(e))
        connectClient.close()
        return

    #We send the result we got
    try:
        resultPickle = pickle.dumps([workerPort,result])
        connectClient.send(resultPickle)
    except Exception as e:
        print("Error sending result to client: " + str(e))
    finally:
        connectClient.close()


def processData(data):
    if type(data) == int or type(data) == float or type(data) == np.float64:
        return data
    result = sum(data)
    #time.sleep(5)
    return result

#Creating a worker port to listen to client
workerPort = input("Introduce worker port number:\n")
workerPort = int(workerPort)

try:
    workerServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    workerServer.bind((host,workerPort))
    workerServer.listen()
except Exception as e:
    print("Error connection socket to listen to the client: " + str(e))
    workerServer.close()
    sys.exit(1)

#We tell the server we are ready
if not msgServer("Yes"):
    workerServer.close()
    sys.exit(1)

#We ping the server from time to time so that it knows we are alive
pingThread = threading.Thread(target = pingServer)
pingThread.start()

#We start a thread to handle the data sent from the client
handleDataThread = threading.Thread(target = handleDataFromClient)
handleDataThread.start()
