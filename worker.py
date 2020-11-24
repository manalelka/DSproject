import threading
import socket
import time
import pickle
from include import *

host = '127.0.0.1'
serverPort = SERVER_PORT_WORKERS


# Creating a worker port to listen to client and possibly with each other?
workerPort = 1111
workerServer = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
workerServer.bind((host,workerPort))
workerServer.listen()

isReady = True

def pingServer():
    while True:
        try:
            # Connecting to server port
            connectServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connectServer.connect((host, serverPort))
            if isReady:
                msg = 'Yes ' + str(workerPort)
                connectServer.send(msg.encode('ascii'))
            else:
                msg = 'No ' + str(workerPort)
                connectServer.send(msg.encode('ascii'))
            connectServer.close()
            time.sleep(PING_SLEEP)
        except:
            print("An error occurred!")
            connectServer.close()
            break

def handleDataFromClient():
    while True:
        client,address = workerServer.accept()
        try:
            data = client.recv(1024)
            y = pickle.loads(data)
            print(y)
            processData(y)
            isReady = True
        except:
            print('Client has left')
            client.close()
            break

def processData(data):
    isReady = False
    time.sleep(30)
    result = sum(data)
    return data


pingThread = threading.Thread(target = pingServer)
pingThread.start()
handleDataThread = threading.Thread(target = handleDataFromClient)
handleDataThread.start()
