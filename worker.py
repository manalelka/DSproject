import threading
import socket
import time
import pickle
import numpy
from include import *

host = HOST
serverPort = SERVER_PORT_WORKERS

#TODO: LET THEM DO THE MEAN CALLING EACH OTHER. DO IT THINKING... DONT LET WORKERS INTERACT WITH EACH OTHER IN A CRAZY WAY. MAYBE COMMUNICATE WITH SERVER TO FIND WORKER WILLING TO DO THE MEAN

def pingServer():
    while True:
        try:
            msgServer("PING")
            time.sleep(PING_SLEEP)
        except:
            break

def msgServer(msg):
    try:
        dumpMsg = pickle.dumps([msg,workerPort])
        # Connecting to server port
        try:
            connectServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error as e:
            print("Error creating socket connection with server: " + e)

        connectServer.connect((host, serverPort))
        connectServer.send(dumpMsg)
        connectServer.close()
    except:
        print("An error occurred when sending msg to server!")
        connectServer.close()

def handleDataFromClient():
    while True:
        client,address = workerServer.accept()
        # We tell the server we are not ready
        msgServer("No")
        try:
            data = client.recv(1024)
            clientPort,data = pickle.loads(data)
            print("Received the following data:")
            print(data)
            result = processData(data)
            sendResult(address[0],clientPort,result)
            msgServer("Yes")
        except:
            print('Client has left')
            client.close()
            break

def sendResult(addressIp,port,result):
    #try:
    try:
        connectClient = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error as e:
        print("Error creating socket connection with client: " + e)

    connectClient.connect((addressIp, port))
    resultPickle= pickle.dumps([workerPort,result])
    connectClient.send(resultPickle)
    connectClient.close()
    #except:
        #print("An error occurred when sending result to client!")
        #connectClient.close()

def processData(data):
    result = sum(data)
    time.sleep(0)
    return result


# Creating a worker port to listen to client
workerPort = input("Introduce worker port number:\n")
workerPort = int(workerPort)

try:
    workerServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
except socket.error as e:
    print("Error creating socket connection to listen to the client: " + e)

workerServer.bind((host,workerPort))
workerServer.listen()

#We tell the server we are ready
msgServer("Yes")
pingThread = threading.Thread(target = pingServer)
pingThread.start()
handleDataThread = threading.Thread(target = handleDataFromClient)
handleDataThread.start()
