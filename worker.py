import threading
import socket
import time
import pickle


host = '127.0.0.1'
serverPort = 1

# Connecting to server port
connectServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
connectServer.connect((host, serverPort))

# Creating a worker port to listen to client and possibly with each other?
workerPort = 11
workerServer = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
workerServer.bind((host,workerPort))
workerServer.listen()

isReady = True

def handlePingFromServer():
    while True:
        try:
            message = connectServer.recv(1024).decode('ascii')
            if message == 'Are you ready?' and isReady:
                connectServer.send('Yes'.encode('ascii'))
            else:
                connectServer.send('No'.encode('ascii'))
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


handlePingThread = threading.Thread(target = handlePingFromServer)
handlePingThread.start()
handleDataThread = threading.Thread(target = handleDataFromClient)
handleDataThread.start()
