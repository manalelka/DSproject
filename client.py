import socket
import threading
import time
import pickle
y=[0,12,6,8,3,2,10]
data=pickle.dumps(y)
host = '127.0.0.1'
print(data)


serverPort = 1

# Connecting to server
connectServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
connectServer.connect((host, serverPort))

# Connecting to worker
workerPort = 11
connectWorker = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
connectWorker.connect((host,workerPort))

connectWorker.send(data)

# def sendDataToWorker():
#     while True:
#         try:
