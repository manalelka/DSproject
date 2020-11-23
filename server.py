import socket
import threading
import time

host = 'localhost'
port = 1

server = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
server.bind((host,port))
server.listen()

workers = []
client = ''

def pingWorker():
    while(True):
        time.sleep(5)
        for worker in workers:
            worker.send('Are you ready?'.encode('ascii'))

def handleWorker (worker):
    while True:
        try:
            ready = worker.recv(1024).decode('ascii')
            if ready == 'Yes':
                print(ready)
                print('Worker is ready')
            else:
                print(ready)
                print('Worker is not ready')
        except:
            index = workers.index(worker)
            workers.remove(worker)
            worker.close()
            print('A worker',worker,'has left')
            break

def go():
    while True:
        worker,address = server.accept()
        print(f'Connected with {str(address)}')
        workers.append(worker)
        thread = threading.Thread(target=handleWorker, args=(worker,)) # If it is ready, do something ... (currently only print it is ready)
        thread.start()
        thread2 = threading.Thread(target=pingWorker) # Ping every 5 seconds to check readiness
        thread2.start()

print('Server is starting')
go()
