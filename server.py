import socket
import threading
import time
from include import *

availableWorkers = [] #list of addresses of the available workers
notReadyWorkers = [] #list of addresses of the not available workers
workersLeftToSend = [] #list of tuples [client, numberWorkers], numberWorkers is the number of workers the client still needs
tsWorkers = {} #dictionary with workerAddress: ts, ts the timestamp of the last interaction we had with the worker
jobs = {} #dictionary with client: workers, workers processing the client petition at the moment

def moveToNotAvailable(address):
    #Check if it was already between the available workers, delete itif so, and
    # append it to not ready ones
	if address not in notReadyWorkers:
		try:
			i = availableWorkers.index(address)
			del availableWorkers[i]
		except:
			pass
		notReadyWorkers.append(address)

def moveToAvailable(address):
    #Check if it was already between not ready workers, delete it if so, and
    # append it to available ones
	if address not in availableWorkers:
		try:
			i = notReadyWorkers.index(address)
			del notReadyWorkers[i]
		except:
			pass
		availableWorkers.append(address)

def listenWorkers():
    #Create socket to listen to workers
    workersSocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    workersSocket.bind((HOST,SERVER_PORT_WORKERS))
    workersSocket.listen()

    while True:
        #Accept worker connection and process its message
        worker,address = workersSocket.accept()

        #Depending on the message save the worker in the corresponding list
        try:
			#TODO check msg well sent (correct format)
            msg = worker.recv(1024).decode('ascii')
            if msg[:3] == 'Yes':
                moveToAvailable((address[0], int(msg[4:])))
                print("Worker at: " + str(address) + msg[4:] + ' is ready')
            elif msg[:2] == 'No':
                moveToNotAvailable((address[0], int(msg[4:])))
                print("Worker at: " + str(address) + msg[4:] + ' is not ready')

            #Save the timestamp of the communication to later check dead workers
            tsWorkers[(address[0], int(msg[4:]))] = time.time()
            #We just received one more worker, so maybe a client can use it.
            sendWorkers()
            print(tsWorkers)
            print(availableWorkers)
        except:
			#TODO not exactly this but i think even the port is wrong it should then work
            moveToNotAvailable(address)
        	#finally:
            worker.close()

def sendWorkers():
    #Send to the client the workers it can use
    noMoreWorkers = False #flag for when we give all the workers

    for i in workersLeftToSend.copy():
        client = i[0]
        wantedWorkers = i[1]
        l = len(availableWorkers)

        #If we have more than the needed workers we assign those to the client and continue
        if l > wantedWorkers:
            workersToSend = availableWorkers[:wantedWorkers]
            workersLeftToSend.remove(i)
        #else we send the workers we have and update the needed workers by the client
        else:
            workersToSend = availableWorkers[:l]
            ind = workersLeftToSend.index(i)
            workersLeftToSend[ind][1] = wantedWorkers - l
            #We don't have more clients to send so we want to end the cycle
            noMoreWorkers = True

        for addr in workersToSend:
            moveToNotAvailable(addr)
        client.send([NEW_WORKERS,workersToSend])

        jobs[client] += workersToSend

        if(noMoreWorkers):
            break

def listenClients():
    #We wait for a connection with a client
    clientSocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    clientSocket.bind((HOST,SERVER_PORT_CLIENTS))
    clientSocket.listen()

    while True:
        client, address = clientSocket.accept()
        try:
            #We receive from the client how many workers it wants
            data = client.recv(1024)
            wantedNodes = data[9] #ask for 3 ? TODO decide msg

            #We decide how many workers we give the client
            l = len(availableWorkers)
            numberWorkers = min(wantedWorkers, MAX_WORKERS)
            workersLeftToSend.append([client,numberWorkers])

            #We check if there are free workers for the client
            sendWorkers()

            #We don't close the connection because we will use it
            # to inform the client for workers failing and resend workers
        except:
            client.close()


def checkJobs():
    #Check the workers in a job are still alive and working
    while True:
        for client in jobs.keys():
            deadWorkers = []
            clientWorkers = jobs[client]
            for addr in clientWorkers:
                try:
                    #If it's been inactive for too long we declare it dead
                    i = notReadyWorkers.index(addr)
                    if time.time() - tsWorkers[addr] > MAX_TSDIFF:
                        deadWorkers.append(addr)
                except:
                    pass
            lenDeathWorkers = len(deadWorkers)
            #If there are dead workers we search for new workers to substitute the dead
            if lenDeathWorkers > 0:
                newWorkers = searchWorkers(lenDeathWorkers)
                client.send([DEAD_WORKERS,deadWorkers])
                clientWorkers = list(set(clientWorkers) - set(deadWorkers))
                jobs[client] = clientWorkers

                #We update the new number of needed workers by the client
                try:
                    i = workersLeftToSend.index(client)
                    workersLeftToSend[i][1] += lenDeathWorkers
                except:
                    workersLeftToSend = [client,lenDeathWorkers] + workersLeftToSend
                #We send workers if they are free
                sendWorkers()
        time.sleep(CHECK_JOBS_SLEEP)


def checkWorkers():
    #Check ts of workers and, in case it's too large, declare the worker not available
    while True:
        for addr, ts in tsWorkers.items():
            if (time.time() - ts) > MAX_TSDIFF:
                #TODO not doing it ??
                moveToNotAvailable(addr)
        time.sleep(CHECK_WORKERS_SLEEP)

def startServer():

    threadHandleClients = threading.Thread(target=listenClients) #listen from connections from clients
    threadHandleClients.start()

    threadHandleWorkers = threading.Thread(target=listenWorkers) #listen from connections from workers
    threadHandleWorkers.start()

    threadJobs = threading.Thread(target=checkJobs) #check if jobs are fine or else warns the client
    threadJobs.start()

    threadCheckWorkers = threading.Thread(target=checkWorkers) #check if workers are dead
    threadCheckWorkers.start()

print('Server is starting')
startServer()
