import socket
import threading
import time
import pickle
import signal
import sys
import os
import json
from include import *

availableWorkers = []  # list of addresses of the available workers
notReadyWorkers = []  # list of addresses of the not available workers
# list of tuples [client, numberWorkers], numberWorkers is the number of workers the client still needs
workersLeftToSend = []
tsWorkers = {}  # dictionary with workerAddress: ts, ts the timestamp of the last interaction we had with the worker
jobs = {}  # dictionary with client: workers, workers processing the client petition at the moment
mutex = threading.Lock()
mutexJobs = threading.Lock()
mutexNotAvailable = threading.Lock()
mutexAvailable = threading.Lock()
host = HOST


def signalHandler(sig, frame):
    print('Closing the server...')
    sys.exit(0)


def moveToNotAvailable(address):
    global mutexNotAvailable
    # Check if it was already between the available workers, delete it if so, and
    # append it to not ready ones
    mutexNotAvailable.acquire()
    if address not in notReadyWorkers:
        try:
            i = availableWorkers.index(address)
            del availableWorkers[i]
        except:
            pass
        print("Worker at: " + str(address) + ' is not available')
        notReadyWorkers.append(address)
    mutexNotAvailable.release()


def moveToAvailable(address):
    global mutex
    # Check if it was already between not ready workers, delete it if so, and
    # append it to available ones
    mutexAvailable.acquire()
    if address not in availableWorkers:
        try:
            i = notReadyWorkers.index(address)
            del notReadyWorkers[i]
        except:
            pass
        print("Worker at: " + str(address) + ' is ready')
        availableWorkers.append(address)
    mutexAvailable.release()


def listenWorkers():
    # Create socket to listen to workers
    try:
        workersSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        workersSocket.bind((host, SERVER_PORT_WORKERS))
        workersSocket.listen()
    except Exception as e:
        print("Error with listening socket to workers: " + str(e))
        sys.exit(0)

    while True:
        # Accept worker connection and process its message
        worker, address = workersSocket.accept()
        # Depending on the message save the worker in the corresponding list
        try:
            msg = worker.recv(1024)
        except Exception as e:
            print('Error receiving message from worker: ' + str(e))
            worker.close()
            continue

        msg = pickle.loads(msg)
        if msg[0] == 'PING':
            pass
        elif msg[0] == 'Yes':
            moveToAvailable((address[0], int(msg[1])))
        elif msg[0] == 'No':
            moveToNotAvailable((address[0], int(msg[1])))

        # Save the timestamp of the communication to later check dead workers
        tsWorkers[(address[0], int(msg[1]))] = time.time()
        # We just received one more worker, so maybe a client can use it.
        sendWorkers()


def sendWorkers():
    global mutex
    global workersLeftToSend

    mutex.acquire()
    # Send to the client the workers it can use
    noMoreWorkers = False  # flag for when we give all the workers

    for job in workersLeftToSend.copy():
        client = job[0]
        wantedWorkers = job[1]
        l = len(availableWorkers)
        workersToSend = []

        if l > 0:
            # If we have more than the needed workers we assign those to the client and continue
            if l >= wantedWorkers:
                workersToSend = availableWorkers[:wantedWorkers]
                workersLeftToSend.remove(job)
            # else we send the workers we have and update the needed workers by the client
            else:
                workersToSend = availableWorkers[:l]
                ind = workersLeftToSend.index(job)
                workersLeftToSend[ind][1] = wantedWorkers - l
                # We don't have more clients to send so we want to end the cycle
                noMoreWorkers = True

            for addr in workersToSend:
                moveToNotAvailable(addr)

            length = len(workersToSend)

            if length > 0:

                msg = json.dumps([NEW_WORKERS, workersToSend])
                try:
                    client.send(bytes(msg, encoding="utf-8"))

                except socket.error as e:
                    if job in workersLeftToSend:
                        workersLeftToSend.remove(job)
                    mutexJobs.acquire()
                    jobs.pop(client, None)
                    mutexJobs.release()
                    print('Error sending to client: ' + str(e))
                    continue

                except Exception as exce:
                    print('Error new workers to client: ' + str(exce))
                    continue

                mutexJobs.acquire()
                if client in jobs:
                    jobs[client] += workersToSend
                else:
                    jobs[client] = workersToSend
                mutexJobs.release()

        else:
            noMoreWorkers = True
        if(noMoreWorkers):
            break
    mutex.release()


def listenClients():
    global workersLeftToSend
    # We wait for a connection with a client
    try:
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientSocket.bind((host, SERVER_PORT_CLIENTS))
        clientSocket.listen()
    except Exception as e:
        print("Error with connection socket with client: " + str(e))
        os._exit(1)

    while True:
        try:
            client, address = clientSocket.accept()
        except Exception as e:
            print("Error connecting with client: " + str(e))
            continue

        try:
            # We receive from the client how many workers it wants
            data = client.recv(1024)
        except Exception as e:
            print("Error receiving from client: " + str(e))
            client.close()
            continue

        dum, wantedNodes = pickle.loads(data)
        print('Client wants ' + str(wantedNodes) + ' workers.')

        # We decide how many workers we give the client and tell the client
        numberWorkers = min(wantedNodes, MAX_WORKERS)
        numberWorkersSerialized = pickle.dumps(numberWorkers)

        try:
            client.send(numberWorkersSerialized)
        except Exception as e:
            print("Error sending number of workers to client: " + str(e))
            client.close()
            continue

        mutex.acquire()
        workersLeftToSend.append([client, numberWorkers])
        mutex.release()

        print('Sent number of workers: ' + str(numberWorkers))

        # We check if there are free workers for the client
        sendWorkers()

        # We don't close the connection because we will use it
        # to inform the client for workers failing and resend workers


def checkJobs():
    global workersLeftToSend
    global mutex
    # Check the workers in a job are still alive and working
    while True:
        for client in set(jobs.keys()):
            deadWorkers = []
            clientWorkers = jobs[client]
            for addr in set(clientWorkers):
                try:
                    # If it's been inactive for too long we declare it dead
                    i = notReadyWorkers.index(addr)
                    if time.time() - tsWorkers[addr] > MAX_TSDIFF:
                        deadWorkers.append(addr)
                except:
                    pass

            lenDeathWorkers = len(deadWorkers)
            # If there are dead workers we search for new workers to substitute the dead
            if lenDeathWorkers > 0:
                msg = json.dumps([DEAD_WORKERS, deadWorkers])

                try:
                    client.send(bytes(msg, encoding="utf-8"))

                except socket.error as e:
                    mutex.acquire()
                    if [client, len(jobs[client])] in workersLeftToSend:
                        workersLeftToSend.remove([client, len(jobs[client])])
                    mutex.release()
                    mutexJobs.acquire()
                    jobs.pop(client, None)
                    mutexJobs.release()
                    continue
                except Exception as exce:
                    print("Error sending dead workers to client: " + str(e))
                    continue

                clientWorkers = list(set(clientWorkers) - set(deadWorkers))
                mutexJobs.acquire()
                jobs[client] = clientWorkers
                mutexJobs.release()
                # We update the new number of needed workers by the client
                mutex.acquire()
                try:
                    ind = workersLeftToSend.index(client)
                    workersLeftToSend[ind][1] += lenDeathWorkers
                except:
                    workersLeftToSend.insert(0, [client, lenDeathWorkers])
                finally:
                    mutex.release()
                # We send workers if they are free
                sendWorkers()
        time.sleep(CHECK_JOBS_SLEEP)


def checkWorkers():
    # Check ts of workers and, in case it's too large, declare the worker not available
    while True:
        for addr, ts in tsWorkers.items():
            if (time.time() - ts) > MAX_TSDIFF:
                moveToNotAvailable(addr)
        time.sleep(CHECK_WORKERS_SLEEP)


def startServer():

    # We capture SIGINT to end gracefully
    signal.signal(signal.SIGINT, signalHandler)

    # listen from connections from clients
    threadHandleClients = threading.Thread(target=listenClients)
    threadHandleClients.start()

    # listen from connections from workers
    threadHandleWorkers = threading.Thread(target=listenWorkers)
    threadHandleWorkers.start()

    # check if jobs are fine or else warns the client
    threadJobs = threading.Thread(target=checkJobs)
    threadJobs.start()

    threadCheckWorkers = threading.Thread(
        target=checkWorkers)  # check if workers are dead
    threadCheckWorkers.start()

    print("Server started")


print('Server is starting')
startServer()
