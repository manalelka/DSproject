import socket
import threading
import time
import pickle
import signal
import sys
import os
import json
from include import *
import logging


availableWorkers = []  # list of addresses of the available workers
notReadyWorkers = []  # list of addresses of the not available workers
workersLeftToSend = {} # dictionary with client: numberWorkers, numberWorkers is the number of workers the client still needs
tsWorkers = {}  # dictionary with workerAddress: ts, ts the timestamp of the last interaction we had with the worker
jobs = {}  # dictionary with client: workers, workers processing the client petition at the moment
mutex = threading.Lock() # mutex to protect workersLeftToSend
mutexJobs = threading.Lock() # mutex to protect jobs
mutexNotAvailable = threading.Lock() # mutex to protect notReadyWorkers
mutexAvailable = threading.Lock() # mutex to protect availableWorkers
host = HOST
logging.basicConfig(filename='server.log', level=logging.DEBUG)


def signalHandler(sig, frame):
    """
        Name: signalHandler
        Description: Function that handles SIGINT to end the program.
        Arguments:
            -sig: signal number.
            -frame: current stack frame.
        Return: None.
    """
    msgInfo = 'Closing the server...'
    print(msgInfo)
    logging.info(msgInfo)
    sys.exit(0)


def moveToNotAvailable(address):
    """
        Name: moveToNotAvailable
        Description: Function that moves a certain address to the list of not
            available.
        Arguments:
            -address: address of the form (ip, port) to move.
        Return: None.
    """
    global mutexNotAvailable

    mutexNotAvailable.acquire()
    if address not in notReadyWorkers:
        # Checking if it is in the availableWorkers list and, if so, removing it
        try:
            i = availableWorkers.index(address)
            del availableWorkers[i]
        except:
            pass
        msgInfo = "Worker at: " + str(address) + ' is not available'
        print(msgInfo)
        logging.info(msgInfo)
        # Append to notReadyWorkers in any case
        notReadyWorkers.append(address)
    mutexNotAvailable.release()


def moveToAvailable(address):
    """
        Name: moveToAvailable
        Description: Function that moves a certain address to the list of
            available workers.
        Arguments:
            -address: address of the form (ip, port) to move.
        Return: None.
    """
    global mutex

    mutexAvailable.acquire()
    if address not in availableWorkers:
        # Checking if it is in the notReadyWorkers list and, if so, removing it
        try:
            i = notReadyWorkers.index(address)
            del notReadyWorkers[i]
        except:
            pass
        msgInfo = "Worker at: " + str(address) + ' is ready'
        print(msgInfo)
        logging.info(msgInfo)
        # Append to availableWorkers in any case
        availableWorkers.append(address)
    mutexAvailable.release()


def listenWorkers():
    """
        Name: listenWorkers
        Description: Function that creates a socket to listen to the workers.
        Arguments: None.
        Return: None.
    """
    # Create socket to listen to workers
    try:
        workersSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        workersSocket.bind((host, SERVER_PORT_WORKERS))
        workersSocket.listen()
    except Exception as e:
        msgInfo = "Error with listening socket to workers: " + str(e)
        if(printMode):
            print(msgInfo)
        logging.error(msgInfo)
        sys.exit(0)

    while True:
        # Accept worker connection and process its message
        worker, address = workersSocket.accept()
        try:
            msg = worker.recv(1024)
        except Exception as e:
            msgInfo = 'Error receiving message from worker: ' + str(e)
            if(printMode):
                print(msgInfo)
            logging.error(msgInfo)
            worker.close()
            continue

        # Depending on the message save the worker in the corresponding list
        msg = pickle.loads(msg)
        if msg[0] == 'PING':
            pass # everything is fine so we don't do anything
        elif msg[0] == 'Yes':
            moveToAvailable((address[0], int(msg[1])))
        elif msg[0] == 'No':
            moveToNotAvailable((address[0], int(msg[1])))

        # Save the timestamp of the communication to check dead workers
        tsWorkers[(address[0], int(msg[1]))] = time.time()
        # We just received one more worker, so maybe a client can use it
        sendWorkers()


def sendWorkers():
    """
        Name: sendWorkers
        Description: Function that sees available workers and send them to the
            clients.
        Arguments: None.
        Return: None.
    """
    global mutex
    global workersLeftToSend

    mutex.acquire()

    noMoreWorkers = False  # flag for when we give all the workers

    # We go through the list of clients and the number of workers they still need
    dictCopy = workersLeftToSend.copy()
    for job in dictCopy.keys():
        client = job
        wantedWorkers = workersLeftToSend[job] #number of workers wanted
        l = len(availableWorkers)
        workersToSend = [] #list of workers we will be sending

        if l > 0:
            # If we have more than the needed workers we assign those to the client and continue
            if l >= wantedWorkers:
                workersToSend = availableWorkers[:wantedWorkers]
                del workersLeftToSend[client]
            # else we send the workers we have and update the needed workers by the client
            else:
                workersToSend = availableWorkers[:l]
                workersLeftToSend[client] = wantedWorkers - l
                # We don't have more clients to send so we want to end the cycle
                noMoreWorkers = True

            length = len(workersToSend)
            if length > 0: # if there are workers to send
                msg = json.dumps([NEW_WORKERS, workersToSend])
                try:
                    client.send(bytes(msg, encoding="utf-8"))
                except socket.error as e:
                    # if there is a connection error we delete the client from the list
                    if job in workersLeftToSend:
                        del workersLeftToSend[client]
                    mutexJobs.acquire()
                    jobs.pop(client, None)
                    mutexJobs.release()
                    msgInfo = 'Warning: error sending to client: ' + str(e)
                    if(printMode):
                        print(msgInfo)
                    logging.warning(msgInfo)
                    continue
                except Exception as exce:
                    msgInfo = 'Error sending new workers to client: ' + str(exce)
                    if(printMode):
                        print(msgInfo)
                    logging.error(msgInfo)
                    continue

                # We move the sent workers to not available
                for addr in workersToSend:
                    moveToNotAvailable(addr)

                mutexJobs.acquire()
                # We update the workers doing an specific job
                if client in jobs:
                    jobs[client] += workersToSend
                else:
                    jobs[client] = workersToSend
                mutexJobs.release()

        else:
            noMoreWorkers = True

        if(noMoreWorkers): # we stop sending workers
            break
    mutex.release()


def listenClients():
    """
        Name: listenClients
        Description: Function that open a socket to listen to new clients and
            communicate with them.
        Arguments: None.
        Return: None.
    """
    global workersLeftToSend

    # We wait for a connection with a client
    try:
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientSocket.bind((host, SERVER_PORT_CLIENTS))
        clientSocket.listen()
    except Exception as e:
        msgInfo = "Error with connection socket with client: " + str(e)
        if(printMode):
            print(msgInfo)
        logging.error(msgInfo)
        os._exit(1)

    while True:
        try:
            client, address = clientSocket.accept()
        except Exception as e:
            msgInfo = "Error connecting with client: " + str(e)
            if(printMode):
                print(msgInfo)
            logging.error(msgInfo)
            continue

        # We receive from the client how many workers it wants
        try:
            data = client.recv(1024)
        except Exception as e:
            msgInfo = "Error receiving from client: " + str(e)
            if(printMode):
                print(msgInfo)
            logging.error(msgInfo)
            client.close()
            continue

        # We receive the number of nodes wanted and we tell the client that number
        # or the maximum number of workers we permit
        dum, wantedNodes = pickle.loads(data)
        msgInfo = 'Client wants ' + str(wantedNodes) + ' workers.'
        print(msgInfo)
        logging.info(msgInfo)
        numberWorkers = min(wantedNodes, MAX_WORKERS)
        numberWorkersSerialized = pickle.dumps(numberWorkers)

        try:
            client.send(numberWorkersSerialized)
        except Exception as e:
            msgInfo = "Error sending number of workers to client: " + str(e)
            if(printMode):
                print(msgInfo)
            logging.error(msgInfo)
            client.close()
            continue

        # We update the number of workers the client needs
        mutex.acquire()
        workersLeftToSend[client] = numberWorkers
        mutex.release()

        msgInfo = 'Sent number of workers: ' + str(numberWorkers)
        print(msgInfo)
        logging.info(msgInfo)

        # We check if there are free workers for the client
        sendWorkers()

        # We don't close the connection because we will use it
        # to inform the client for workers failing and resend workers


def checkJobs():
    """
        Name: checkJobs
        Description: Function that check that workers from a job are alive and,
            if they are not, tell the corresponding client.
        Arguments: None.
        Return: None.
    """
    global workersLeftToSend
    global mutex

    while True:
        # We check the workers working right now
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
                    # if there is a connection error we delete the client from the list
                    mutex.acquire()
                    if client in workersLeftToSend:
                        del workersLeftToSend[client]
                    mutex.release()
                    mutexJobs.acquire()
                    jobs.pop(client, None)
                    mutexJobs.release()
                    continue
                except Exception as exce:
                    msgInfo = "Error sending dead workers to client: " + str(e)
                    if(printMode):
                        print(msgInfo)
                    logging.error(msgInfo)
                    continue

                # The client workers have now changed so we calculate the new list
                clientWorkers = list(set(clientWorkers) - set(deadWorkers))
                mutexJobs.acquire()
                jobs[client] = clientWorkers
                mutexJobs.release()
                # We update the new number of needed workers by the client
                mutex.acquire()
                try:
                    workersLeftToSend[client] += lenDeathWorkers
                except:
                    workersLeftToSend[client] = lenDeathWorkers
                finally:
                    mutex.release()
                # We send workers if they are free
                sendWorkers()
        time.sleep(CHECK_JOBS_SLEEP)


def checkWorkers():
    """
        Name: checkWorkers
        Description: Function that checks workers' last connection with the server
            to see wether they are dead or alive.
        Arguments: None.
        Return: None.
    """
    # Check ts of workers and, in case it's too large, declare the worker not available
    while True:
        for addr, ts in tsWorkers.items():
            if (time.time() - ts) > MAX_TSDIFF:
                moveToNotAvailable(addr)
        time.sleep(CHECK_WORKERS_SLEEP)


def startServer():
    """
        Name: startServer
        Description: main function to initialize the server.
        Arguments: None.
        Return: None.
    """
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

    msgInfo = "Server started"
    print(msgInfo)
    logging.info(msgInfo)

msgInfo = 'Server is starting'
print(msgInfo)
logging.info(msgInfo)

startServer()
