HOST = 'localhost'

SERVER_PORT_WORKERS = 10001 #port used by the server to listen to workers
SERVER_PORT_CLIENTS = 11000 #port used by the server to listen to clients

MAX_WORKERS = 10 #max workers to assign
MAX_TSDIFF = 10 #max ts difference to declare a worker not availabale

# sleep times for several processes
CHECK_JOBS_SLEEP = 1
CHECK_WORKERS_SLEEP = 1
PING_SLEEP = 1

# flags for messages between client and server
NEW_WORKERS = 0
DEAD_WORKERS = 1

#Mean decimals returned
meanDecimals = 10

#If True, it uses prints and logs, else for errors only logs
printMode = True
