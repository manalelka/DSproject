# **Distributed Analysis of a Dataset**



Our application is a distributed system that serves as a tool for distributed analysis of a specific dataset.

## Architecture 

Our code consists of three main components : client.py , server.py and worker.py.

![Simplified architecture of the application](https://i.imgur.com/c4qEIAd.png)



**- Client** :

The client initiates a connection with the server and asks him for a number of working nodes. After receiving the IP addresses and the port numbers of the available working nodes from the server, the client splits the dataset accordingly and sends the subsets of the dataset to the available nodes. He collects the final result by the end.

**- Server** :

The server has the main role of managing a queue of the working nodes and establishing a link between the client(s) and the worker(s). 

**- Worker** :

The workers are able to communicate between each other and to compute a final result that is later collected by the client.

## Usage 

Our application is a simplified prototype of a distributed data analysis tool. To demonstrate it, all is needed is to run the three python files simultaneously with the command : `python *fileName*.py` 

The only technical requirement is to have Python3 installed on the local machine.

Our system is able to handle more than one client and more than one worker.