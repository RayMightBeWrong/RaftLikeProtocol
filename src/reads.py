import time, logging
from threading import Lock, Thread
from math import comb
from random import Random
from ms import reply, send
from state import State as RaftVars

class ReadsState:
    def __init__(self, rv : RaftVars):
        self.probReadFromLeader = calculateProbabilityOfQueryingLeader(rv.getNumNodes()) # Probability(%) of querying the leader instead of performing a quorum read

        self.readTout = 300 # read timeout, i.e., amount of time to wait before trying to redo the reading operation
        self.idleTime = 100 # when there are no read entries, the thread sleeps this amount of time, before checking again 
        self.readAdditionalAttempts = 2 # max number of additional attemps

        #To associate an ID with the read requests from the clients
        self.nextReadID = 0 

        #Dictionary to map an ID to a read request,
        # and in case of a quorum read, store the answers
        # of the contacted nodes-
        #The mapping is deleted after the answer is sent to the client.
        #Example of values:
        # 1. (msg, "quorum", {node1: (value1, index1, term1), node2: (value2, index2, term2)})
        # 2. (msg, "leader", leaderID)
        self.activeReads = dict()

        # key: read ID ---> value: (timeout, nr of attemps left) 
        self.readTimeouts = dict()
        
        self.readsLock = Lock() # exclusive lock for read operations. Not used in auxiliary functions.
        self.randomizer = Random(rv.getNodeId()) # For operations that require randomness. The seed is the name of the node itself
        self.rv = rv # Raft Variables
        self.readTimeoutsThread = self.__initReadTimeoutsThread() # Thread used to control the timeouts and issue new read attempts

    #Returns true if the follower should query the leader
    # to answer the client's read request
    def maybeQueryLeader(self) -> bool:
        return self.randomizer.uniform(0,100) < self.probReadFromLeader

    #Adds a new read entry, and sets a timeout.
    #Retuns: identification of the read entry.
    #Arguments:
    # - msg: Client's read request
    # - readType: "quorum" or "leader"
    # - readCustomInfo: any custom information to be associated with the entry
    def __addNewRead(self, msg, readType, readCustomInfo) -> int:
        readID = self.nextReadID
        self.nextReadID += 1
        self.activeReads[readID] = (msg, readType, readCustomInfo)
        self.readTimeouts[readID] = (time.time() + self.readTout / 1000, self.readAdditionalAttempts)
        return readID

    #Adds a new leader read entry and timeout
    def addNewLeaderRead(self, msg, leaderID) -> int:
        return self.__addNewRead(msg, "leader", leaderID)
    
    #Adds a new quorum read entry and timeout
    def addNewQuorumRead(self, msg) -> int:
        return self.__addNewRead(msg, "quorum", {})
    
    #Adds a new quorum read entry and timeout
    def addNewQuorumReadWithAttempts(self, msg, attempts) -> int:
        readID = self.nextReadID
        self.nextReadID += 1
        self.activeReads[readID] = (msg, "quorum", {})
        self.readTimeouts[readID] = (time.time() + self.readTout / 1000, attempts)
        return readID

    #Deletes read entry and timeout
    def deleteRead(self, readID):
        del self.activeReads[readID]
        del self.readTimeouts[readID]
    
    #Get read entry
    def getReadEntry(self, readID):
        return self.activeReads.get(readID)
    
    #Get read entry's timeout and attempts
    def getReadTimeoutAndAttempts(self, readID):
        return self.readTimeouts.get(readID)
    
    #If possible, decreases the number of attempts from a read entry by creating a new one. Deletes the past entry and resets the timeout.
    #If there are no more attempts, an error response is sent to the client.
    #Returns:
    # - a new read ID, if the attempt is successful 
    # - 'None' if there is no entry with the given 'readID', or if there are no more attempts.
    #The function does not issue another read operation.
    def decreaseAttempts(self, readID, readType, leaderID) -> int | None:
        pastEntry = self.activeReads.get(readID)

        #If the entry does not exist, cannot decrease nr of attempts
        if pastEntry != None:
            #Gets the attempts and decreases it by one unit
            _,attempts = self.readTimeouts[readID]
            
            #Deletes entry and timeout
            del self.activeReads[readID]
            del self.readTimeouts[readID]

            #Can only add another entry if the 
            # number of attempts is zero or positive.
            if attempts > 0:
                if readType == 'quorum':
                    return self.addNewQuorumReadWithAttempts(pastEntry[0], attempts - 1)
                else:
                    return self.addNewLeaderRead(pastEntry[0], leaderID)
            else:
                reply(pastEntry[0], type="error", code=11) # replies to client with timeout error
                return None
        else:
            return None

    
    
    ########### Quorum/Leader Read Timeouts ###########

    def __initReadTimeoutsThread(self):
        self.readTimeoutsThread = Thread(target=self.readTimeoutsLoop, args=())
        self.readTimeoutsThread.start()

    def readTimeoutsLoop(self):
        lowestTout = None

        while True:
            lowestTout = float("inf") # sets the variable to infinite

            self.readsLock.acquire()
            
            for readID in list(self.readTimeouts.keys()):
                tout, attempts = self.readTimeouts[readID]

                #if the read timed out, handle the client's read request again
                if tout < time.time():
                    
                    m = self.getReadEntry(readID)
                    if m != None:             
                        #acquire the locks in the correct order to avoid deadlock
                        self.readsLock.release()
                        self.rv.lock.acquire()
                        self.readsLock.acquire()
                        
                        #Only redo read operation if there are attempts left, otherwise
                        # inform the client that it was not possible
                        if attempts > 0:
                            logging.info("Retrying read(ID=" + str(readID) + "): " + str(m[0]))

                            #Can only perform a leader read if the node knows who the leader is.
                            #Even if the node knows the leader it only performs a leader read
                            # with a certain probability. 
                            if self.rv.leader_id != None and self.maybeQueryLeader():
                                #add new leader read entry and set a timeout
                                readID = self.decreaseAttempts(readID, "leader", self.rv.leader_id) 
                                if readID != None:
                                    logging.info("Trying leader read(ID=" + str(readID) + "): " + str(m[0]))
                                    #Ask leader for the value
                                    send(self.rv.getNodeId(), self.rv.leader_id, type="read_leader", read_id=readID, key=m[0].body.key)
                            else:
                                #add new quorum read entry and set a timeout
                                readID = self.decreaseAttempts(readID, "quorum", None)
                                if readID != None:
                                    logging.info("Trying quorum read(ID=" + str(readID) + "): " + str(m[0]))
                                    #broadcast to a random subset of nodes (excluding the node itself and the leader). 
                                    #The number of nodes must be enough to form a majority
                                    broadcastToRandomSubset(self.rv.getNodeId(), self.rv.getNodes(), self.rv.leader_id, self.randomizer, 
                                                            type="read_quorum", read_id=readID, key=m[0].body.key)

                            self.rv.lock.release()
                        else:
                            reply(m[0], type="error", code=11) # replies to client with timeout error
                            
                #else tries to update the 'lowestTout' to set the sleep time until the next 
                elif tout < lowestTout:
                    lowestTout = tout
            
            self.readsLock.release()

            # if the lowestTout changed sleep until that time
            if lowestTout != float("inf"):
                sleepTime = time.time() - lowestTout
                if sleepTime > 0: time.sleep(sleepTime)
            else:
                time.sleep(self.idleTime / 1000)

    #################################### // End of ReadsState class // ####################################


########## Auxiliary Functions ##########

#Calculates the probability of querying the leader when
# performing a read operation.
def calculateProbabilityOfQueryingLeader(nodeCount):
    P = None
    if nodeCount <= 3: P = 1
    else: P = comb(nodeCount - 3, nodeCount // 2 - 1) / comb(nodeCount - 2, nodeCount // 2)
    return (P * (nodeCount - 2)) / (nodeCount + P * (nodeCount - 2)) * 100

#Gets the most updated value by comparing
# the index and term of each answer.
def getMostUpdatedValue(answers):
    max_index = 0 
    max_term = 0
    max_value = None # value associated with the most updated combination of term and index

    for value, index, term in answers.values():
        if (term == max_term and index > max_index) or term > max_term:
            max_term = term
            max_index = index
            max_value = value

    return max_value

#Broadcast to random subset of nodes, 
# excluding the leader and the node itself.
def broadcastToRandomSubset(node_id, node_ids, leader_id, randomizer, **body):
    nodeCount = len(node_ids)
    #Random selection of subset size. The value must be at least N // 2, 
    # so that a majority can be reached when counting the node itself.
    #The maximum value is number of nodes excluding the leader and the node itself (N - 2).
    subsetSize = randomizer.randint(nodeCount // 2, nodeCount - 2)

    #Instead of performing 'subsetSize' number of operations,
    # by removing, to the total number of nodes, 2 to exclude 
    # the leader and the node itself, and 'subsetSize',
    # we can get a random subset by performing the least amount
    # of random selections.
    nrRandRemoves = nodeCount - 2 - subsetSize 
    
    subset = node_ids.copy() 

    #If the leader is known, we can remove the leader from the subset
    if leader_id != None: 
        subset.remove(leader_id)
    #Otherwise, we perform 1 more random remove selection 
    else:
        nrRandRemoves += 1

    #Remove the node itself from the subset
    subset.remove(node_id)

    # Removes 'nrRandRemoves' nodes randomly
    for i in range(0, nrRandRemoves):
        subset.pop(randomizer.randint(0, len(subset) - 1))

    # Broadcasts to the subset
    for n in subset:
        send(node_id, n, **body)
    
    logging.info("Quorum(readID=" + str(body["read_id"]) + "): " + str(subset))

########## Message Handlers ##########

# Handles leader read request
def handleLeaderRead(rv : RaftVars, msg):
    rv.lock.acquire()

    if rv.state == 'Leader':
        result = rv.stateMachine.get(msg.body.key)
        if result != None:
            reply(msg, type='read_leader_resp', read_id=msg.body.read_id, success=True, value=result[0])
        else:
            reply(msg, type='read_leader_resp', read_id=msg.body.read_id, success=True, value=None)
    else:
        reply(msg, type='read_leader_resp', read_id=msg.body.read_id, success=False, value=None)

    rv.lock.release()

# Handles response to leader read request
def handleLeaderReadResponse(rv : RaftVars, rs: ReadsState, msg):
    readID = msg.body.read_id
    rs.readsLock.acquire() # locks the reads state object
    m = rs.getReadEntry(readID) # gets the entry associated with the read id
    
    # If the entry does not exist, ignores and returns
    if m == None:
        rs.readsLock.release()
        return

    #If the leader read was a success, sends the answer to
    # the client, and deletes the read.
    if msg.body.success:
        reply(m[0], type="read_ok", value=msg.body.value)
        rs.deleteRead(readID)
        rs.readsLock.release()
    #If the leader read was not successful, another attempt will
    # be performed
    else:
        #Can only perform a leader read if the node knows who the leader is.
        #Even if the node knows the leader it only performs a leader read
        # with a certain probability. 
        if rv.leader_id != None and rs.maybeQueryLeader():
            readID = rs.decreaseAttempts(readID, "leader", rv.leader_id)
            if readID != None:
                send(rv.getNodeId(), rv.leader_id, type="read_leader", read_id=readID, key=m[0].body.key)
                logging.info("Trying leader read(ID=" + str(readID) + "): " + str(m[0]))
        else:
            readID = rs.decreaseAttempts(readID, "quorum", None)
            if readID != None:
                logging.info("Trying quorum read(ID=" + str(readID) + "): " + str(msg))
                #broadcast to a random subset of nodes (excluding the node itself and the leader). 
                #The number of nodes must be enough to form a majority
                broadcastToRandomSubset(rv.getNodeId(), rv.getNodes(), rv.leader_id, rs.randomizer, 
                                        type="read_quorum", read_id=readID, key=m[0].body.key)
        rs.readsLock.release()

#Handles quorum read request
def handleQuorumRead(rv : RaftVars, msg):
    rv.lock.acquire()

    result = rv.stateMachine.get(msg.body.key)
    if result != None:
        value,(index,term) = result
        reply(msg, type="read_quorum_resp", 
                   read_id=msg.body.read_id, 
                   value=value, 
                   index=index, 
                   term=term)
    else:
        reply(msg, type="read_quorum_resp", 
                   read_id=msg.body.read_id, 
                   value=None, 
                   index=0, 
                   term=0)
    rv.lock.release()

#Handles response to quorum read request
def handleQuorumReadResponse(rv : RaftVars, rs: ReadsState, msg):
    readID = msg.body.read_id

    #Acquiring locks in this order to avoid deadlocks
    rv.lock.acquire()
    rs.readsLock.acquire()

    #Gets the entry associated with the read id
    m = rs.getReadEntry(readID) 
    
    #If the entry does not exist, ignores and returns
    if m == None:
        rs.readsLock.release()
        rv.lock.release()
        return
    
    #Adds the new answer to the dictionary of answers
    answers = m[2]
    answers[msg.src] = (msg.body.value, msg.body.index, msg.body.term)

    #If the number of answers received (plus 1 to include the node itself) equals to 
    # the majority, then the node can send a response to the client
    if len(answers) + 1 == rv.majority:
        #Deletes read entry and timeout
        rs.deleteRead(readID)

        #Gets key from client's read request
        key = m[0].body.key

        #Adds own answer to the dictionary of answers
        result = rv.stateMachine.get(key)
        if result != None:
            value, (index, term) = result
            answers[rv.getNodeId()] = (value, index, term)
        else:
            answers[rv.getNodeId()] = (None, 0, 0)

        #Replies to client with the most updated value
        reply(m[0], type="read_ok", value=getMostUpdatedValue(answers))

    rs.readsLock.release()
    rv.lock.release()