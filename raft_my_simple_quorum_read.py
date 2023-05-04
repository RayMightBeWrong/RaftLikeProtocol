#!/usr/bin/env python3

import logging
import time
from threading import Thread, Lock
from ms import receiveAll, reply, send
from random import Random
from datetime import datetime
from math import comb

logging.getLogger().setLevel(logging.DEBUG)


########### global variables ###########

node_id = None
node_ids = None
state = 'Follower' # can be one of the following: 'Follower', 'Candidate', 'Leader'
leader_id = None
majority = None # quantity of nodes necessary to represent the majority
stateMachine = dict()
lock = Lock() # lock of all variables


### Persistent state on all servers ###
#(Updated on stable storage before responding to RPCs)

currentTerm = 0 #latest term the server has seen
votedFor = None #candidateId that received the vote in current term (or null if none)
receivedVotes = set() #data structure to keep the ids of the nodes that granted the vote

#log entries; 
#Each entry contains the msg sent by the client (which contains the command for state machine), 
# and term when entry was received by leader;
#First index is 1
log = []

requestsBuffer = [] # when on 'Follower' state, buffer received Write/CAS requests until next contact with a leader


### Volatile state on all servers ###

#index of highest log entry known to be committed 
# (initialized to 0, increases monotonically)
commitIndex = 0 

#index of highest log entry applied to state machine 
# (initialized to 0, increases monotonically)
lastApplied = 0

probReadFromLeader = None # Probability(%) of querying the leader instead of performing a quorum read

nextReadID = 0 # To associate an ID with the read requests from the clients

#Dictionary to map an ID to a read request,
# and in case of a quorum read, store the answers
# of the contacted nodes-
#The mapping is deleted after the answer is sent to the client.
#Example of values:
# 1. (msg, "quorum", {node1: (value1, index1, term1), node2: (value2, index2, term2)})
# 2. (msg, "leader", leaderID)
activeReads = dict()

readsLock = Lock() # exclusive lock for read operations


### Volatile state on leaders ### 
#(Reinitialized after election)

#for each server, index of the next log entry to send to that server
# (initialized to leader last log index + 1)
nextIndex = dict() 

#for each server, index of highest log entry known to be replicated on server
# (initialized to 0, increases monotonically)
matchIndex = dict()
 
randomizer = None


########### Raft Timeouts ###########

raftTimeoutsThread = None
runTimeoutThread = True # while true keep running the timeout thread 
sendAppendEntriesRPCTout = 50 # timeout to send AppendEntries RPC to every server (in milliseconds)
lowerLimElectionTout = 150 # lower limit for the random generation of the timeout to become candidate in case no message from a leader arrives (in milliseconds)
upperLimElectionTout = 300 # upper limit for the random generation of the timeout to become candidate in case no message from a leader arrives (in milliseconds)
raftTout = None #raft related timeout

def getRandomElectionTimeout():
    return randomizer.randint(lowerLimElectionTout, upperLimElectionTout)

#Depending on the state returns the raft related timeout
def getRaftTimeout():
    if state == 'Leader':
        return sendAppendEntriesRPCTout
    else:
        return getRandomElectionTimeout()

def resetRaftTimeout():
    global raftTout
    raftTout = getRaftTimeout()

def initRaftTimeoutsThread():
    global raftTimeoutsThread
    raftTimeoutsThread = Thread(target=raftTimeoutsScheduler, args=())
    raftTimeoutsThread.start()

def raftTimeoutsScheduler():
    global state, raftTout

    #This will be running in a secondary thread, and the timeouts are important, 
    # so if the node changes to another state it is important to catch that 
    # change as soon as possible.
    # For that we will execute sleeps of min(raftTout, lowest of all timeouts).
    # Since the lowest of all timeouts is the 50 milliseconds for a leader to send
    # a new batch of AppendEntries RPCs, the sleeps will be of a maximum of 50 ms
    
    # set the initial timeout
    lock.acquire()
    resetRaftTimeout()
    lock.release()

    while runTimeoutThread:
        lock.acquire()

        if raftTout > sendAppendEntriesRPCTout:
            #updates 'raftTout' variable, decreasing it by 'sendAppendEntriesRPCTout', 
            # but it can't go bellow 0, so we use the max function 
            raftTout = max(0, raftTout - sendAppendEntriesRPCTout) 

            lock.release() # releases lock before going to sleep

            #dividing by thousand, since the argument is in seconds
            time.sleep(sendAppendEntriesRPCTout / 1000) 
        else:
            auxTout = raftTout
            raftTout = 0
            lock.release() # releases lock before going to sleep
            time.sleep(auxTout / 1000)

        lock.acquire()

        #if the timeout reached 0, then its time to 
        # call the timeout handler and reset the timeout
        if raftTout == 0:
            if state == 'Leader':
                handleSendAppendEntriesRPCTimeout()
            else:
                handleElectionTimeout()
            resetRaftTimeout()

        lock.release()

def handleSendAppendEntriesRPCTimeout():
    sendAppendEntriesRPCToAll()

def handleElectionTimeout():
    global state, currentTerm, votedFor

    # changes to Candidate state, increments term and votes for itself
    changeStateTo('Candidate')
    currentTerm += 1
    votedFor = node_id
    receivedVotes.add(node_id)

    # broadcast RequestVote RPC
    broadcastRequestVoteRPC()



########### Quorum/Leader Read Timeouts ###########

readTimeoutsThread = None
# key: read ID ---> value: (timeout, nr of attemps left) 
readTimeouts = dict()
readTout = 300 # read timeout, i.e., amount of time to wait before trying to redo the reading operation
idleTime = 100 # when there are no read entries, the thread sleeps this amount of time, before checking again 
readAdditionalAttempts = 2 # max number of additional attemps

def initReadTimeoutsThread():
    global readTimeoutsThread
    readTimeoutsThread = Thread(target=readTimeoutsLoop, args=())
    readTimeoutsThread.start()

def readTimeoutsLoop():
    global readsLock
    lowestTout = None
    currentTime = None

    while True: #TODO - change condition
        lowestTout = float("inf") # sets the variable to infinite
        currentTime = time.time()

        readsLock.acquire()
        
        for readID in list(readTimeouts.keys()):
            tout, attempts = readTimeouts[readID]

            #if the read timed out,
            # handle the read again
            if tout < currentTime:
                attempts -= 1
                del readTimeouts[readID] # removes read timeout entry
                
                m = activeReads.get(readID)
                if m != None:
                    del activeReads[readID] # delete active read entry
                    readsLock.release() # releasing the lock is needed to perform the handleRead function #TODO - talvez seja melhor remover o lock acquire e release do handleRead e encapsula-la noutra funcao com isso                    
                    
                    #Only redo read operation if there are attempts left, otherwise
                    # inform the client that it was not possible
                    if attempts > 0:
                        logging.info("Retrying read(ID=" + str(readID) + "): " + str(m[0]))

                        # handle the read request again
                        newReadID = handleRead(m[0])

                        # Reduces the number of attempts
                        readsLock.acquire()
                        tout,_ = readTimeouts[newReadID]
                        readTimeouts[newReadID] = (tout, attempts - 1)
                    else:
                        reply(m[0],type="error",code=0)
                        
            #else tries to update the 'lowestTout' to set the sleep time until the next 
            elif tout < lowestTout:
                lowestTout = tout
        
        readsLock.release()

        # if the lowestTout changed sleep until that time
        if lowestTout != float("inf"):
            time.sleep(time.time() - lowestTout)
        else:
            time.sleep(idleTime / 1000)


########### Handle Init Msg ###########

def handleInit(msg):
    global node_id, node_ids, state, majority, probReadFromLeader, randomizer

    node_id = msg.body.node_id
    node_ids = msg.body.node_ids
    logging.info('node %s initialized', node_id)

    majority = len(node_ids) // 2 + 1
    probReadFromLeader = calculateProbabilityOfQueryingLeader(len(node_ids))
    randomizer = Random(node_id)

    initRaftTimeoutsThread()

    reply(msg, type='init_ok')


########### Handle Read Request ###########

#TODO - preciso adicionar ao readTimeouts
#Returns a read ID (equal or higher than 0) if the node itself is not a leader
# Else returns -1
def handleRead(msg):
    global requestsBuffer, nextReadID, activeReads
    try:
        lock.acquire()
        if state == 'Leader':
            logging.info('read %s', msg.body.key)
            if msg.body.key in stateMachine:
                value,_ = stateMachine.get(msg.body.key)
                reply(msg, type='read_ok', value=value)
            else:
                reply(msg, type='read_ok', value=None)
            return -1
        else:
            readsLock.acquire()
            readID = nextReadID
            nextReadID += 1

            #Check if the read request should be answered by
            # querying the leader or by performing a quorum read
            if leader_id != None and maybeQueryLeader():
                activeReads[readID] = (msg, "leader", leader_id)
                send(node_id, leader_id, type="read_leader", read_id=readID, key=msg.body.key)
                logging.info("Trying leader read(ID=" + str(readID) + "): " + str(msg))
            else:
                activeReads[readID] = (msg, "quorum", dict())           
                logging.info("Trying quorum read(ID=" + str(readID) + "): " + str(msg))
                #broadcast to a random subset of nodes (excluding the node itself and the leader). 
                #The number of nodes must be enough to form a majority
                broadcastToRandomSubset(type="read_quorum", read_id=readID, key=msg.body.key)

            addReadTimeout(readID) # register the timeout for this read attempt

            readsLock.release()    
            return readID
        
    finally: lock.release()

#Broadcast to random subset of nodes, 
# excluding the leader and the node itself.
def broadcastToRandomSubset(**body):
    #Random selection of subset size. The value must be at least N // 2, 
    # so that a majority can be reached when counting the node itself.
    #The maximum value is number of nodes excluding the leader and the node itself (N - 2).
    subsetSize = randomizer.randint(len(node_ids) // 2, len(node_ids) - 1)
    nrRandRemoves = len(node_ids) - 2 - subsetSize
    subset = node_ids.copy()
    if leader_id != None: 
        subset.remove(leader_id)
    else:
        nrRandRemoves += 1
    subset.remove(node_id)

    # Removes 'nrRandRemoves' nodes randomly
    for i in range(0, nrRandRemoves):
        subset.pop(randomizer.randint(0,len(subset)))

    # Broadcasts to the subset
    for n in subset:
        send(node_id, n, **body)
    
    logging.info("Quorum(readID= " + str(body["read_id"]) + "): " + str(subset))

########### Handle Write Request ###########

def handleWrite(msg):
    global requestsBuffer
    lock.acquire()

    if state == 'Leader':
        logging.info('write %s:%s', msg.body.key, msg.body.value)
        addEntryToLog(msg)
        sendAppendEntriesRPCToAll()
    else: 
        #Only Leader can answer this type of requests from clients
        requestsBuffer.append(msg)  # save request while election is still happening
    
    lock.release()

########### Handle CAS Request ###########

def handleCAS(msg):
    lock.acquire()

    if state == 'Leader':
        logging.info('cas %s:%s:%s', msg.body.key, getattr(msg.body,"from"), msg.body.to)
        addEntryToLog(msg)
        sendAppendEntriesRPCToAll()
    else: #Only Leader can answer requests from clients
        #Only Leader can answer this type of requests from clients
        requestsBuffer.append(msg)  # save request while election is still happening
    
    lock.release()

########### Auxiliar Functions ###########

def initializeLeaderVolatileState():
    global nextIndex, matchIndex
    
    #clear data structures
    nextIndex.clear()
    matchIndex.clear()

    #since log entries index start at 1, the length of the log
    # equals to the index of the last log entry.
    # To obtain the index of the next entry, we only need to add 1 
    # to the index of the last log entry
    nextI = len(log) + 1 

    for n in node_ids:
        if n != node_id:
            nextIndex[n] = nextI
            matchIndex[n] = 0


def changeStateTo(newState):
    global state, receivedVotes, leader_id

    leader_id = None
    state = newState
    resetRaftTimeout() #refresh timeout

    if newState == 'Leader':
        initializeLeaderVolatileState()
        
        #if there are messages buffered, add them to the log
        for m in requestsBuffer:
            addEntryToLog(m)
        requestsBuffer.clear()

    elif newState == 'Candidate':
        receivedVotes.clear()
        

    

def addEntryToLog(msg):
    log.append((msg,currentTerm))

#'index' starts at 1, therefore to get the entry we want, this 
#  variable must be decremented by 1 unit
def getLogEntry(index : int):
    i = index - 1
    if i < 0: return None
    else: 
        if i < len(log):
            return log[i]
        else:
            return None

#Returns list of entries that follow the one represented by the given index (including the one with the index)
#'index' starts at 1, therefore to get the entry we want, this 
#  variable must be decremented by 1 unit
def getLogNextEntries(index : int):
    i = index - 1
    if i < 0: return []
    else: return log[i:]

#tries to update commit index if all the necessary conditions are met
# returns true if the commitIndex was updated
def leaderTryUpdateCommitIndex():
    global commitIndex

    #gets all match indexes and sorts them
    matchIndexes = list(matchIndex.values())
    matchIndexes.sort()

    #starting from left we will find the biggest 
    # index, that is higher then the commitIndex of the leader
    # and that is replicated in the majority of nodes
    N = matchIndexes[-(majority - 1)]

    #If N is higher then the commitIndex, and if that entry's term 
    # equals the current term, then update the commitIndex to N
    if N > commitIndex and getLogEntry(N)[1] == currentTerm:
        commitIndex = N
        return True
    
    return False


def tryUpdateLastApplied():
    global lastApplied

    if commitIndex > lastApplied:
        #increment lastApplied in one unit and apply the command to the state machine
        lastApplied+=1
        applyToStateMachine(lastApplied)
        
        #try updating again
        tryUpdateLastApplied()

# Apply command to state machine
def applyToStateMachine(logIndex):
    if logIndex > len(log): return

    msg, term = getLogEntry(logIndex)

    if msg.body.type == 'write':
        stateMachine[msg.body.key] = (msg.body.value,(logIndex, term))
        if state == 'Leader':
            reply(msg, type='write_ok')

    elif msg.body.type == 'cas':
        
        if msg.body.key not in stateMachine:
            if state == 'Leader':
                reply(msg, type='error', code=20)
        else:
            value,_ = stateMachine.get(msg.body.key)

            if value == getattr(msg.body,"from"):
                stateMachine[msg.body.key] = (msg.body.to,(logIndex, term))
                if state == 'Leader':
                    reply(msg, type="cas_ok")
            else:
                if state == 'Leader':
                    reply(msg, type="error", code=22)
    

# Sends msg with the given body to all the other nodes
def broadcast(**body):
    for n in node_ids:
        if n != node_id:
            send(node_id, n, **body)

########### AppendEntries RPC ###########
#(Invoked by leader to replicate log entries; also used as heartbeat)

# 'dest' - follower who is the target of this RPC
def sendAppendEntriesRPC(dest, heartbeatOnly):
    #Get next entry to send to the follower
    nextI = nextIndex[dest]

    #Get the index and the term of the entry that 
    # precedes the next entry to send to that node
    prevLogIndex = nextI - 1
    prevLogTerm = 0
    if prevLogIndex > 0:
        prevEntry = getLogEntry(prevLogIndex)
        prevLogTerm = prevEntry[1]

    entries = []
    if not heartbeatOnly:
        entries = getLogNextEntries(nextI)

    send(node_id, 
         dest, 
         type="AppendEntriesRPC", 
         term=currentTerm,
         leaderId=node_id,
         leaderCommit=commitIndex,
         prevLogIndex=prevLogIndex,
         prevLogTerm=prevLogTerm,
         entries=entries
         )

def sendAppendEntriesRPCToAll():
    for n in node_ids:
        if n != node_id:
            sendAppendEntriesRPC(n, False)

def sendHeartBeatsToAll():
    for n in node_ids:
        if n != node_id:
            sendAppendEntriesRPC(n, True)

#TODO - ver se devemos manter o buffer até receber confirmação que o leader recebeu os pedidos, dos clientes, encaminhados
def handleAppendEntriesRPC(rpc):
    global currentTerm, log, commitIndex, votedFor, state, requestsBuffer, leader_id

    try: 

        lock.acquire()
        if currentTerm > rpc.body.term:
            reply(rpc, type="AppendEntriesRPCResponse", term=currentTerm, success=False, buffered_messages=[])
            return;
        elif currentTerm < rpc.body.term:
            currentTerm = rpc.body.term #update own term
            votedFor = None
        
        #ensures that the state is of follower and resets election timeout
        changeStateTo('Follower')
        leader_id = rpc.src

        if rpc.body.prevLogIndex != 0:
            #check existence of the entry with prevLogIndex sent by the leader
            # if it doesnt exist, or if the entry does not match the entry 
            # present in the leader's log, then a false response must be sent
            prevEntry = getLogEntry(rpc.body.prevLogIndex)
            if prevEntry == None or prevEntry[1] != rpc.body.prevLogTerm: 
                reply(rpc, type="AppendEntriesRPCResponse", term=currentTerm, success=False, buffered_messages=requestsBuffer)
                requestsBuffer.clear()
                return;
            
        #used to keep track of the index that the entries have
        log_i = rpc.body.prevLogIndex + 1 
        
        for i in range(0, len(rpc.body.entries)):

            leader_entry = rpc.body.entries[i]
            node_entry = getLogEntry(log_i) 

            #if the entry exists and it does not match the one sent by the leader,
            # then that entry and the ones that follow must be removed from the log
            if node_entry != None and node_entry[1] != leader_entry[1]:
                for j in range(log_i - 1, len(log)):
                    log.pop(log_i - 1)

            #appends the new entry to the log
            log.append(leader_entry)

            log_i += 1

        #updates commitIndex to the minimum
        # between the leader's commit index and
        # the highest index present in the log
        if rpc.body.leaderCommit > commitIndex:
            commitIndex = min(rpc.body.leaderCommit, log_i - 1)

        #sends positive reply
        #also informs the nextIndex to the leader to allow
        # the optimization of sending less times the same entry
        reply(rpc, type="AppendEntriesRPCResponse", term=currentTerm, success=True, nextIndex=log_i, buffered_messages=requestsBuffer)
        requestsBuffer.clear()

        #tries to apply some commands
        #only happens if there are commands 
        # that have been applied by the leader
        tryUpdateLastApplied()

    finally: lock.release()

def handleAppendEntriesRPCResponse(response):
    global currentTerm, nextIndex, matchIndex, state, votedFor, log

    try:

        lock.acquire()

        # add forwarded messages from followers to log 
        bufferedMessages = response.body.buffered_messages
        for m in bufferedMessages:
            addEntryToLog(m)  

        #ignores in case it is no longer a leader
        if state != 'Leader': return

        if response.body.success == False:
            #Check the reason why the success of the AppendEntriesRPC was false
            
            #if it was because the leader's term is old,
            # then this node must update its term and 
            # convert to follower
            if currentTerm < response.body.term:
                currentTerm = response.body.term
                votedFor = None
                changeStateTo('Follower')
                return;
            #otherwise, the follower must not have some 
            # entries that the leader assumed it had.
            # In this case, the nextIndex of this node must
            # be decreased, and another AppendEntriesRPC must
            # be sent
            else:
                nextIndex[response.src] -= 1
                sendAppendEntriesRPC(response.src, False)

        else:
            #the follower sends is nextIndex when the
            # AppendEntriesRPC is a success. This allows
            # the update the nextIndex and matchIndex
            nextIndex[response.src] = response.body.nextIndex
            matchIndex[response.src] = response.body.nextIndex - 1

            #Checks if the commitIndex can be incremented
            updated = leaderTryUpdateCommitIndex()

            #If the commitIndex was updated, then 
            # it can update the lastApplied and
            # apply one or more commands to the 
            # state machine
            if updated:
                tryUpdateLastApplied()

    finally: lock.release()


########### RequestVote RPC ###########

#Returns a tuple containing the index and term of the last log's entry, 
# in the order mentioned previously
#If no entry exists, then returns (0,0)
def getLastLogEntryIndexAndTerm():
    lastLogIndex = len(log)
    lastLogTerm = 0
    lastEntry = getLogEntry(lastLogIndex)
    if lastEntry != None:
        lastLogTerm = lastEntry[1]
    return lastLogIndex, lastLogTerm

def broadcastRequestVoteRPC():
    lastLogIndex, lastLogTerm = getLastLogEntryIndexAndTerm() 
    broadcast(type="RequestVoteRPC", 
              term=currentTerm,
              candidateId=node_id,
              lastLogIndex=lastLogIndex,
              lastLogTerm=lastLogTerm)

def handleRequestVoteRPC(rpc):
    global currentTerm, state, votedFor
    
    try:
        lock.acquire()

        if currentTerm < rpc.body.term: 
            currentTerm = rpc.body.term
            votedFor = rpc.body.candidateId
            changeStateTo('Follower')
            reply(rpc, type="RequestVoteRPCResponse", voteGranted=True, term=currentTerm)
            return
        
        elif currentTerm > rpc.body.term:
            reply(rpc, type="RequestVoteRPCResponse", voteGranted=False, term=currentTerm)
            return
        
        else: #currentTerm == rpc.body.term:
            
            if state == 'Follower':

                #In case this follower as already voted in this current term
                if votedFor != None:
                    if votedFor == rpc.body.candidateId:
                        reply(rpc, type="RequestVoteRPCResponse", voteGranted=True, term=currentTerm) 
                    else:
                        reply(rpc, type="RequestVoteRPCResponse", voteGranted=False, term=currentTerm) 
                    return

                #If it hasn't already voted in this term
                lastLogIndex, lastLogTerm = getLastLogEntryIndexAndTerm() 

                if lastLogTerm < rpc.body.lastLogTerm:
                    votedFor = rpc.body.candidateId #votes for the candidate because it more up to date
                    resetRaftTimeout() #resets timeout
                    reply(rpc, type="RequestVoteRPCResponse", voteGranted=True, term=currentTerm)

                elif lastLogTerm > rpc.body.lastLogTerm:
                    #rejects vote because it has a more up to date log than the candidate that sent the rpc
                    reply(rpc, type="RequestVoteRPCResponse", voteGranted=False, term=currentTerm)

                else: # lastLogTerm == rpc.body.lastLogTerm:
                    if lastLogIndex > rpc.body.lastLogIndex:
                        #rejects vote because it has a bigger log than the candidate that sent the rpc
                        reply(rpc, type="RequestVoteRPCResponse", voteGranted=False, term=currentTerm)
                    
                    else: #lastLogIndex <= rpc.body.lastLogIndex:
                        votedFor = rpc.body.candidateId
                        resetRaftTimeout() #resets timeout
                        reply(rpc, type="RequestVoteRPCResponse", voteGranted=True, term=currentTerm)
                

            elif state == 'Candidate': # is this response necessary? looks like wasted bandwidth
                reply(rpc, type="RequestVoteRPCResponse", voteGranted=False, term=currentTerm)
                return
            
            else: # state == 'Leader'
                sendAppendEntriesRPC(rpc.body.candidateId, False)
                return

    finally: lock.release()


def handleRequestVoteRPCResponse(response):
    global currentTerm, state, votedFor, receivedVotes

    try:

        lock.acquire()

        #checks the term first, since receiving a higher term 
        # will result in the same action, regardless of the 
        # value of 'voteGranted'
        if currentTerm < response.body.term:
            currentTerm = response.body.term
            changeStateTo('Follower')
            votedFor = None
            return

        if response.body.voteGranted == True:
            if currentTerm == response.body.term:
                receivedVotes.add(response.src)
                resetRaftTimeout() # resets timeout

                #if the size of the set 'receivedVotes' equals to the majority
                # then the node can declare itself as leader
                if len(receivedVotes) == majority:
                    changeStateTo('Leader')
                    sendHeartBeatsToAll()
                    logging.warning(str(datetime.now()) + " :Became leader")

            # elif currentTerm > response.body.term: ignores because its an outdated reply
    
    finally: lock.release()


########### Leader/Quorum Read ###########

#### Auxiliary Functions ####
def calculateProbabilityOfQueryingLeader(nodeCount):
    P = None
    if nodeCount <= 3: P = 1
    else: P = comb(nodeCount - 3, nodeCount // 2 - 1) / comb(nodeCount - 2, nodeCount // 2)
    return (P * (nodeCount - 2)) / (nodeCount + P * (nodeCount - 2)) * 100

#Returns true if the follower should query the leader
# to answer the client's read request
def maybeQueryLeader():
    return randomizer.uniform(0,100) < probReadFromLeader


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


def addReadTimeout(readID):
    readTimeouts[readID] = (time.time() + readTout / 1000, readAdditionalAttempts)

#### Message Handlers ####

def handleLeaderRead(msg):
    lock.acquire()

    if state == 'Leader':
        if msg.body.key in stateMachine:
            value,_ = stateMachine.get(msg.body.key)
            reply(msg, type='read_leader_resp', read_id=msg.body.read_id, success=True, value=value)
        else:
            reply(msg, type='read_leader_resp', read_id=msg.body.read_id, success=True, value=None)
    else:
        reply(msg, type='read_leader_resp', read_id=msg.body.read_id, success=False, value=None)

    lock.release()

#TODO - vale a pena verificar mesmo se ainda é o leader atual? Se nao, é preciso eliminar os dados desnecessarios no handleRead
def handleLeaderReadResponse(msg):
    readsLock.acquire()
    m = activeReads.get(msg.body.read_id)
    
    if m == None:
        readsLock.release()
        return

    if msg.body.success:
        reply(m[0], type="read_ok", value=msg.body.value)
        del activeReads[msg.body.read_id] # delete active read entry
        readsLock.release()
    else:
        del activeReads[msg.body.read_id] # delete active read entry
        # TODO - provavelmente é melhor ter um lock diferente para a cena dos readTimeouts
        _,attempts = readTimeouts[msg.body.read_id]
        del readTimeouts[msg.body.read_id] # removes read timeout entry
        readsLock.release()
        
        if attempts > 0:
            # handle the read request again
            newReadID = handleRead(m[0]) 
            
            # Reduces the number of attempts
            readsLock.acquire()
            newTout,_ = readTimeouts[newReadID]
            readTimeouts[newReadID] = (newTout, attempts - 1) 
            readsLock.release()
        else:
            reply(m[0],type="error",code=0)

def handleQuorumRead(msg):
    lock.acquire()
    if msg.body.key in stateMachine:
        value,(index,term) = stateMachine.get(msg.body.key)
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
    lock.release()

def handleQuorumReadResponse(msg):
    lock.acquire()
    m = activeReads.get(msg.body.read_id)
    
    if m == None:
        lock.release()
        return
    
    answers = m[2]
    answers[msg.src] = (msg.body.value, msg.body.index, msg.body.term)

    if len(answers) + 1 == majority:
        activeReads.pop(msg.body.read_id) # delete active read entry
        key = m[0].body.key # gets key from client's read request
        
        if key in stateMachine:
            value, (index, term) = stateMachine.get(key)
            answers[node_id] = (value, index, term)
        else:
            answers[node_id] = (None, 0, 0)

        reply(m[0], type="read_ok", value=getMostUpdatedValue(answers))

    lock.release()

########### Main Loop ###########

for msg in receiveAll():
    if msg.body.type == 'init':
        handleInit(msg)

    elif msg.body.type == 'read':
        handleRead(msg)

    elif msg.body.type == 'write':
        handleWrite(msg)
    
    elif msg.body.type == 'cas':
        handleCAS(msg)      
    
    elif msg.body.type == 'AppendEntriesRPC':
        handleAppendEntriesRPC(msg)

    elif msg.body.type == 'AppendEntriesRPCResponse':
        handleAppendEntriesRPCResponse(msg)

    elif msg.body.type == 'RequestVoteRPC':
        handleRequestVoteRPC(msg)

    elif msg.body.type == 'RequestVoteRPCResponse':
        handleRequestVoteRPCResponse(msg)

    elif msg.body.type == 'read_leader':
        handleLeaderRead(msg)

    elif msg.body.type == 'read_leader_resp':
        handleLeaderReadResponse(msg)

    elif msg.body.type == 'read_quorum':
        handleQuorumRead(msg)

    elif msg.body.type == 'read_quorum_resp':
        handleQuorumReadResponse(msg)

    else:
        logging.warning('unknown message type %s', msg.body.type)

runTimeoutThread = False