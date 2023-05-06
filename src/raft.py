#!/usr/bin/env python3

import logging
import time
from threading import Thread, Lock
from ms import receiveAll, reply, send
from random import Random
from datetime import datetime
from math import comb
import state


logging.getLogger().setLevel(logging.DEBUG)

########### STATE CLASSES ###########

stateClass = state.State()



########### Global Variables ###########

#Dictionary to map an ID to a read request,
# and in case of a quorum read, store the answers
# of the contacted nodes-
#The mapping is deleted after the answer is sent to the client.
#Example of values:
# 1. (msg, "quorum", {node1: (value1, index1, term1), node2: (value2, index2, term2)})
# 2. (msg, "leader", leaderID)
activeReads = dict()

readsLock = Lock() # exclusive lock for read operations

########### Raft Timeouts ###########

raftTimeoutsThread = None
runTimeoutThread = True # while true keep running the timeout thread 



def getRandomElectionTimeout():
    randomizer = stateClass.getRandomizer()
    return randomizer.randint(stateClass.getLowerLimitElectionTout(), stateClass.getUpperLimitElectionTout())


#Depending on the state returns the raft related timeout
def getRaftTimeout():
    if stateClass.getState() == 'Leader':
        return stateClass.getSendAppendEntriesRPCTout()
    else:
        return getRandomElectionTimeout()

def resetRaftTimeout():
    stateClass.setRaftTout(getRaftTimeout())

def initRaftTimeoutsThread():
    global raftTimeoutsThread
    raftTimeoutsThread = Thread(target=raftTimeoutsScheduler, args=())
    raftTimeoutsThread.start()

def raftTimeoutsScheduler():
    global stateClass, raftTout

    #This will be running in a secondary thread, and the timeouts are important, 
    # so if the node changes to another state it is important to catch that 
    # change as soon as possible.
    # For that we will execute sleeps of min(raftTout, lowest of all timeouts).
    # Since the lowest of all timeouts is the 50 milliseconds for a leader to send
    # a new batch of AppendEntries RPCs, the sleeps will be of a maximum of 50 ms
    
    # set the initial timeout
    stateClass.lock.acquire()
    resetRaftTimeout()
    stateClass.lock.release()

    while runTimeoutThread:
        stateClass.lock.acquire()

        if stateClass.getRaftout() > stateClass.getSendAppendEntriesRPCTout():
            #updates 'raftTout' variable, decreasing it by 'sendAppendEntriesRPCTout', 
            # but it can't go bellow 0, so we use the max function 
            #TODO -> update raft timout 
            stateClass.updateRaftTout()
            #raftTout = max(0, raftTout - sendAppendEntriesRPCTout) 

            stateClass.lock.release() # releases lock before going to sleep

            #dividing by thousand, since the argument is in seconds
            time.sleep(stateClass.getSendAppendEntriesRPCTout() / 1000) 
        else:

            auxTout = stateClass.getRaftout() #TODO -> get rafttout
            stateClass.setRaftTout(0)
            stateClass.lock.release() # releases lock before going to sleep
            time.sleep(auxTout / 1000)

        stateClass.lock.acquire()

        #if the timeout reached 0, then its time to 
        # call the timeout handler and reset the timeout
        if stateClass.getRaftout() == 0: #TODO -> get raftout 
            if stateClass.getState() == 'Leader': #TODO -> get state 
                handleSendAppendEntriesRPCTimeout() 
            else:
                logging.warning("  ELECTION TIMEOUT")
                handleElectionTimeout()
            resetRaftTimeout()

        stateClass.lock.release()

def handleSendAppendEntriesRPCTimeout():
    sendAppendEntriesRPCToAll()

def handleElectionTimeout():
    global currentTerm, votedFor

    node_id = stateClass.getNodeId()
    # changes to Candidate state, increments term and votes for itself
    stateClass.changeStateTo('Candidate')
    
    #TODO -> use incrementTerm of state class function
    stateClass.incrementTerm()

    stateClass.setVotedFor(node_id)
    
    #TODO -> add to votes
    stateClass.receiveVote(node_id)

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
    global node_id, node_ids, majority, probReadFromLeader, randomizer

    stateClass.setNodeId(msg.body.node_id)
    
    stateClass.setNodeIds(msg.body.node_ids)

    logging.info('node %s initialized', stateClass.getNodeId())
    stateClass.initMajority()

   
    stateClass.initProbReadFromLeader()
    
    stateClass.initRandomizer()

    initRaftTimeoutsThread()

    reply(msg, type='init_ok')


########### Handle Read Request ###########

#TODO - preciso adicionar ao readTimeouts
#Returns a read ID (equal or higher than 0) if the node itself is not a leader
# Else returns -1
def handleRead(msg):
    global requestsBuffer, nextReadID, activeReads
    try:
        stateClass.lock.acquire()
        if stateClass.getState() == 'Leader': 
            logging.info('read %s', msg.body.key)
            if stateClass.keyInStateMachine(msg.body.key): 
                value,_ = stateClass.getValueFromStateMachine(msg.body.key)
                reply(msg, type='read_ok', value=value) 
            else:
                reply(msg, type='read_ok', value=None)
            return -1
        else:
            readsLock.acquire() 
            readID = stateClass.getNextReadID() 
            stateClass.incrementNextReadID()

            #Check if the read request should be answered by
            # querying the leader or by performing a quorum read
            leader_id = stateClass.getLeaderId()
            if leader_id != None and maybeQueryLeader():
                activeReads[readID] = (msg, "leader", leader_id)
                send(stateClass.getNodeId(), leader_id, type="read_leader", read_id=readID, key=msg.body.key)
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
        
    finally: stateClass.lock.release()

#Broadcast to random subset of nodes, 
# excluding the leader and the node itself.
def broadcastToRandomSubset(**body):
    #Random selection of subset size. The value must be at least N // 2, 
    # so that a majority can be reached when counting the node itself.
    #The maximum value is number of nodes excluding the leader and the node itself (N - 2).
    randomizer = stateClass.getRandomizer()
    num_nodes = stateClass.getNumNodes()
    subsetSize = randomizer.randint(num_nodes // 2, num_nodes - 1) 
    nrRandRemoves = num_nodes - 2 - subsetSize 
    subset = stateClass.getCopyNodes()
    leader_id = stateClass.getLeaderId()
    node_id = stateClass.getNodeId()
    if leader_id != None: 
        subset.remove(leader_id) 
    else:
        nrRandRemoves += 1
    subset.remove(node_id)

    # Removes 'nrRandRemoves' nodes randomly
    for i in range(0, nrRandRemoves):
        subset.pop(randomizer.randint(0,len(subset)-1))

    # Broadcasts to the subset
    for n in subset:
        send(node_id, n, **body)
    
    logging.info("Quorum(readID= " + str(body["read_id"]) + "): " + str(subset))

########### Handle Write Request ###########

def handleWrite(msg):
    global requestsBuffer
    stateClass.lock.acquire()

    if stateClass.getState() == 'Leader': 
        logging.info('write %s:%s', msg.body.key, msg.body.value)
        stateClass.addEntryToLog(msg) 
        sendAppendEntriesRPCToAll()
    else: 
        #Only Leader can answer this type of requests from clients
        stateClass.appendRequestEntry(msg)
    
    stateClass.lock.release()

########### Handle CAS Request ###########

def handleCAS(msg):
    stateClass.lock.acquire()

    #TODO -> get from state class function
    if stateClass.getState() == 'Leader':  
        logging.info('cas %s:%s:%s', msg.body.key, getattr(msg.body,"from"), msg.body.to)
        stateClass.addEntryToLog(msg)
        sendAppendEntriesRPCToAll() 
    else: #Only Leader can answer requests from clients
        #Only Leader can answer this type of requests from clients
        stateClass.appendRequestEntry(msg)
    
    stateClass.lock.release()

########### Auxiliar Functions ###########

# Sends msg with the given body to all the other nodes
def broadcast(**body):
    logging.warning(body)
    for n in stateClass.getNodes():
        node_id = stateClass.getNodeId()
        if n != node_id:
            send(node_id, n, **body)

########### AppendEntries RPC ###########
#(Invoked by leader to replicate log entries; also used as heartbeat)

# 'dest' - follower who is the target of this RPC
def sendAppendEntriesRPC(dest, heartbeatOnly):
    #Get next entry to send to the follower
    #nextI = nextIndex[dest]
    nextI = stateClass.getNextIndex(dest)

    #Get the index and the term of the entry that 
    # precedes the next entry to send to that node
    prevLogIndex = nextI - 1
    prevLogTerm = 0
    if prevLogIndex > 0:
        prevEntry = stateClass.getLogEntry(prevLogIndex) #TODO -> use state class function 
        prevLogTerm = prevEntry[1] 

    entries = []
    if not heartbeatOnly:
        entries = stateClass.getLogNextEntries(nextI) 

    send(stateClass.getNodeId(), 
         dest, 
         type="AppendEntriesRPC", 
         term=stateClass.getCurrentTerm(),
         leaderId=stateClass.getLeaderId(),
         leaderCommit=stateClass.getCommitIndex(),
         prevLogIndex=prevLogIndex,
         prevLogTerm=prevLogTerm,
         entries=entries
         )

def sendAppendEntriesRPCToAll():
    for n in stateClass.getNodes():
        if n != stateClass.getNodeId():
            sendAppendEntriesRPC(n, False)

def sendHeartBeatsToAll():
    for n in stateClass.getNodes():
        if n != stateClass.getNodeId():
            sendAppendEntriesRPC(n, True)

#TODO - ver se devemos manter o buffer até receber confirmação que o leader recebeu os pedidos, dos clientes, encaminhados
def handleAppendEntriesRPC(rpc):
    global currentTerm, log, commitIndex, votedFor, state, requestsBuffer, leader_id

    try: 

        stateClass.lock.acquire()
        if stateClass.getCurrentTerm() > rpc.body.term:
            reply(rpc, type="AppendEntriesRPCResponse", term=stateClass.getCurrentTerm(), success=False, buffered_messages=[])
            return;
        elif stateClass.getCurrentTerm() < rpc.body.term:
            stateClass.setCurrentTerm(rpc.body.term)
            #currentTerm = rpc.body.term #update own term
            #votedFor = None
            stateClass.setVotedFor(None)
        
        #ensures that the state is of follower and resets election timeout
        #changeStateTo('Follower')
        stateClass.changeStateTo('Follower')
        stateClass.setLeader(rpc.src)
        #leader_id = rpc.src

        if rpc.body.prevLogIndex != 0:
            #check existence of the entry with prevLogIndex sent by the leader
            # if it doesnt exist, or if the entry does not match the entry 
            # present in the leader's log, then a false response must be sent
            prevEntry = stateClass.getLogEntry(rpc.body.prevLogIndex)
            if prevEntry == None or prevEntry[1] != rpc.body.prevLogTerm: 
                reply(rpc, type="AppendEntriesRPCResponse", term=stateClass.getCurrentTerm(), success=False, buffered_messages=stateClass.getRequestsBuffer())
                return;
            
        #used to keep track of the index that the entries have
        log_i = rpc.body.prevLogIndex + 1 
        
        for i in range(0, len(rpc.body.entries)):

            leader_entry = rpc.body.entries[i]
            node_entry = stateClass.getLogEntry(log_i) 

            #if the entry exists and it does not match the one sent by the leader,
            # then that entry and the ones that follow must be removed from the log
            if node_entry != None and node_entry[1] != leader_entry[1]:
                for j in range(log_i - 1, stateClass.getLogSize()):
                    stateClass.removeLogEntry(log_i - 1 )

            #appends the new entry to the log
            stateClass.logAppendEntry(leader_entry)
            #log.append(leader_entry)

            log_i += 1

        #updates commitIndex to the minimum
        # between the leader's commit index and
        # the highest index present in the log
        if rpc.body.leaderCommit > stateClass.getCommitIndex():
            newCommitIndex = min(rpc.body.leaderCommit, log_i - 1)
            stateClass.setCommitIndex(newCommitIndex)

        #sends positive reply
        #also informs the nextIndex to the leader to allow
        # the optimization of sending less times the same entry
        requestsBuffer = stateClass.getRequestsBuffer()
        reply(rpc, type="AppendEntriesRPCResponse", term=stateClass.getCurrentTerm(), success=True, nextIndex=log_i, buffered_messages=list(requestsBuffer))
        #requestsBuffer.clear()

        #tries to apply some commands
        #only happens if there are commands 
        # that have been applied by the leader
        stateClass.tryUpdateLastApplied()

    finally: stateClass.lock.release()

def handleAppendEntriesRPCResponse(response):
    global currentTerm, nextIndex, matchIndex, state, votedFor, log

    try:

        stateClass.lock.acquire()

        # add forwarded messages from followers to log 
        bufferedMessages = response.body.buffered_messages
        for m in bufferedMessages:
            stateClass.addEntryToLog(m)  

        #ignores in case it is no longer a leader
        if stateClass.getState() != 'Leader': return

        if response.body.success == False:
            #Check the reason why the success of the AppendEntriesRPC was false
            
            #if it was because the leader's term is old,
            # then this node must update its term and 
            # convert to follower
            if stateClass.getCurrentTerm() < response.body.term:
                stateClass.setCurrentTerm(response.body.term)
                #currentTerm = response.body.term
                stateClass.setVotedFor(None)
                #votedFor = None
                stateClass.changeStateTo('Follower')
                return;
            #otherwise, the follower must not have some 
            # entries that the leader assumed it had.
            # In this case, the nextIndex of this node must
            # be decreased, and another AppendEntriesRPC must
            # be sent
            else:
                stateClass.decrementNextIndex(response.src)
                #nextIndex[response.src] -= 1
                sendAppendEntriesRPC(response.src, False)

        else:
            #the follower sends is nextIndex when the
            # AppendEntriesRPC is a success. This allows
            # the update the nextIndex and matchIndex
            stateClass.setNextIndex(response.src, response.body.nextIndex)
            #nextIndex[response.src] = response.body.nextIndex
            stateClass.setMatchIndex(response.src, response.body.nextIndex - 1 )
            #matchIndex[response.src] = response.body.nextIndex - 1

            #Checks if the commitIndex can be incremented
            updated = stateClass.leaderTryUpdateCommitIndex()

            #If the commitIndex was updated, then 
            # it can update the lastApplied and
            # apply one or more commands to the 
            # state machine
            if updated:
                stateClass.tryUpdateLastApplied()

    finally: stateClass.lock.release()


########### RequestVote RPC ###########

#Returns a tuple containing the index and term of the last log's entry, 
# in the order mentioned previously
#If no entry exists, then returns (0,0)
def getLastLogEntryIndexAndTerm():
    lastLogIndex = stateClass.getNumNodes()
    lastLogTerm = 0
    lastEntry = stateClass.getLogEntry(lastLogIndex)
    if lastEntry != None:
        lastLogTerm = lastEntry[1]
    return lastLogIndex, lastLogTerm

def broadcastRequestVoteRPC():
    lastLogIndex, lastLogTerm = getLastLogEntryIndexAndTerm() 
    broadcast(type="RequestVoteRPC", 
              term=stateClass.getCurrentTerm(),
              candidateId=stateClass.getNodeId(),
              lastLogIndex=lastLogIndex,
              lastLogTerm=lastLogTerm)

def handleRequestVoteRPC(rpc):
    global currentTerm, stateClass, votedFor
    
    try:
        stateClass.lock.acquire()
        #logging.warning("CURRENT TERM: " +str(stateClass.getCurrentTerm()))
        #logging.warning("RPC TERM: " + str(rpc.body.term))
        if stateClass.getCurrentTerm() < rpc.body.term: 

            stateClass.setCurrentTerm(rpc.body.term)
            #currentTerm = rpc.body.term
            stateClass.setVotedFor(rpc.body.candidateId)
            stateClass.changeStateTo('Follower')
            reply(rpc, type="RequestVoteRPCResponse", voteGranted=True, term=stateClass.getCurrentTerm())
            return
        
        elif stateClass.getCurrentTerm() > rpc.body.term:
            reply(rpc, type="RequestVoteRPCResponse", voteGranted=False, term=stateClass.getCurrentTerm())
            return
        
        else: #currentTerm == rpc.body.term:
            
            if stateClass.getState() == 'Follower':

                #In case this follower as already voted in this current term
                votedFor = stateClass.getVotedFor()
                if  votedFor != None:
                    if votedFor == rpc.body.candidateId:
                        reply(rpc, type="RequestVoteRPCResponse", voteGranted=True, term=stateClass.getCurrentTerm()) 
                    else:
                        reply(rpc, type="RequestVoteRPCResponse", voteGranted=False, term=stateClass.getCurrentTerm()) 
                    return

                #If it hasn't already voted in this term
                lastLogIndex, lastLogTerm = getLastLogEntryIndexAndTerm() 

                if lastLogTerm < rpc.body.lastLogTerm:
                    stateClass.setVotedFor(rpc.body.candidateId)
                    #votedFor = rpc.body.candidateId #votes for the candidate because it more up to date
                    resetRaftTimeout() #resets timeout
                    reply(rpc, type="RequestVoteRPCResponse", voteGranted=True, term=stateClass.getCurrentTerm())

                elif lastLogTerm > rpc.body.lastLogTerm:
                    #rejects vote because it has a more up to date log than the candidate that sent the rpc
                    reply(rpc, type="RequestVoteRPCResponse", voteGranted=False, term=stateClass.getCurrentTerm())

                else: # lastLogTerm == rpc.body.lastLogTerm:
                    if lastLogIndex > rpc.body.lastLogIndex:
                        #rejects vote because it has a bigger log than the candidate that sent the rpc
                        reply(rpc, type="RequestVoteRPCResponse", voteGranted=False, term=stateClass.getCurrentTerm())
                    
                    else: #lastLogIndex <= rpc.body.lastLogIndex:
                        stateClass.setVotedFor(rpc.body.candidateId)
                        #votedFor = rpc.body.candidateId
                        resetRaftTimeout() #resets timeout
                        reply(rpc, type="RequestVoteRPCResponse", voteGranted=True, term=stateClass.getCurrentTerm())
                

            elif stateClass.getState() == 'Candidate': # is this response necessary? looks like wasted bandwidth
                reply(rpc, type="RequestVoteRPCResponse", voteGranted=False, term=stateClass.getCurrentTerm())
                return
            
            else: # state == 'Leader'
                sendAppendEntriesRPC(rpc.body.candidateId, False)
                return

    finally: stateClass.lock.release()


def handleRequestVoteRPCResponse(response):
    global currentTerm, stateClass, votedFor, receivedVotes

    try:

        stateClass.lock.acquire()

        #checks the term first, since receiving a higher term 
        # will result in the same action, regardless of the 
        # value of 'voteGranted'
        
        if stateClass.getCurrentTerm() < response.body.term:
            stateClass.setCurrentTerm(response.body.term)
            #currentTerm = response.body.term
            stateClass.changeStateTo('Follower')
            stateClass.setVotedFor(None)
            #votedFor = None
            return

        if response.body.voteGranted == True:
            if stateClass.getCurrentTerm() == response.body.term:
                logging.debug("vote received from : " + str(response.src))
                stateClass.receiveVote(response.src)
                
                #receivedVotes.add(response.src)
                resetRaftTimeout() # resets timeout

                #if the size of the set 'receivedVotes' equals to the majority
                # then the node can declare itself as leader
                if stateClass.numberOfVotes() == stateClass.getMajority():
                    stateClass.changeStateTo('Leader')
                    sendHeartBeatsToAll()
                    logging.warning(str(datetime.now()) + " :Became leader")

            # elif currentTerm > response.body.term: ignores because its an outdated reply
    
    finally: stateClass.lock.release()


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
    randomizer = stateClass.getRandomizer()
    return randomizer.uniform(0,100) < stateClass.getProbReadFromLeader()


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
    stateClass.lock.acquire()

    if stateClass.getState() == 'Leader':
        if stateClass.keyInStateMachine(msg.body.key):
            value,_ = stateClass.getValueFromStateMachine(msg.body.key)
            #value,_ = stateMachine.get(msg.body.key)
            reply(msg, type='read_leader_resp', read_id=msg.body.read_id, success=True, value=value)
        else:
            reply(msg, type='read_leader_resp', read_id=msg.body.read_id, success=True, value=None)
    else:
        reply(msg, type='read_leader_resp', read_id=msg.body.read_id, success=False, value=None)

    stateClass.lock.release()

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
    stateClass.lock.acquire()
    if stateClass.keyInStateMachine(msg.body.key):
        value,(index,term) = stateClass.getValueFromStateMachine(msg.body.key)
        #value,(index,term) = stateMachine.get(msg.body.key)
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
    stateClass.lock.release()

def handleQuorumReadResponse(msg):
    stateClass.lock.acquire()
    m = activeReads.get(msg.body.read_id)
    node_id = stateClass.getNodeId()
    if m == None:
        stateClass.lock.release()
        return
    
    answers = m[2]
    answers[msg.src] = (msg.body.value, msg.body.index, msg.body.term)

    if len(answers) + 1 == stateClass.getMajority():
        activeReads.pop(msg.body.read_id) # delete active read entry
        key = m[0].body.key # gets key from client's read request
        
        if stateClass.keyInStateMachine(key):
            value, (index,term) = stateClass.getValueFromStateMachine(key)
            #value, (index, term) = stateMachine.get(key)
            answers[node_id] = (value, index, term)
        else:
            answers[node_id] = (None, 0, 0)

        reply(m[0], type="read_ok", value=getMostUpdatedValue(answers))

    stateClass.lock.release()

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