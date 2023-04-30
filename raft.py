#!/usr/bin/env python3

import logging
from time import sleep
from threading import Thread, Lock
from ms import receiveAll, reply, send
from random import Random
from datetime import datetime

logging.getLogger().setLevel(logging.DEBUG)


########### global variables ###########

node_id = None
node_ids = None
state = 'Follower' # can be one of the following: 'Follower', 'Candidate', 'Leader'
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

### Volatile state on all servers ###

#index of highest log entry known to be committed 
# (initialized to 0, increases monotonically)
commitIndex = 0 

#index of highest log entry applied to state machine 
# (initialized to 0, increases monotonically)
lastApplied = 0


### Volatile state on leaders ### 
#(Reinitialized after election)

#for each server, index of the next log entry to send to that server
# (initialized to leader last log index + 1)
nextIndex = dict() 

#for each server, index of highest log entry known to be replicated on server
# (initialized to 0, increases monotonically)
matchIndex = dict()
 

########### Timeouts ###########

timeoutThread = None
runTimeoutThread = True # while true keep running the timeout thread 
sendAppendEntriesRPCTout = 50 # timeout to send AppendEntries RPC to every server (in milliseconds)
lowerLimElectionTout = 150 # lower limit for the random generation of the timeout to become candidate in case no message from a leader arrives (in milliseconds)
upperLimElectionTout = 300 # upper limit for the random generation of the timeout to become candidate in case no message from a leader arrives (in milliseconds)
randomizer = None
tout = None

def getRandomElectionTimeout():
    return randomizer.randint(lowerLimElectionTout, upperLimElectionTout)

#depending on the state returns the timeout
def getTimeout():
    if state == 'Leader':
        return sendAppendEntriesRPCTout
    else:
        return getRandomElectionTimeout()

def initTimeoutThread():
    global timeoutThread, randomizer
    randomizer = Random(node_id)
    timeoutThread = Thread(target=timeoutsScheduler, args=())
    timeoutThread.start()

def timeoutsScheduler():
    global state, tout

    #This will be running in a secondary thread, and the timeouts are important, 
    # so if the node changes to another state it is important to catch that 
    # change as soon as possible.
    # For that we will execute sleeps of min(tout, lowest of all timeouts).
    # Since the lowest of all timeouts is the 50 milliseconds for a leader to send
    # a new batch of AppendEntries RPCs, the sleeps will be of a maximum of 50 ms
    
    # set the initial timeout
    lock.acquire()
    tout = getTimeout()
    lock.release()

    while runTimeoutThread:
        lock.acquire()

        if tout > sendAppendEntriesRPCTout:
            #updates 'tout' variable, decreasing it by 'sendAppendEntriesRPCTout', 
            # but it can't go bellow 0, so we use the max function 
            tout = max(0, tout - sendAppendEntriesRPCTout) 

            lock.release() # releases lock before going to sleep

            #dividing by thousand, since the argument is in seconds
            sleep(sendAppendEntriesRPCTout / 1000) 
        else:
            auxTout = tout
            tout = 0
            lock.release() # releases lock before going to sleep
            sleep(auxTout / 1000)

        lock.acquire()

        #if the timeout reached 0, then its time to 
        # call the timeout handler and reset the timeout
        if tout == 0:
            if state == 'Leader':
                handleSendAppendEntriesRPCTimeout()
            else:
                handleElectionTimeout()
            tout = getTimeout()

        lock.release()

def handleSendAppendEntriesRPCTimeout():
    sendAppendEntriesRPCToAll()

def handleElectionTimeout():
    global state, currentTerm, votedFor, tout

    # changes to Candidate state, increments term and votes for itself
    changeStateTo('Candidate')
    currentTerm += 1
    votedFor = node_id
    receivedVotes.add(node_id)

    # broadcast RequestVote RPC
    broadcastRequestVoteRPC()

########### Handle Init Msg ###########

def handleInit(msg):
    global node_id, node_ids, state, majority

    node_id = msg.body.node_id
    node_ids = msg.body.node_ids
    logging.info('node %s initialized', node_id)

    majority = int(len(node_ids) / 2) + 1

    initTimeoutThread()

    reply(msg, type='init_ok')


########### Handle Read Request ###########

def handleRead(msg):
    if state == 'Leader':
        logging.info('read %s', msg.body.key)
        lock.acquire()
        addEntryToLog(msg)
        sendAppendEntriesRPCToAll()
        lock.release()
    else: #Only Leader can answer requests from clients
        reply(msg, type='error', code=11)


########### Handle Write Request ###########

def handleWrite(msg):
    if state == 'Leader':
        logging.info('write %s:%s', msg.body.key, msg.body.value)
        lock.acquire()
        addEntryToLog(msg)
        sendAppendEntriesRPCToAll()
        lock.release()
    else: #Only Leader can answer requests from clients
        reply(msg, type='error', code=11)


########### Handle CAS Request ###########

def handleCAS(msg):
    if state == 'Leader':
        logging.info('cas %s:%s:%s', msg.body.key, getattr(msg.body,"from"), msg.body.to)
        lock.acquire()
        addEntryToLog(msg)
        sendAppendEntriesRPCToAll()
        lock.release()
    else: #Only Leader can answer requests from clients
        reply(msg, type='error', code=11)

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
    global state, tout, receivedVotes

    state = newState
    tout = getTimeout() #refresh timeout

    if newState == 'Leader':
        initializeLeaderVolatileState()
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
        applyToStateMachine(getLogEntry(lastApplied)[0])
        
        #try updating again
        tryUpdateLastApplied()

# Apply command to state machine
def applyToStateMachine(msg):
    if msg.body.type == 'read':
        value = stateMachine.get(msg.body.key)

        if state == 'Leader':
            reply(msg, type='read_ok', value=value)
    
    elif msg.body.type == 'write':
        stateMachine[msg.body.key] = msg.body.value
        if state == 'Leader':
            reply(msg, type='write_ok')

    elif msg.body.type == 'cas':
        value = stateMachine.get(msg.body.key)
        if value == None:
            if state == 'Leader':
                reply(msg, type='error', code=20)
        else:
            if value == getattr(msg.body,"from"):
                stateMachine[msg.body.key] = msg.body.to
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

def handleAppendEntriesRPC(rpc):
    global currentTerm, log, commitIndex, votedFor, state, tout

    try: 

        lock.acquire()
        if currentTerm > rpc.body.term:
            reply(rpc, type="AppendEntriesRPCResponse", term=currentTerm, success=False)
            return;
        elif currentTerm < rpc.body.term:
            currentTerm = rpc.body.term #update own term
            votedFor = None
        
        #ensures that the state is of follower and resets election timeout
        changeStateTo('Follower')

        if rpc.body.prevLogIndex != 0:
            #check existence of the entry with prevLogIndex sent by the leader
            # if it doesnt exist, or if the entry does not match the entry 
            # present in the leader's log, then a false response must be sent
            prevEntry = getLogEntry(rpc.body.prevLogIndex)
            if prevEntry == None or prevEntry[1] != rpc.body.prevLogTerm: 
                reply(rpc, type="AppendEntriesRPCResponse", term=currentTerm, success=False)
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
        reply(rpc, type="AppendEntriesRPCResponse", term=currentTerm, success=True, nextIndex=log_i)

        #tries to apply some commands
        #only happens if there are commands 
        # that have been applied by the leader
        tryUpdateLastApplied()

    finally: lock.release()

def handleAppendEntriesRPCResponse(response):
    global currentTerm, nextIndex, matchIndex, state, votedFor, tout

    try:

        lock.acquire()

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

#TODO - verificar a seccao 5.4 do paper para porque se pode enviar o último index do log (sao mudancas que podem nao ter sido aplicadas à state machine)
def broadcastRequestVoteRPC():
    lastLogIndex, lastLogTerm = getLastLogEntryIndexAndTerm() 
    broadcast(type="RequestVoteRPC", 
              term=currentTerm,
              candidateId=node_id,
              lastLogIndex=lastLogIndex,
              lastLogTerm=lastLogTerm)

def handleRequestVoteRPC(rpc):
    global currentTerm, state, votedFor, tout
    
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
                    tout = getTimeout() #resets timeout
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
                        tout = getTimeout() #resets timeout
                        reply(rpc, type="RequestVoteRPCResponse", voteGranted=True, term=currentTerm)
                

            elif state == 'Candidate': # is this response necessary? looks like wasted bandwidth
                reply(rpc, type="RequestVoteRPCResponse", voteGranted=False, term=currentTerm)
                return
            
            else: # state == 'Leader'
                sendAppendEntriesRPC(rpc.body.candidateId, False)
                return

    finally: lock.release()


def handleRequestVoteRPCResponse(response):
    global currentTerm, state, votedFor, tout, receivedVotes

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
                tout = getTimeout() # resets timeout

                #if the size of the set 'receivedVotes' equals to the majority
                # then the node can declare itself as leader
                if len(receivedVotes) == majority:
                    changeStateTo('Leader')
                    sendHeartBeatsToAll()
                    logging.warning(str(datetime.now()) + " :Became leader")

            # elif currentTerm > response.body.term: ignores because its an outdated reply
    
    finally: lock.release()



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

    else:
        logging.warning('unknown message type %s', msg.body.type)

runTimeoutThread = False