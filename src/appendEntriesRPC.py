from ms import send, reply
from state import State
import logging

########### AppendEntries RPC ###########
#(Invoked by leader to replicate log entries; also used as heartbeat)

# 'dest' - follower who is the target of this RPC
def sendAppendEntriesRPC(stateClass : State, dest, heartbeatOnly):
    #Get next entry to send to the follower
    #nextI = nextIndex[dest]
    nextI = stateClass.getNextIndex(dest)

    #Get the index and the term of the entry that 
    # precedes the next entry to send to that node
    prevLogIndex = nextI - 1
    prevLogTerm = 0
    if prevLogIndex > 0:
        prevEntry = stateClass.getLogEntry(prevLogIndex)
        prevLogTerm = prevEntry[1] 

    entries = []
    if not heartbeatOnly:
        entries = stateClass.getLogNextEntries(nextI) 
    
    send(stateClass.getNodeId(), 
         dest, 
         type="AppendEntriesRPC", 
         term=stateClass.getCurrentTerm(),
         leaderId=stateClass.getNodeId(),
         leaderCommit=stateClass.getCommitIndex(),
         prevLogIndex=prevLogIndex,
         prevLogTerm=prevLogTerm,
         entries=entries
         )

def sendAppendEntriesRPCToAll(stateClass : State):
    for n in stateClass.getNodes():
        if n != stateClass.getNodeId():
            sendAppendEntriesRPC(stateClass, n, False)

def sendHeartBeatsToAll(stateClass : State):
    for n in stateClass.getNodes():
        if n != stateClass.getNodeId():
            sendAppendEntriesRPC(stateClass, n, True)

def handleAppendEntriesRPC(stateClass : State, rpc):
    global currentTerm, log, commitIndex, votedFor, state, requestsBuffer, leader_id

    try: 

        stateClass.lock.acquire()
        if stateClass.getCurrentTerm() > rpc.body.term:
            reply(rpc, type="AppendEntriesRPCResponse", term=stateClass.getCurrentTerm(), success=False, buffered_messages=[])
            return;
        elif stateClass.getCurrentTerm() < rpc.body.term:
            stateClass.setCurrentTerm(rpc.body.term) #update own term
            stateClass.setVotedFor(None)
        
        #ensures that the state is of follower and resets election timeout
        stateClass.changeStateTo('Follower')
        stateClass.setLeader(rpc.src)

        if rpc.body.prevLogIndex != 0:
            #check existence of the entry with prevLogIndex sent by the leader
            # if it doesnt exist, or if the entry does not match the entry 
            # present in the leader's log, then a false response must be sent
            prevEntry = stateClass.getLogEntry(rpc.body.prevLogIndex)
            if prevEntry == None or prevEntry[1] != rpc.body.prevLogTerm: 
                #The 'success' is False, but it doesn't mean that the node should stop following the leader
                # so the messages in the requests buffer can be sent to the leader 
                reply(rpc, type="AppendEntriesRPCResponse", term=stateClass.getCurrentTerm(), success=False, buffered_messages=stateClass.getRequestsBuffer())
                stateClass.clearRequestsBuffer()
                return;
            
        #used to keep track of the index that the entries have
        log_i = rpc.body.prevLogIndex + 1 
        
        for i in range(0, len(rpc.body.entries)):
            leader_entry = rpc.body.entries[i]
            node_entry = stateClass.getLogEntry(log_i) 

            #if the entry exists and it does not match the one sent by the leader,
            # then that entry and the ones that follow must be removed from the log
            if node_entry != None and node_entry[1] != leader_entry[1]:
                for j in range(log_i, stateClass.getLogSize() + 1):
                    #deleting the same position multiple times, because deletes 
                    # left shift elements from the right
                    stateClass.removeLogEntry(log_i)

            #appends the new entry to the log
            stateClass.logInsertEntryAt(leader_entry, log_i)

            log_i += 1

        #updates commitIndex to the minimum
        # between the leader's commit index and
        # the highest index present in the log
        if rpc.body.leaderCommit > stateClass.getCommitIndex():
            newCommitIndex = min(rpc.body.leaderCommit, stateClass.getLogSize())
            stateClass.setCommitIndex(newCommitIndex)

        #sends positive reply
        #also informs the nextIndex to the leader to allow
        # the optimization of sending less times the same entry
        requestsBuffer = stateClass.getRequestsBuffer()
        reply(rpc, type="AppendEntriesRPCResponse", term=stateClass.getCurrentTerm(), success=True, nextIndex=log_i, buffered_messages=requestsBuffer)
        stateClass.clearRequestsBuffer()

        #tries to apply some commands
        #only happens if there are commands 
        # that have been applied by the leader
        stateClass.tryUpdateLastApplied()

    finally: stateClass.lock.release()

def handleAppendEntriesRPCResponse(stateClass : State, response):
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
                sendAppendEntriesRPC(stateClass, response.src, False)

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