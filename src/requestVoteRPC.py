import logging, time
from auxiliarFunctions import broadcast
from ms import reply
from state import State
from appendEntriesRPC import sendAppendEntriesRPC, sendHeartBeatsToAll


########### RequestVote RPC ###########

def broadcastRequestVoteRPC(stateClass : State):
    lastLogIndex, lastLogTerm = stateClass.getLastLogEntryIndexAndTerm() 
    broadcast(stateClass,
              type="RequestVoteRPC", 
              term=stateClass.getCurrentTerm(),
              candidateId=stateClass.getNodeId(),
              lastLogIndex=lastLogIndex,
              lastLogTerm=lastLogTerm)

def handleRequestVoteRPC(stateClass : State, rpc):
    try:
        stateClass.lock.acquire()
        #logging.warning("CURRENT TERM: " +str(stateClass.getCurrentTerm()))
        #logging.warning("RPC TERM: " + str(rpc.body.term))
        
        #If the current term is less than the one from the RPC
        # then the node updates its term to match the one from
        # the RPC and converts to a follower.
        #In the next if statements, the node will check if
        # the log of the node that sent the RPC is more updated,
        # and if it is, the vote will be granted.
        if stateClass.getCurrentTerm() < rpc.body.term: 
            stateClass.setCurrentTerm(rpc.body.term)
            stateClass.setVotedFor(None)
            stateClass.changeStateTo('Follower')
        
        
        if stateClass.getCurrentTerm() == rpc.body.term:
            
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
                lastLogIndex, lastLogTerm = stateClass.getLastLogEntryIndexAndTerm() 

                if lastLogTerm < rpc.body.lastLogTerm:
                    stateClass.setVotedFor(rpc.body.candidateId)
                    #votedFor = rpc.body.candidateId #votes for the candidate because it more up to date
                    stateClass.resetTimeout() #resets timeout
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
                        stateClass.resetTimeout() #resets timeout
                        reply(rpc, type="RequestVoteRPCResponse", voteGranted=True, term=stateClass.getCurrentTerm())
                

            elif stateClass.getState() == 'Candidate': # is this response necessary? looks like wasted bandwidth
                reply(rpc, type="RequestVoteRPCResponse", voteGranted=False, term=stateClass.getCurrentTerm())
                return
            
            else: # state == 'Leader'
                sendAppendEntriesRPC(stateClass, rpc.body.candidateId, False)
                return
        else: 
            #if stateClass.getCurrentTerm() > rpc.body.term
            # then the node that sent the RPC is not updated
            # since it has an outdated term
            reply(rpc, type="RequestVoteRPCResponse", voteGranted=False, term=stateClass.getCurrentTerm())
            return

    finally: stateClass.lock.release()


def handleRequestVoteRPCResponse(stateClass : State, response):
    try:

        stateClass.lock.acquire()

        #checks the term first, since receiving a higher term 
        # will result in the same action, regardless of the 
        # value of 'voteGranted'
        
        if stateClass.getCurrentTerm() < response.body.term:
            stateClass.setCurrentTerm(response.body.term)
            stateClass.changeStateTo('Follower')
            stateClass.setVotedFor(None)
            return

        if stateClass.getState() == 'Candidate' and response.body.voteGranted == True and stateClass.getCurrentTerm() == response.body.term:
            #logging.debug("vote received from : " + str(response.src))
            stateClass.receiveVote(response.src)
            stateClass.resetTimeout() # resets timeout

            #if the size of the set 'receivedVotes' equals to the majority
            # then the node can declare itself as leader
            if stateClass.numberOfVotes() == stateClass.getMajority():
                stateClass.changeStateTo('Leader')
                sendHeartBeatsToAll(stateClass)

                #Add client requests, present in the requests buffer, to the log
                for m in stateClass.getRequestsBuffer():
                    stateClass.addEntryToLog(m)
                        
                #Clear the requests buffer
                stateClass.clearRequestsBuffer()
                
                logging.warning(str(time.time()) + " :Became leader")

        #Else ignores because it is either outdated, or the response is negative, or the node is not in candidate mode
    finally: stateClass.lock.release()