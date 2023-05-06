from threading import Thread, Lock
from ms import reply
from math import comb
from random import Random
import logging

#TODO - check what can be initialized from the start
class State:
    def __init__(self, node_id, node_ids):

        self.__node_id = node_id
        self.__node_ids = node_ids
        self.state = 'Follower' #   can be onde of the following: 'Follower', 'Candidate', 'Leader'
        self.leader_id = None 
        self.majority = len(self.__node_ids) // 2 + 1  # quantity of nodes necessary to represent the majority
        self.stateMachine = dict() # state machine k -> ( value, term, index )

        ## Persistent state on all servers ##
        #( Updated on stable storage before responding to RPCs)

        self.currentTerm = 0
        self.votedFor = None
        self.receivedVotes = set()

        #log entries; 
        #Each entry contains the msg sent by the client (which contains the command for state machine), 
        # and term when entry was received by leader;
        #First index is 1
        self.log = [] 
        self.requestsBuffer = [] # when on 'Follower' state, buffer received requests until next contact with a leader

        ### Volatile state on all servers ###
                
        #index of highest log entry known to be committed 
        # (initialized to 0, increases monotonically)
        self.commitIndex = 0 

        #index of highest log entry applied to state machine 
        # (initialized to 0, increases monotonically)
        self.lastApplied = 0

        ### Volatile state on leaders ### 
        #(Reinitialized after election)

        #for each server, index of the next log entry to send to that server
        # (initialized to leader last log index + 1)
        self.nextIndex = dict() 

        #for each server, index of highest log entry known to be replicated on server
        # (initialized to 0, increases monotonically)
        self.matchIndex = dict()

        self.randomizer = Random(node_id)
        self.lock = Lock()

        ########### Raft Timeouts ###########
        
        self.raftTimeoutsThread = None
        self.runTimeoutThread = True # while true keep running the timeout thread 
        self.sendAppendEntriesRPCTout = 50 # timeout to send AppendEntries RPC to every server (in milliseconds)
        self.lowerLimElectionTout = 300 # lower limit for the random generation of the timeout to become candidate in case no message from a leader arrives (in milliseconds)
        self.upperLimElectionTout = 500 # upper limit for the random generation of the timeout to become candidate in case no message from a leader arrives (in milliseconds)
        self.raftTout = self.getTimeout() #raft related timeout

   ########################## AUX FUNCTIONS ##########################
    
    def numberOfVotes(self):
        return len(self.receivedVotes)

    def logAppendEntry(self, entry):
        self.log.append(entry)
    
    def logInsertEntryAt(self, entry, index):
        i = index - 1
        self.log.insert(i, entry)

    def removeLogEntry(self, index):
        i = index - 1
        if i < len(self.log):
            logging.warning("Removing entry(index:" + str(index) +"): " + self.log[i])
            del self.log[i]

    def updateRaftTout(self):
        self.raftTout = max(0, self.raftTout - self.sendAppendEntriesRPCTout)

    def appendRequestEntry(self, msg):
        self.requestsBuffer.append(msg)
    
    def keyInStateMachine(self, key):
        return key in self.stateMachine
    
    def incrementTerm(self):
        self.currentTerm += 1
    

    def receiveVote(self, node_id):
        self.receivedVotes.add(node_id)

    def initializeLeaderVolatileState(self):
        #clear data structures
        self.nextIndex.clear()
        self.matchIndex.clear()

        #since log entries index start at 1, the length of the log
        # equals to the index of the last log entry.
        # To obtain the index of the next entry, we only need to add 1 
        # to the index of the last log entry
        nextI = len(self.log) + 1

        for n in self.__node_ids:
            if n != self.__node_id:
                self.nextIndex[n] = nextI
                self.matchIndex[n] = 0


    def changeStateTo(self,newState):
        self.leader_id = None
        self.state  = newState
        self.resetTimeout() #refresh timeout

        if newState == 'Leader':
            self.initializeLeaderVolatileState()
        elif newState == 'Candidate':
            self.receivedVotes.clear()
        

    def addEntryToLog(self, msg):
        self.log.append((msg,self.currentTerm))

    #tries to update commit index if all the necessary conditions are met
    # returns true if the commitIndex was updated
    def leaderTryUpdateCommitIndex(self):
        #gets all match indexes and sorts them
        matchIndexes = list(self.matchIndex.values())
        matchIndexes.sort()
        
        #starting from left we will find the biggest 
        # index, that is higher then the commitIndex of the leader
        # and that is replicated in the majority of nodes
        N = matchIndexes[-(self.majority - 1)]
        
        #If N is higher then the commitIndex, and if that entry's term 
        # equals the current term, then update the commitIndex to N
        if N > self.commitIndex and self.getLogEntry(N)[1] == self.currentTerm:
            self.commitIndex = N
            return True
        return False
    

    def tryUpdateLastApplied(self):
        if self.commitIndex > self.lastApplied:
            #increment lastApplied in one unit and apply the command to the state machine
            self.lastApplied += 1
            self.applyToStateMachine(self.lastApplied)

            #try updating again
            self.tryUpdateLastApplied()

    def applyToStateMachine(self,logIndex):
        if logIndex > len(self.log) : return

        msg, term = self.getLogEntry(logIndex)
        if msg.body.type == 'write':
            self.stateMachine[msg.body.key] = (msg.body.value, (logIndex,term))
            logging.debug("[Write] Applied index:" + str(logIndex) + " term: " + str(term) + " key: " + str(msg.body.key) + " value: " + str(msg.body.value))
            if self.state == 'Leader':
                reply(msg, type="write_ok")
    
        elif msg.body.type == 'cas':
            if msg.body.key not in self.stateMachine:
                logging.debug("[CAS] Applied index:" + str(logIndex) + " term: " + str(term) + " key: " + str(msg.body.key) + " error: Key does not exist")
                if self.state == 'Leader':
                    reply(msg, type="error", code=20)
            else:
                value,_ = self.stateMachine.get(msg.body.key)
                if value == getattr(msg.body,"from"):
                    logging.debug("[CAS] Applied index:" + str(logIndex) + " term: " + str(term) + " key: " + str(msg.body.key) + " value: " + str(value) + " from: " + str(getattr(msg.body,"from")) + " to: " + str(msg.body.to))
                    self.stateMachine[msg.body.key] = (msg.body.to, (logIndex,term))
                    if self.state == 'Leader':
                        reply(msg, type="cas_ok")
                else:
                    logging.debug("[CAS] Applied index:" + str(logIndex) + " term: " + str(term) + " key: " + str(msg.body.key) + " value: " + str(value) + " from: " + str(getattr(msg.body,"from")) + "[FAILED] to: " + str(msg.body.to))
                    if self.state == 'Leader':
                        reply(msg, type="error", code=22)
    
    def decrementNextIndex(self, node_id):
        self.nextIndex[node_id] -= 1

    #Returns a tuple containing the index and term of the last log's entry, 
    # in the order mentioned previously
    #If no entry exists, then returns (0,0)
    def getLastLogEntryIndexAndTerm(self):
        lastLogIndex = self.getLogSize()
        lastLogTerm = 0
        lastEntry = self.getLogEntry(lastLogIndex)
        if lastEntry != None:
            lastLogTerm = lastEntry[1]
        return lastLogIndex, lastLogTerm
                    
    ########################## GETTERS AND SETTERS ##########################

    def getLogSize(self):
        return len(self.log)

    def getRandomizer(self):
        return self.randomizer

    def getMajority(self):
        return self.majority

    def getVotedFor(self):
        return self.votedFor
    
    def getRequestsBuffer(self):
        result = self.requestsBuffer.copy()
        return result
    
    def clearRequestsBuffer(self):
        self.requestsBuffer.clear()
    
    def getNodes(self):
        return self.__node_ids

    def getCommitIndex(self):
        return self.commitIndex
    
    def getNodeId(self):
        return self.__node_id
    
    def getCurrentTerm(self):
        return self.currentTerm
    
    def getNextIndex(self, node_id):
        return self.nextIndex[node_id]
    
    def getLowerLimitElectionTout(self):
        return self.lowerLimElectionTout
    
    def getUpperLimitElectionTout(self):
        return self.upperLimElectionTout
    
    def getRaftTout(self):
        return self.raftTout
    
    def getLeaderId(self):
        return self.leader_id
    
    def getState(self):
        return self.state
    
    def getTimeout(self):
        if self.state == 'Leader':
            return self.sendAppendEntriesRPCTout
        else: 
            return self.getRandomElectionTimeout()

    def getRandomElectionTimeout(self):
        return self.randomizer.randint(self.lowerLimElectionTout, self.upperLimElectionTout)
    
    def resetTimeout(self):
        self.setRaftTout(self.getTimeout())
    
    def getValueFromStateMachine(self, key):
        return self.stateMachine.get(key)
    
    #'index' starts at 1, therefore to get the entry we want, this 
    #  variable must be decremented by 1 unit
    def getLogEntry(self, index : int):
        i = index - 1
        if i < 0: return None
        else:
            if i < len(self.log):
                return self.log[i]
            else:
                return None
    
    #Returns list of entries that follow the one represented by the given index (including the one with the index)
    #'index' starts at 1, therefore to get the entry we want, this 
    #  variable must be decremented by 1 unit
    def getLogNextEntries(self, index : int):
        i = index - 1
        if i < 0: return []
        else: return self.log[i:]

    def getSendAppendEntriesRPCTout(self):
        return self.sendAppendEntriesRPCTout

    def getNumNodes(self):
        return len(self.__node_ids)

    def getCopyNodes(self):
        return self.__node_ids.copy()

    def setRaftTout(self, timeoutValue):
        self.raftTout = timeoutValue
    
    def setVotedFor(self, vote):
        self.votedFor = vote

    def setCurrentTerm(self, term):
        self.currentTerm = term
    
    def setLeader(self, node_id):
        self.leader_id = node_id
    
    def setCommitIndex(self, commitIndex):
        self.commitIndex = commitIndex
    
    def setNextIndex(self, node_id, newNextIndex):
        self.nextIndex[node_id] = newNextIndex
    
    def setMatchIndex(self, node_id, newMatchIndex):
        self.matchIndex[node_id] = newMatchIndex


    