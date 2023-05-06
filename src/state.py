from threading import Thread, Lock
from ms import reply
from math import comb
from random import Random
import logging

class State:
    def __init__(self):

        self.node_id = None
        self.node_ids = None
        self.state = 'Follower' #   can be onde of the following: 'Follower', 'Candidate', 'Leader'
        self.leader_id = None 
        self.majority = None # quantity of nodes necessary to represent the majority
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

        # Probability(%) of querying the leader instead of performing a quorum read
        self.probReadFromLeader = None 

        #index of highest log entry applied to state machine 
        # (initialized to 0, increases monotonically)
        self.lastApplied = 0

        self.nextReadID = 0 # To associate an ID with the read requests from the clients

        ### Volatile state on leaders ### 
        #(Reinitialized after election)

        #for each server, index of the next log entry to send to that server
        # (initialized to leader last log index + 1)
        self.nextIndex = dict() 

        #for each server, index of highest log entry known to be replicated on server
        # (initialized to 0, increases monotonically)
        self.matchIndex = dict()

        self.randomizer = None
        self.lock = Lock()

        ###### QUORUM READ STATE ######
        self.quorumReadCounter = 0 # request id, usefull to keep track of responses 
        self.quorumReadRequests = {} # dictionary to collect responses ( id ) -> ( msg, value, index, term, numberOfAcks )

        ########### Raft Timeouts ###########
        
        self.raftTimeoutsThread = None
        self.runTimeoutThread = True # while true keep running the timeout thread 
        self.sendAppendEntriesRPCTout = 50 # timeout to send AppendEntries RPC to every server (in milliseconds)
        self.lowerLimElectionTout = 300 # lower limit for the random generation of the timeout to become candidate in case no message from a leader arrives (in milliseconds)
        self.upperLimElectionTout = 500 # upper limit for the random generation of the timeout to become candidate in case no message from a leader arrives (in milliseconds)
        self.raftTout = None #raft related timeout

   ########################## AUX FUNCTIONS ##########################
    
    def numberOfVotes(self):
        return len(self.receivedVotes)

    def logAppendEntry(self, entry):
        self.log.append(entry)
    
    def removeLogEntry(self, index):
        self.log.pop(index)

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

        for n in self.node_ids:
            if n != self.node_id:
                self.nextIndex[n] = nextI
                self.matchIndex[n] = 0


    def changeStateTo(self,newState):
        self.leader_id = None
        self.state  = newState
        self.raftTout = self.getTimeout() #refresh timeout

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
        N = matchIndexes[-(self.majority - 1 )]
        
        #If N is higher then the commitIndex, and if that entry's term 
        # equals the current term, then update the commitIndex to N
        if N > self.commitIndex  and self.getLogEntry(N)[1] == self.currentTerm:
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
            if self.state == 'Leader':
                reply(msg, type="write_ok")
        
        if msg.body.type == 'cas':
            if msg.body.key not in self.stateMachine:
                if self.state == 'Leader':
                    reply(msg, type="error", code=20)
            else:
                value,_ = self.stateMachine.get(msg.body.key)
                if value == getattr(msg.body,"from"):
                    self.stateMachine[msg.body.key] = (msg.body.to,(logIndex,self.currentTerm))
                    if self.state == 'Leader':
                        reply(msg, type="cas_ok")
                else:
                    if self.state == 'Leader':
                        reply(msg, type="error", code=22)
    
    def incrementNextReadID(self):
        self.nextReadID += 1
    
    def decrementNextIndex(self, node_id):
        self.nextIndex[node_id] -= 1

                    
    ########################## GETTERS AND SETTERS ##########################

    def getLogSize(self):
        return len(self.log)

    def getProbReadFromLeader(self):
        return self.probReadFromLeader

    def getRandomizer(self):
        return self.randomizer

    def getMajority(self):
        return self.majority

    def getVotedFor(self):
        return self.votedFor
    
    def getRequestsBuffer(self):
        result = self.requestsBuffer.copy()
        self.requestsBuffer.clear()
        return result
    
    def getNodes(self):
        return self.node_ids

    def getCommitIndex(self):
        return self.commitIndex
    
    def getNodeId(self):
        return self.node_id
    
    def getCurrentTerm(self):
        return self.currentTerm
    
    def getNextIndex(self, node_id):
        return self.nextIndex[node_id]
    
    def getLowerLimitElectionTout(self):
        return self.lowerLimElectionTout
    
    def getUpperLimitElectionTout(self):
        return self.upperLimElectionTout
    
    def getRaftout(self):
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
    
    def getNextReadID(self):
        self.nextReadID

    def getNumNodes(self):
        return len(self.node_ids)

    def getCopyNodes(self):
        return self.node_ids.copy()

    def setRaftTout(self, timeoutValue):
        self.raftTout = timeoutValue
    
    def setNodeId(self,node_id):
        self.node_id = node_id
    
    def setNodeIds(self,node_ids):
        self.node_ids = node_ids.copy()
    
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
    
    ########################## INIT FUNCTIONS ##########################

    def initMajority(self):
        self.majority = len(self.node_ids) // 2 + 1 
    
    def initProbReadFromLeader(self):
        nodeCount = len(self.node_ids)
        P = None
        if nodeCount <= 3 : P = 1
        else: P = comb(nodeCount - 3, nodeCount // 2 - 1 ) / comb(nodeCount - 2, nodeCount // 2) 
        self.probReadFromLeader = (P * (nodeCount - 2)) / (nodeCount + P * (nodeCount - 2)) * 100

    def initRandomizer(self):
        self.randomizer = Random(self.node_id)

    