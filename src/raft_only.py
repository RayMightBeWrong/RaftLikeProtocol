#!/usr/bin/env python3

from threading import Thread
from ms import receiveAll, reply, send
import logging, time
from state import State
from appendEntriesRPC import *
from requestVoteRPC import *

logging.getLogger().setLevel(logging.DEBUG)

########### STATE CLASSES ###########

stateClass : State

########### Raft Timeouts ###########

raftTimeoutsThread = None
runTimeoutThread = True # while true keep running the timeout thread 


def initRaftTimeoutsThread():
    global raftTimeoutsThread
    raftTimeoutsThread = Thread(target=raftTimeoutsScheduler, args=())
    raftTimeoutsThread.start()

def raftTimeoutsScheduler():
    global stateClass

    #This will be running in a secondary thread, and the timeouts are important, 
    # so if the node changes to another state it is important to catch that 
    # change as soon as possible.
    # For that we will execute sleeps of min(raftTout, lowest of all timeouts).
    # Since the lowest of all timeouts is the 50 milliseconds for a leader to send
    # a new batch of AppendEntries RPCs, the sleeps will be of a maximum of 50 ms
    
    # set the initial timeout
    stateClass.lock.acquire()
    stateClass.resetTimeout()
    stateClass.lock.release()

    while runTimeoutThread:
        stateClass.lock.acquire()

        if stateClass.getRaftTout() > stateClass.getSendAppendEntriesRPCTout():
            #updates 'raftTout' variable, decreasing it by 'sendAppendEntriesRPCTout', 
            # but it can't go bellow 0, so we use the max function 
            stateClass.updateRaftTout()
            #raftTout = max(0, raftTout - sendAppendEntriesRPCTout) 

            stateClass.lock.release() # releases lock before going to sleep

            #dividing by thousand, since the argument is in seconds
            time.sleep(stateClass.getSendAppendEntriesRPCTout() / 1000) 
        else:

            auxTout = stateClass.getRaftTout()
            stateClass.setRaftTout(0)
            stateClass.lock.release() # releases lock before going to sleep
            time.sleep(auxTout / 1000)

        stateClass.lock.acquire()

        #if the timeout reached 0, then its time to 
        # call the timeout handler and reset the timeout
        if stateClass.getRaftTout() == 0:
            if stateClass.getState() == 'Leader':
                handleSendAppendEntriesRPCTimeout() 
            else:
                logging.warning("  ELECTION TIMEOUT")
                handleElectionTimeout()
            stateClass.resetTimeout()

        stateClass.lock.release()

def handleSendAppendEntriesRPCTimeout():
    sendAppendEntriesRPCToAll(stateClass)

def handleElectionTimeout():
    global currentTerm, votedFor

    node_id = stateClass.getNodeId()
    # changes to Candidate state, increments term and votes for itself
    stateClass.changeStateTo('Candidate')
    
    stateClass.incrementTerm()

    stateClass.setVotedFor(node_id)
    
    stateClass.receiveVote(node_id)

    # broadcast RequestVote RPC
    broadcastRequestVoteRPC(stateClass)


########### Handle Init Msg ###########

def handleInit(msg):
    global stateClass, readsState
    stateClass = State(msg.body.node_id, msg.body.node_ids)
    initRaftTimeoutsThread()
    logging.info('node %s initialized', stateClass.getNodeId())
    reply(msg, type='init_ok')


########### Handle Read Request ###########

def handleRead(msg):
    stateClass.lock.acquire()

    if stateClass.getState() == 'Leader': 
        logging.info('read %s', msg.body.key)
        
        if stateClass.keyInStateMachine(msg.body.key): 
            value,_ = stateClass.getValueFromStateMachine(msg.body.key)
            reply(msg, type='read_ok', value=value) 
        else:
            reply(msg, type='read_ok', value=None)
        
        stateClass.lock.release()
        return
    
    else:
        #Get the necessary variables before release the raft state lock
        leaderId = stateClass.getLeaderId()
        nodeId = stateClass.getNodeId()
        stateClass.lock.release()

        #If the leader is known, redirect the read to the leader
        if leaderId != None:
            logging.info("Redirecting read to leader:" + str(msg))
            send(nodeId, leaderId, type="read_leader", read_msg=msg)
        else:
            reply(msg, type="error", code=11)
    
        return

########### Handle Write Request ###########

def handleWrite(msg):
    stateClass.lock.acquire()

    if stateClass.getState() == 'Leader': 
        logging.info('write %s:%s', msg.body.key, msg.body.value)
        stateClass.addEntryToLog(msg) 
        sendAppendEntriesRPCToAll(stateClass)
    else: 
        #Only Leader can answer this type of requests from clients
        stateClass.appendRequestEntry(msg)
    
    stateClass.lock.release()

########### Handle CAS Request ###########

def handleCAS(msg):
    stateClass.lock.acquire()

    if stateClass.getState() == 'Leader':  
        logging.info('cas %s:%s:%s', msg.body.key, getattr(msg.body,"from"), msg.body.to)
        stateClass.addEntryToLog(msg)
        sendAppendEntriesRPCToAll(stateClass) 
    else: #Only Leader can answer requests from clients
        #Only Leader can answer this type of requests from clients
        stateClass.appendRequestEntry(msg)
    
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
        handleAppendEntriesRPC(stateClass, msg)

    elif msg.body.type == 'AppendEntriesRPCResponse':
        handleAppendEntriesRPCResponse(stateClass, msg)

    elif msg.body.type == 'RequestVoteRPC':
        handleRequestVoteRPC(stateClass, msg)

    elif msg.body.type == 'RequestVoteRPCResponse':
        handleRequestVoteRPCResponse(stateClass, msg)

    elif msg.body.type == 'read_leader':
        logging.info('read leader: ' + str(msg.body.read_msg))
        stateClass.lock.acquire()
        if stateClass.keyInStateMachine(msg.body.read_msg.body.key): 
            value,_ = stateClass.getValueFromStateMachine(msg.body.read_msg.body.key)
            reply(msg, type='read_leader_resp', read_msg=msg.body.read_msg, value=value) 
        else:
            reply(msg, type='read_leader_resp', read_msg=msg.body.read_msg, value=None)
        stateClass.lock.release()

    elif msg.body.type == 'read_leader_resp':
        logging.info("Received leader read response: msg=" + str(msg.body.read_msg) + " | value=" + str(msg.body.value))
        reply(msg.body.read_msg, type="read_ok", value=msg.body.value)
        
    else:
        logging.warning('unknown message type %s', msg.body.type)

runTimeoutThread = False