from ms import send
from state import State
import logging

########### Auxiliar Functions ###########

# Sends msg with the given body to all the other nodes
def broadcast(stateClass : State, **body):
    logging.warning(body)
    for n in stateClass.getNodes():
        node_id = stateClass.getNodeId()
        if n != node_id:
            send(node_id, n, **body)

#Returns a tuple containing the index and term of the last log's entry, 
# in the order mentioned previously
#If no entry exists, then returns (0,0)
def getLastLogEntryIndexAndTerm(stateClass : State):
    lastLogIndex = stateClass.getNumNodes()
    lastLogTerm = 0
    lastEntry = stateClass.getLogEntry(lastLogIndex)
    if lastEntry != None:
        lastLogTerm = lastEntry[1]
    return lastLogIndex, lastLogTerm