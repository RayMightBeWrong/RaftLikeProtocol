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