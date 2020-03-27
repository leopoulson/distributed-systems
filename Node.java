import java.util.*;
import java.util.stream.Collectors;


/* Class to represent a node. Each node must run on its own thread.*/

public class Node extends Thread {

	private int id;
	private boolean isParticipant = false;
	private boolean isLeader = false;
	private boolean hasFailed = false;
	private Integer leader = -1;
	private Network network;

	// T is the time interval to perform a failure check
	// D is the time to wait for a response.
	// sinceT is the time elapsed since the last failure check
	private int T = 8;
	private int D = 5;
	private int sinceT = 0;
	private int sinceD = 0;
	private FCState fcState = FCState.Successful;
	
	// Neighbouring nodes
	// TODO Also make sure to add the "implicit" edges
	public Set<Node> myNeighbours;

	// Nodes that this node knows have failed.
	private Set<Node> failedNodes;
	private Set<Integer> failedIds;

	// Next and previous nodes
	public Node next;
	private Node previous;

	// Queues for the incoming messages
	public Queue<String> incomingMsgs;

	// Place for the outgoing messages.
	public Queue<Pair<Integer, String>> outgoingMsgs;

	// This tells us what the action for the next round should be.
	// This is done separately from the network-given action.
	private Action internalAction = Action.None;

	// This map tells us where to send a message.
	// Pre-failure, it is just the place. But after failure, it could be elsewhere.
	private Map<Integer, Integer> redirection;

	// FCState embodies the state of failure check for the next node.
	enum FCState { Sent, Waiting, Successful, Failed }

	public Node(int id, Network network){
	
		this.id = id;
		this.network = network;
		
		myNeighbours = new HashSet<>();
		incomingMsgs = new LinkedList<>();
		outgoingMsgs = new LinkedList<>();

		failedNodes = new HashSet<>();
		failedIds = new HashSet<>();

		redirection = new LinkedHashMap<>();
//		direction.put(network.networkId, network.networkId);

	}
	
	// Basic methods for the Node class
	
	public int getNodeId() {
		/*
		Method to get the Id of a node instance
		*/

		return id;
		}
			
	public boolean isNodeLeader() {
		/*
		Method to return true if the node is currently a leader
		*/

		return false;
		}
		
	public Set<Node> getNeighbors() {
		/*
		Method to get the neighbours of the node
		*/

		return myNeighbours;
		}
		
	public void addNeighbour(Node n) {
		/*
		Method to add a neighbour to a node
		*/


		myNeighbours.add(n);
//		direction.put(n.getNodeId(), n.getNodeId());
//		System.out.println("Added neighbour " + n.getNodeId() + " to node " + this.id + ".");
		}

	public Node getNext() {
		return next;
	}

	public void setNext(Node next) {

		this.next = next;
		if (!myNeighbours.contains(next))
			myNeighbours.add(next);
	}

	public Node getPrevious() {
		return previous;
	}

	public void setPrevious(Node previous) {
		this.previous = previous;
	}

	public void run(Action action) {
		// Reset the outgoing message.
		// We don't do this. we just want to add
		//outgoingMsgs = Optional.empty();

		// if told to start an election, start it!
		// it can also occur that the node has to start an election next turn because the next node, the leader, failed.
		// in this case the next node will have indeed failed, and if we try and send a message there it will get lost,
		// so we have to wait one turn for the network to re-route the node.
		if ((action == Action.StartElection || this.internalAction == Action.StartElection)
			&& !(failedNodes.contains(this.next))) {
			startElection();
			this.internalAction = Action.None;
		}
		else if (action == Action.Fail) {
			// We don't do anything else.
			System.out.println("Node " + getNodeId() + " has failed.");
			hasFailed = true;
		}


		// Pop a message from the queue.
		// If there is no message, then we do nothing.
		// Also if we have failed, we cannot remove a message from the queue.
		// TODO: Consider if it's better to handle failure in the receiveMsg function?
		if (incomingMsgs.size() > 0 && !hasFailed) {
			String msg = incomingMsgs.remove();
			receiveMsg(msg);
		}


		// Here we perform failure checking operations.
		if (!hasFailed) {
			updateFailures();
		}


	}
				
	public void receiveMsg(String m) {
		System.out.println("Node " + getNodeId() + " receives `" + m + "`.");

		List<String> messageTokens = Arrays.asList(m.split(" "));

		String message = messageTokens.get(0);
		Integer parameter = Integer.parseInt(messageTokens.get(1));
		Integer destination = Integer.parseInt(messageTokens.get(2));

		if (destination == this.getNodeId()) {
			switch (message) {
				case "elect":
					handleElection(parameter);
					break;
				case "leader":
					handleLeader(parameter);
					break;
				case "failure_check":
					handleFailureCheck(parameter);
					break;
				case "failure_response":
					handleFailureResponse(parameter);
					break;
				default:
					// maybe error?
					break;
			}
		}
		else {
			// In here, we want to redirect the message to the desination.
			Integer redestination = redirection.get(destination);
			sendMsg(message + " " + parameter, redestination);
		}
	}

	public void sendMsg(String m, Integer destination) {
		/*
		Method that implements the sending of a message by a node.
		The message must be delivered to its recepients through the network.
		This method need only implement the logic of the network receiving an outgoing message from a node.
		The remainder of the logic will be implemented in the network class.
		*/

		if (hasFailed) {
			System.out.println("Node " + getNodeId() + " has failed, so cannot send message `" + m + ".");
			return;
		}

		m = m + " " + destination;

//		Set<Integer> failedIds = failedNodes.stream().map(n -> n.getNodeId()).collect(Collectors.toSet());
		if (redirection.containsKey(destination)) {
//			System.out.println(this.getNodeId() + " Destination failed " + destination);
			destination = redirection.get(destination);
		}

		System.out.println("Node " + getNodeId() + " sends message `" + m + "` to " + destination + ".");
//		Integer redirectedDest = redirection.get(destination);
		outgoingMsgs.add(new Pair(destination, m));
	}

	private void startElection() {
		System.out.println("Node " + id + " starting election.");
		isParticipant = true;
		sendMsg("elect " + getNodeId(), next.getNodeId());
	}

	private void updateFailures() {

		// First, if we are waiting for a response, increment the failure timer.
		if (fcState == FCState.Waiting) {
			sinceD++;
		}

		// If twice the estimated time to reach a node has passed, we say that the node has failed.
		if (sinceD > 2 * D && fcState.equals(FCState.Waiting)) {
			failFailureCheck();
		}

		// If T rounds have elapsed since the last successful check, and we haven't
		// just sent another check, start a new one.
		if (sinceT > T && fcState.equals(FCState.Successful)) {
			startFailureCheck();
		}

		// If the last failure check was successful, we increment the time since it happened.
		if (fcState.equals(FCState.Successful)) {
			sinceT++;
		}
	}

	private void startFailureCheck() {
		System.out.println("Node " + id + " starting failure check.");

		// Send the failure check message to the next node.
		sendMsg("failure_check " + id, next.getNodeId());

		// Update failure check state and reset timer.
		fcState = FCState.Waiting;
		sinceD = 0;
	}

	// Call this to reset the failure checker to the idle state.
	private void resetFailureCheck() {
		fcState = FCState.Successful;
		sinceT = 0;
		sinceD = 0;
	}

	private void failFailureCheck() {
		if (!failedNodes.contains(next))  {
			System.out.println("Node " + id + " detects that node " + next.getNodeId() + " has failed.");
			failedNodes.add(next);
			failedIds.add(next.getNodeId());
			System.out.println(failedIds.toString());
			sendMsg("failed_node " + next.getNodeId(), network.networkId);

			// TODO: See if we need to re-elect a leader.
			if (next.getNodeId() == leader) {
				// We need to start election /next/ round.
				this.internalAction = Action.StartElection;
			}

			// Finally reset the failure checker.
			// This is because we could be assigned a new node,
			// and now need to check the failure status of this node.
			resetFailureCheck();
		}
	}

	private void handleElection(Integer electorId) {

		if (!isParticipant) {
			isParticipant = true;
			// Send election message of max (m.id, p.id) to the next node
			sendMsg("elect " + Integer.max(electorId, getNodeId()), next.getNodeId());
		}
		else if (isParticipant) {
			if (electorId > getNodeId()){
				// send elector id to the next one.
				sendMsg("elect " + electorId, next.getNodeId());
			}
			else if (electorId == getNodeId()) {
				isLeader = true;
				leader = getNodeId();
				sendMsg("leader " + getNodeId(), next.getNodeId());
			}
			// If electorId < getNodeID(), we do nothing.
		}
	}

	private void handleLeader(Integer leaderId) {
		if (this.leader.equals(leaderId)) {
			// If the leader is the current one, we don't do anything
			if (this.leader.equals(getNodeId())) {
				// unless it's the node here, in which case we tell the network that our node has been elected as leader.
				sendMsg("leader_elected " + this.leader, network.networkId);
			}
		}
		else {
			leader = leaderId;
			System.out.println("Node " + getNodeId() + "'s leader becomes " + leaderId);
			sendMsg("leader " + leaderId, next.getNodeId());
		}
	}

	private void handleFailureCheck(Integer checkerId) {
		//System.out.println("Node " + getNodeId() + " received failure check from " + checkerId + ".");
		sendMsg("failure_response " + getNodeId(), checkerId);
	}

	private void handleFailureResponse(Integer responderId) {
		if (responderId.equals(next.getNodeId())) {
			System.out.println("Node " + getNodeId() + " acknowledges that node " + responderId + " is active.");
			resetFailureCheck();
		}
	}

	public void changeDirection(Integer finalDest, Integer stopOff) {
		System.out.println(redirection.toString());
		System.out.println("Updating " + this.getNodeId() + " redirection to " + finalDest + " via " + stopOff);

		if (redirection.containsKey(finalDest)) {
			redirection.remove(finalDest);
			redirection.put(finalDest, stopOff);
		}
		else{
			redirection.put(finalDest, stopOff);
		}


		System.out.println(redirection.toString());
	}
}
