import java.util.*;


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
	private int T = 10;
	private int D = 3;
	private int sinceT = 0;
	private int sinceD = 0;
	private FCState fcState = FCState.Successful;
	
	// Neighbouring nodes
	public List<Node> myNeighbours;

	// Next and previous nodes
	private Node next;
	private Node previous;

	// Queues for the incoming messages
	public Queue<String> incomingMsgs;

	// Place for the outgoing messages.
	public Queue<Pair<Integer, String>> outgoingMsgs;

	// FCState embodies the state of failure check for the next node.
	enum FCState { Sent, Waiting, Successful, Failed };

	public Node(int id){
	
		this.id = id;
		this.network = network;
		
		myNeighbours = new ArrayList<Node>();
		incomingMsgs = new LinkedList<String>();
		outgoingMsgs = new LinkedList<>();

//		System.out.println("Created node with id " + id + ".");
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
		
	public List<Node> getNeighbors() {
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
//		System.out.println("Added neighbour " + n.getNodeId() + " to node " + this.id + ".");
		}

	public Node getNext() {
		return next;
	}

	public void setNext(Node next) {
		this.next = next;
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
		if (action == Action.StartElection) {
			startElection();
		}
		else if (action == Action.Fail) {
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
		/*
		Method that implements the reception of an incoming message by a node
		*/

		System.out.println("Node " + getNodeId() + " receives `" + m + "`.");

		List<String> messageTokens = Arrays.asList(m.split(" "));

		Integer parameter = Integer.parseInt(messageTokens.get(1));

		switch (messageTokens.get(0)) {
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
		}
		else {
			System.out.println("Node " + getNodeId() + " sends message `" + m + "` to " + destination + ".");
			outgoingMsgs.add(new Pair(destination, m));
		}
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
		if (sinceD > 2 * D) {
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

	private void failFailureCheck() {
		System.out.println("Node " + id + " detects that node " + next.getNodeId() + " has failed.");
		// do some other stuff
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
		if (this.leader == leaderId) {
			// We don't do anything?
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
			fcState = FCState.Successful;
			sinceT = 0;
		}
	}
}
