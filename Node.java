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
	private int T = 12;
	private int D = 5;
	private int sinceT = 0;
	private int sinceD = 0;
	private FCState fcState = FCState.Successful;
	
	// Neighbouring nodes
	public Set<Node> myNeighbours;

	// Nodes that this node knows have failed.
	private Set<Node> failedNodes;

	public Node next;

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

		redirection = new LinkedHashMap<>();
	}
	
	// Basic methods for the Node class
	public int getNodeId() {
		/*
		Method to get the Id of a node instance
		*/

		return id;
		}
		
	public void addNeighbour(Node n) {
		myNeighbours.add(n);
		}

	public void setNext(Node next) {

		this.next = next;
		myNeighbours.add(next);
	}

	public void run(Action action) {
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
			System.out.println("Node " + getNodeId() + " has failed.");
			hasFailed = true;
		}

		// Handle all incoming messages
		if (!hasFailed) {
			for (String msg : incomingMsgs) {
				receiveMsg(msg);
			}
			incomingMsgs.clear();
		}

		// Here we perform failure checking operations.
		if (!hasFailed) {
			updateFailures();
		}
	}

	// Here we receive a single message and choose what to do based on its contents.
	public void receiveMsg(String m) {
		System.out.println("Node " + getNodeId() + " receives `" + m + "`.");

		List<String> messageTokens = Arrays.asList(m.split(" "));

		String message = messageTokens.get(0);
		int parameter = Integer.parseInt(messageTokens.get(1));
		int destination = Integer.parseInt(messageTokens.get(2));

		// If the destination of the message is this node, we process it.
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
					break;
			}
		}
		// If the destination of the message is not the node in question, we forward it the corresponding place.
		else {
			// In here, we want to redirect the message to the desination.
			Integer redestination = redirection.get(destination);
			sendMsg(message + " " + parameter, redestination);
		}
	}

	// This function "sends" a message.
	public void sendMsg(String m, Integer destination) {
		if (hasFailed) {
			System.out.println("Node " + getNodeId() + " has failed, so cannot send message `" + m + ".");
			return;
		}

		// Always append the destination to the message.
		m = m + " " + destination;

		// If this message is going to a node that has failed, we really send it to the redirected node.
		if (redirection.containsKey(destination)) {
			destination = redirection.get(destination);
		}

		System.out.println("Node " + getNodeId() + " sends message `" + m + "` to " + destination + ".");
		outgoingMsgs.add(new Pair<>(destination, m));
	}

	// This function keeps failure checking ticking over.
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
		// First we check that the next node has not already failed.
		if (!failedNodes.contains(next))  {
			System.out.println("Node " + id + " detects that node " + next.getNodeId() + " has failed.");
			failedNodes.add(next);
			sendMsg("failed_node " + next.getNodeId(), network.networkId);

			if (next.getNodeId() == leader) {
				// We need to start election /next/ round.
				// This is because the network won't yet have found the new path to the next node.
				this.internalAction = Action.StartElection;
			}

			// Finally reset the failure checker.
			// This is because we could be assigned a new node,
			// and now need to check the failure status of this node.
			resetFailureCheck();
		}
	}

	// Here we start an election.
	private void startElection() {
		System.out.println("Node " + id + " starting election.");
		isParticipant = true;
		sendMsg("elect " + getNodeId(), next.getNodeId());
	}

	// This handles an election message being received.
	private void handleElection(Integer electorId) {

		if (!isParticipant) {
			isParticipant = true;
			// Send election message of max (m.id, p.id) to the next node
			sendMsg("elect " + Integer.max(electorId, getNodeId()), next.getNodeId());
		}
		else {
			if (electorId > getNodeId()){
				// send elector id to the next one.
				sendMsg("elect " + electorId, next.getNodeId());
			}
			else if (electorId == getNodeId()) {
				isLeader = true;
				leader = getNodeId();
				sendMsg("leader " + getNodeId(), next.getNodeId());
			}
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
			// if the leader is not the current one, we set our leader to be the one from the message.
			// we also forward the message on to the next node.
			leader = leaderId;
			System.out.println("Node " + getNodeId() + "'s leader becomes " + leaderId);
			sendMsg("leader " + leaderId, next.getNodeId());
		}
	}

	// If we receive a failure check message, we just return with a failure response message.
	private void handleFailureCheck(Integer checkerId) {
		sendMsg("failure_response " + getNodeId(), checkerId);
	}

	// If we get a failure response message, we just say that we know the node is active.
	private void handleFailureResponse(Integer responderId) {
		if (responderId.equals(next.getNodeId())) {
			System.out.println("Node " + getNodeId() + " acknowledges that node " + responderId + " is active.");
			resetFailureCheck();
		}
	}

	// Update redirection map.
	public void changeDirection(Integer finalDest, Integer stopOff) {
		if (redirection.containsKey(finalDest)) {
			redirection.remove(finalDest);
			redirection.put(finalDest, stopOff);
		} else {
			redirection.put(finalDest, stopOff);
		}
	}
}
