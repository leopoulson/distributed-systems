import java.util.*;


/* Class to represent a node. Each node must run on its own thread.*/

public class Node extends Thread {

	private int id;
	private boolean isParticipant = false;
	private boolean isLeader = false;
	private Integer leader = -1;
	private Network network;
	
	// Neighbouring nodes
	public List<Node> myNeighbours;

	// Next and previous nodes
	private Node next;
	private Node previous;

	// Queues for the incoming messages
	public List<String> incomingMsg;

	// Place for the outgoing messages.
	// The node might not send a message, hence the option.
	public Optional<Pair<Integer, String>> outgoingMsg;
	
	public Node(int id){
	
		this.id = id;
		this.network = network;
		
		myNeighbours = new ArrayList<Node>();
		incomingMsg = new LinkedList<String>();

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
		outgoingMsg = Optional.empty();

		// if told to start an election, start it!
		if (action == Action.StartElection) {
			startElection();
		}

//		System.out.println("Node " + getNodeId() + " is running. Fwd = " + this.next.getNodeId() + " bwd = " + this.previous.getNodeId());


		// Pop a message from the queue.
		// If there is no message, then we do nothing.
		if (incomingMsg.size() > 0) {
			String msg = incomingMsg.remove(0);
			receiveMsg(msg);
		}
	}
				
	public void receiveMsg(String m) {
		/*
		Method that implements the reception of an incoming message by a node
		*/

		System.out.println("Node " + getNodeId() + " receives `" + m + "`.");

		List<String> messageTokens = Arrays.asList(m.split(" "));
		if (messageTokens.get(0).equals("elect")) {
//			System.out.println("Node " + getNodeId() + " is electing");
			handleElection(Integer.parseInt(messageTokens.get(1)));
		}
		else if (messageTokens.get(0).equals("leader")) {
			handleLeader(Integer.parseInt(messageTokens.get(1)));
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
		if (this.leader == leaderId) {
			// We don't do anything?
		}
		else {
			leader = leaderId;
			System.out.println("Node " + getNodeId() + "'s leader becomes " + leaderId);
			sendMsg("leader " + leaderId, next.getNodeId());
		}
	}
		
	public void sendMsg(String m, Integer destination) {
		/*
		Method that implements the sending of a message by a node. 
		The message must be delivered to its recepients through the network.
		This method need only implement the logic of the network receiving an outgoing message from a node.
		The remainder of the logic will be implemented in the network class.
		*/

		System.out.println("Node " + getNodeId() + " sends message `" + m + "` to " + destination + ".");
		outgoingMsg = Optional.of(new Pair(destination, m));
	}

	public void startElection() {
		System.out.println("Node " + id + " starting election.");
		isParticipant = true;
		sendMsg("elect " + getNodeId(), next.getNodeId());
	}
}
