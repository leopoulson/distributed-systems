import java.nio.Buffer;
import java.util.*;
import java.io.*;
import java.util.stream.Collectors;

// This is how we tell each node what to do in each round.
enum Action { None, StartElection, Fail }

// custom exception type.
// used for when we try and parse an input file containing failures when we don't expect them.
class WrongFileException extends Exception {
	public WrongFileException () {}
}

public class Network {

	private LinkedHashMap<Integer, Node> nodes;
	private int period = 20;
	// The first integer key is the sender
	// the second integer key is the receiver
	// of the list of messages attached.
	private Map<Integer, Map<Integer, List<String>>> msgsToDeliver;
	private Set<Node> failedNodes;

	// These three are used for parsing.
	private List<List<Integer>> nodeInfos;
	private HashMap<Integer, List<Integer>> electionInfos;
	private HashMap<Integer, Integer> failureInfos;

	// Special id to indicate that a message is being sent to the network, not another node.
	public Integer networkId = -1;

	private File logFile;
	private String logFileName = "log.txt";

	// Flag that expresses whether the graph is disconnected.
	// If it is, it means we halt.
	private boolean isDisconnected = false;

	public void NetSimulator(String fileName, String otherFileName) {
		/*
		Code to call methods for parsing the input file, initiating the system and producing the log can be added here.
		*/

		// Initialise log.
		logFile = new File(logFileName);
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(logFile));
			bw.close();
		}
		catch (IOException e) {
			System.out.println("Couldn't create new file.");
			e.printStackTrace();
		}

		nodeInfos = new ArrayList<>();
		electionInfos = new HashMap<>();
		failureInfos = new HashMap<>();

		// parse the graph from the text file.
		parseGraph(fileName);

		// try and parse the election graph. it may fail with the custom exception defined above.
		// if this happens, we instead parse it as a failure-including file.
		try {
			parseElection(otherFileName);
		}
		catch (WrongFileException e) {
			parseFailures(otherFileName);
		}

		// Set up nodes and ring structure.
		nodes = createNodes(nodeInfos);
		failedNodes = new HashSet<>();
		buildRing();

		// Initialising message maps.
		msgsToDeliver = new HashMap<>();
		for (Node node : nodes.values()) {
			msgsToDeliver.put(node.getNodeId(), new HashMap<>());
			// For every neighbour, put a list of msgs there.
			for (Node neighbour : node.myNeighbours) {
				msgsToDeliver.get(node.getNodeId()).put(neighbour.getNodeId(), new LinkedList<>());
			}
		}

		// Finds the round at which we should stop the simulation.
		int lastRound = this.lastEvent();

		// Now we begin the main simulation loop.
		for (int round = 0; round <= lastRound; round++) {

			// This just lets the simulation exit early if the network becomes disconnected.
			if (this.isDisconnected) return;

			playRound(round);
 		}

		log("Simulation completed. ");
	}

	// Here we have the main logic of what happens in a round.
	private void playRound(int roundNumber) {
		System.out.println("\n\nRound " + roundNumber + ".\n");

		for (Node node : nodes.values()) {
			// Pick what to tell the node to do, based on if there's an election or not.
			Action action = Action.None;
			// If this round is an election round, and if the node in question is the elector, start the election.
			if (electionInfos.containsKey(roundNumber) && electionInfos.get(roundNumber).contains(node.getNodeId()))
				action = Action.StartElection;

			// If the round in question is a failure round and th enode in question is to fail, then make it fail.
			if (failureInfos.getOrDefault(roundNumber, -1).equals(node.getNodeId())) {
				action = Action.Fail;
			}

			// Now we run the action for the period of time.
			// If it takes longer than 20 milliseconds it gets cancelled.
			node.run(action);
			try {
				node.join(period);
			} catch (InterruptedException e) {
				System.out.println("Node " + node.getNodeId() + " was interrupted.");
				e.printStackTrace();
			}

			// Collect the messages from this node.
			collectMessages(node);
		}

		// Deliver the nodes on the network to the nodes.
		deliverMessages();
	}

	public synchronized void addMessage(Node sender, Node destination, String m) {
		msgsToDeliver.get(sender.getNodeId()).get(destination.getNodeId()).add(m);
	}

	// This function collects messages from nodes and puts them into the messages map.
	private synchronized void collectMessages(Node node) {
		// Resent is the nodes that are to be put back into the node's outgoing messages queue.
		// alreadyAddressed tracks the nodes that this node has already sent a message to.
		Queue<Pair<Integer, String>> resent = new LinkedList<>();
		Set<Integer> alreadyAddressed = new HashSet<>();

		for (Pair<Integer, String> message : node.outgoingMsgs) {
			// First we check if the message is addressed to the network.
			// If it is, then the network intercepts it and handles it.
			if (message.x.equals(this.networkId)) {
				processMessage(message.y);
			}
			// next we check if this node has already sent a message to the addressee.
			// if so, we put it in the resent queue, to be sent the next round.
			else if (alreadyAddressed.contains(message.x)){
				resent.add(message);
			}
			// if none of these conditions were true, we can just send the message.
			else {
				addMessage(node, nodes.get(message.x), message.y);
				alreadyAddressed.add(message.x);
			}
		}

		// We now set the outgoing messages to be resent.
		node.outgoingMsgs = resent;
	}

	// Here we take the messages from the intermediate array and deliver them.
	// We can safely deliver all of them; the above function guarantees that no node sends
	// two messages to a single node.
	public synchronized void deliverMessages() {
		// At this point, we deliver the messages of the nodes.
		for (Map.Entry<Integer, Map<Integer, List<String>>> msgMap : msgsToDeliver.entrySet()) {
			// find the sender of the messages
			Integer sender = msgMap.getKey();
			Map<Integer, List<String>> messages = msgMap.getValue();
			// for every message in the list of outgoing messages;
			for (Map.Entry<Integer, List<String>> messageList : messages.entrySet()) {
				if (messageList.getValue().size() > 0) {
					// find the destination, and add the message to that node's incoming messages.
					Integer destination = messageList.getKey();
					String msg = messageList.getValue().get(0);
					messageList.getValue().remove(0);

					if (nodes.get(sender).myNeighbours.contains(nodes.get(destination))) {
						nodes.get(destination).incomingMsgs.add(msg);
					}
				}
			}
		}
	}

	// If the network receives a message, this function gets called to perform the corresponding action.
	private void processMessage(String m) {
		List<String> messageTokens = Arrays.asList(m.split(" "));

		switch(messageTokens.get(0)) {
			// If a leader has been elected, we log it and return.
			case "leader_elected":
				System.out.println("Network is informed that a leader has been elected. ");
				log("Leader Node " + messageTokens.get(1));
				break;
			// If a node fails, we print that it has done, put it in the failed nodes set,
			// and start fixing it!
			case "failed_node":
				System.out.println("Network finds out that node " + messageTokens.get(1) + " has failed.");
				Node failed = nodes.get(Integer.parseInt(messageTokens.get(1)));
				failedNodes.add(failed);
				recoverFailure(failed);
				break;
			default:
				break;
		}
	}

	// This function gets called to reroute a new path around the failed node.
	// this consists of finding a new path to maintain the ring property, and "applying it"; telling the nodes
	// on the path where to redirect messages to.
	private void recoverFailure(Node failed) {
		for (Node node : nodes.values()) {
			// If the next node is the failed one, we have to fix the path.
			if (node.next.equals(failed)) {
				Node newNext = failed.next;

				// We call the findPath function to compute the path from `node` to `newNext`.
				// This might return no value, indicating that no path exists. In this case the graph is incomplete and
				// we return.
				Optional<List<Node>> recoverPath = findPath(node, newNext);

				if (recoverPath.isPresent()) {
					List<Node> path = recoverPath.get();
					System.out.println("New path from " + node.getNodeId() + " to " + newNext.getNodeId() + " via ");
					for (Node np : path) {
						System.out.print(np.getNodeId() + " ");
					}
					System.out.println();

					// Perform failure recovery. Put the new rerouting path in place, and update next.
					node.next = newNext;
					applyPath(path, node.next);
					// We now need to apply the path again, but backwards.
					Collections.reverse(path);
					applyPath(path, node);
				}
				else {
					System.out.println("Graph is incomplete! Simulation stopping.");
					this.isDisconnected = true;
				}
			}
		}
	}

	// Here we call changeDirection which does the rerouting for us.
	private void applyPath(List<Node> rerouting, Node destination) {
		for (int i = 0; i <= (rerouting.size() - 2); i++) {
			Node here = rerouting.get(i);
			Node there = rerouting.get(i + 1);

			here.changeDirection(destination.getNodeId(), there.getNodeId());
		}
	}

	// returns the path from a source node to a target node.
	// Basically just BFS. We store paths rather than nodes, and add on nodes to the end of them.
	// this lets us more easily return the path in the end.
	private Optional<List<Node>> findPath(Node source, Node target) {

		Queue<List<Node>> queue = new LinkedList<>();

		queue.add(new LinkedList<>(Collections.singletonList(source)));

		while (queue.size() > 0) {
			// We need to make sure that this does not loop forever.
			// We only have 9 nodes, so we need to make sure that queue lengths are not greater than 9.
			List<Node> path = queue.remove();
			if (path.size() >= 9) {
				break;
			}

			Node end = path.get(path.size() - 1);

			// if the end of this path is our target, return and finish.
			if (end.equals(target)) {
				return Optional.of(path);
			}

			// for each neighbour of `node`, if it has not failed, add it to the end of the current path and put this
			// on the queue.
			Set<Node> neighbours = end.myNeighbours;
			for (Node neighbour : neighbours) {
				if (!failedNodes.contains(neighbour)) {
					List<Node> newPath = new LinkedList<>(path);
					newPath.add(neighbour);
					queue.add(newPath);
				}
			}
		}

		// if no result, return empty.
		return Optional.empty();
	}

   	private Node lookupNodeById(Map<Integer, Node> nodes, Integer nodeId) {
		return nodes.get(nodeId);
	}

	private void addNeighbours(Node node, List<Node> neighbours) {
		for (Node neighbour : neighbours) {
			node.addNeighbour(neighbour);
		}
	}

	// Creates nodes from the corresponding parsed information.
   	private LinkedHashMap<Integer, Node> createNodes(List<List<Integer>> nodeInfos) {
		LinkedHashMap<Integer, Node> nodes = new LinkedHashMap<>();

		// Initialise each node and put it into nodes map.
		for (List<Integer> nodeInfo : nodeInfos) {
			Node node = new Node(nodeInfo.get(0), this);
			node.start();
			nodes.put(node.getNodeId(), node);
		}

		// Add the neighbours from the parsed file into each node.
		for (List<Integer> nodeNeighbours : nodeInfos) {
			Node toAddNeighbours = lookupNodeById(nodes, nodeNeighbours.get(0));
			// Create a list of nodes from the list of node ids.
			List<Node> neighbours = nodeNeighbours.stream()
					                    .map(nId -> lookupNodeById(nodes, nId))
										.collect(Collectors.toList());
			neighbours.remove(0);
			addNeighbours(toAddNeighbours, neighbours);
		}

		return nodes;
	}

	// Sets up the ring structure.
	private void buildRing() {
		List<Node> nodes = new ArrayList<>(this.nodes.values());
		for (int i = 0; i < nodes.size(); i++) {
			Node prev = nodes.get((i - 1 + nodes.size()) % nodes.size());
			Node next = nodes.get((i + 1) % nodes.size());
			Node here = nodes.get(i);

			here.addNeighbour(prev);
			here.addNeighbour(next);
			here.setNext(next);

		}
	}

	private int lastEvent() {
		Set<Integer> eventRounds = new HashSet<>();
		eventRounds.addAll(electionInfos.keySet());
		eventRounds.addAll(failureInfos.keySet());

		int result;

		try {
			result = Collections.max(eventRounds);
		} catch (NoSuchElementException e) {
			// for whatever reason, we might have no events in there.
			// as such the 'last event' happens at the creation of the graph, i.e. round 0
			result = 0;
		}

		return result + 50;
	}

	private void parseGraph(String graphFileName) {
		Scanner sc = null;
		try {
			sc = new Scanner(new File(graphFileName));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}


		while (sc.hasNextLine()) {
			String line = sc.nextLine();
			List<Integer> args = Arrays.stream(line.split(" "))
					.map(Integer::parseInt)
					.collect(Collectors.toList());
			nodeInfos.add(args);
		}
	}

	private void parseElection(String electionFileName) throws WrongFileException {
		Scanner sc = null;

		try {
			sc = new Scanner(new File(electionFileName));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		while (sc.hasNextLine()) {
			String line = sc.nextLine();

			List<String> args = Arrays.asList(line.split(" "));

			if (args.get(0).equals("FAIL")) {
				throw new WrongFileException();
			}

			Integer key = Integer.parseInt((args.get(1)));
			List<Integer> electors = new LinkedList<>();

			for (int i = 2; i < args.size(); i++) {
				electors.add(Integer.parseInt(args.get(i)));
			}

			electionInfos.put(key, electors);
		}
	}

	private void parseFailures(String failuresFilename) {
		Scanner sc = null;


		try {
			sc = new Scanner(new File(failuresFilename));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		while (sc.hasNextLine()) {
			String line = sc.nextLine();

			List<String> args = Arrays.asList(line.split(" "));
			String type = args.get(0);
			Integer round = Integer.parseInt(args.get(1));
			Integer node = Integer.parseInt(args.get(2));

			switch (type) {
				case "ELECT":
					electionInfos.put(round, Collections.singletonList(node));
					break;
				case "FAIL":
					failureInfos.put(round, node);
					break;
			}
		}

		for (Map.Entry<Integer, Integer> entry : failureInfos.entrySet()) {
			System.out.println(entry.getKey().toString() + entry.getValue().toString());
		}
	}

	private void log (String message) {
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(logFile, true));
			bw.write(message + "\n");
			bw.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String args[]) {
		Network network = new Network();
		network.NetSimulator(args[0], args[1]);
		}

}
