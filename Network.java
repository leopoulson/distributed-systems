import java.nio.Buffer;
import java.util.*;
import java.io.*;
import java.util.stream.Collectors;

/* 
Class to simulate the network. System design directions:

- Synchronous communication: each round lasts for 20ms
- At each round the network receives the messages that the nodes want to send and delivers them
- The network should make sure that:
	- A node can only send messages to its neighbours
	- A node can only send one message per neighbour per round
- When a node fails, the network must inform all the node's neighbours about the failure
*/

enum Action { None, StartElection, Fail }

public class Network {

	private LinkedHashMap<Integer, Node> nodes;
	private int period = 20;
	private Map<Integer, List<String>> msgsToDeliver; //Integer for the id of the sender and String for the message
	private Set<Node> failedNodes;

	private List<List<Integer>> nodeInfos;
	private HashMap<Integer, List<Integer>> electionInfos;
	private HashMap<Integer, Integer> failureInfos;

	public Integer networkId = -1;

	private File logFile;
	private String logFileName = "log.txt";

	public void NetSimulator(String fileName) {
		/*
		Code to call methods for parsing the input file, initiating the system and producing the log can be added here.
		*/

		// Initialise file.
		logFile = new File(logFileName);
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(logFile));
			bw.write("Part A\n");
			bw.close();
		}
		catch (IOException e) {
			System.out.println("Couldn't create new file.");
			e.printStackTrace();
		}


		parseGraph(fileName);
		parseElection("text/ds_elect.txt");
		parseFailures("text/ds_fail.txt");

		nodes = createNodes(nodeInfos);
		failedNodes = new HashSet<>();
		buildRing();

		// Initialising message maps.
		msgsToDeliver = new HashMap<>();
		for (Integer i : nodes.keySet()) {
			msgsToDeliver.put(i, new LinkedList<>());
		}


		for (int round = 50; round < 250; round++) {
			if (round == 50) {
				try {
					BufferedWriter bw = new BufferedWriter(new FileWriter(logFile, true));
					bw.write("\nPart B\n");
					bw.close();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
			playRound(round);
 		}
	}

	private void playRound(int roundNumber) {
		System.out.println("\n\nRound " + roundNumber + ".\n");

		for (Node node : nodes.values()) {
			// Pick what to tell the node to do, based on if there's an election or not.
			Action action = Action.None;
			if (electionInfos.containsKey(roundNumber) && electionInfos.get(roundNumber).contains(node.getNodeId()))
				action = Action.StartElection;

			if (failureInfos.getOrDefault(roundNumber, -1).equals(node.getNodeId())) {
				action = Action.Fail;
			}


			// Now we run the action for the period of time.
			// If it takes longer than 20 milliseconds it gets cancelled.
			node.run(action);

			// This part does the joining. It's annoying that we have to put in a try-catch, but whatever
			try {
				node.join(period);
			} catch (InterruptedException e) {
				System.out.println("Node " + node.getNodeId() + " was interrupted.");
				e.printStackTrace();
			}

			// Here we collect the outgoing messages of the nodes.
			if (node.outgoingMsgs.size() > 0) {
				// That is, we now have a message from this node.
				Pair<Integer, String> message = node.outgoingMsgs.remove();

				if (message.x.equals(this.networkId)) {
					// if the message is sent for the network, intercept it and act
					processMessage(message.y);
				}
				else {
					addMessage(message.x, message.y);
				}
			}
		}

		deliverMessages();
	}

	private void processMessage(String m) {

		List<String> messageTokens = Arrays.asList(m.split(" "));

		switch(messageTokens.get(0)) {
			case "leader_elected":
				// write to file
				System.out.println("========== Leader elected received==========");
				try {
					BufferedWriter bw = new BufferedWriter(new FileWriter(logFileName, true));
					bw.write("Leader Node " + messageTokens.get(1) + "\n");
					bw.close();
				}
				catch (IOException e) {
					System.out.println("Couldn't write to file.");
					e.printStackTrace();
				}

				break;
			case "failed_node":
				System.out.println("===== Network finds out that node " + messageTokens.get(1) + " has failed. =====");
				Node failed = nodes.get(Integer.parseInt(messageTokens.get(1)));
				failedNodes.add(failed);
				recoverFailure(failed);
				break;
			default:
				// something else
				break;
		}
	}

	private void recoverFailure(Node failed) {
		// Find out the new path to maintain the ring.
		for (Node node : nodes.values()) {
			Node newNext;
			if (node.next.equals(failed)) {
				newNext = failed.next;
				Optional<List<Node>> recoverPath = findPath(node, newNext);
				// At this point, we want to look inside the path.
				// If it is just of length 2, we don't have to reroute anything.
				// If it is not, this is harder.
				if (recoverPath.isPresent()) {
					System.out.println("New path from " + node.getNodeId() + " to " + newNext.getNodeId() + " via ");
					for (Node np : recoverPath.get()) {
						System.out.println(np.getNodeId());
					}
				}
				else {
					System.out.println("Graph is incomplete!");
				}
			}
		}
	}

	// returns the path from a source node to a target node.
	private Optional<List<Node>> findPath(Node source, Node target) {

		Map<Integer, List<Node>> paths = new LinkedHashMap<>();
		Queue<List<Node>> queue = new LinkedList<>();

		queue.add(new LinkedList<>(Arrays.asList(source)));

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

			Set<Node> neighbours = end.myNeighbours;
			for (Node neighbour : neighbours) {
				// if this neighbour has not failed
				if (!failedNodes.contains(neighbour)) {
					List<Node> newPath = new LinkedList<>(path);
					newPath.add(neighbour);
					queue.add(newPath);
				}
			}
		}

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

   	private LinkedHashMap<Integer, Node> createNodes(List<List<Integer>> nodeInfos) {
		/* Creates the nodes from the corresponding node information.
		 *
		 */

		LinkedHashMap<Integer, Node> nodes = new LinkedHashMap<>();

		for (List<Integer> nodeInfo : nodeInfos) {
			Node node = new Node(nodeInfo.get(0), this);
			node.start();
			nodes.put(node.getNodeId(), node);
		}

		for (List<Integer> nodeInfo : nodeInfos) {
			Node toAddNeighbours = lookupNodeById(nodes, nodeInfo.get(0));
			List<Node> neighbours = nodeInfo.stream()
					                    .map(nId -> lookupNodeById(nodes, nId))
										.collect(Collectors.toList());
			neighbours.remove(0);
			addNeighbours(toAddNeighbours, neighbours);
		}

		return nodes;
	}

	private void buildRing() {
		// TODO: Rewrite this
		List<Node> nodes = new ArrayList<>(this.nodes.values());
		for (int i = 0; i < nodes.size(); i++) {
			Node prev = nodes.get((i - 1 + nodes.size()) % nodes.size());
			Node next = nodes.get((i + 1) % nodes.size());
			Node here = nodes.get(i);

			here.addNeighbour(prev);
			here.addNeighbour(next);
			here.setPrevious(prev);
			here.setNext(next);

		}
	}



	public synchronized void addMessage(int id, String m) {
		/*
		At each round, the network collects all the messages that the nodes want to send to their neighbours. 
		Implement this logic here.
		*/
		msgsToDeliver.get(id).add(m);
	}
	
	public synchronized void deliverMessages() {
		/*
		At each round, the network delivers all the messages that it has collected from the nodes.
		Implement this logic here.
		The network must ensure that a node can send only to its neighbours, one message per round per neighbour.
		*/
		// At this point, we deliver the messages of the nodes.
		for (Map.Entry<Integer, List<String>> entry : msgsToDeliver.entrySet()) {
			if (entry.getValue().size() > 0) {
				String msg = entry.getValue().get(0);
				entry.getValue().remove(0);

				nodes.get(entry.getKey()).incomingMsgs.add(msg);

				// TODO: I'm not sure if this is "synchronous" enough.
				// nodes.get(entry.getKey()).receiveMsg(msg);
			}
		}
	}
		
	public synchronized void informNodeFailure(int id) {
		/*
		Method to inform the neighbours of a failed node about the event.
		*/
	}

	private void parseGraph(String graphFileName) {
		Scanner sc = null;
		try {
			sc = new Scanner(new File(graphFileName));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		nodeInfos = new ArrayList<>();

		while (sc.hasNextLine()) {
			String line = sc.nextLine();
			List<Integer> args = Arrays.asList(line.split(" "))
					.stream()
					.map(n -> Integer.parseInt(n))
					.collect(Collectors.toList());
			nodeInfos.add(args);
		}
	}

	private void parseElection(String electionFileName)  {
		Scanner sc = null;
		electionInfos = new HashMap<>();

		try {
			sc = new Scanner(new File(electionFileName));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		while (sc.hasNextLine()) {
			String line = sc.nextLine();

			List<String> args = List.of(line.split(" "));
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
		failureInfos = new HashMap<>();

		try {
			sc = new Scanner(new File(failuresFilename));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		while (sc.hasNextLine()) {
			String line = sc.nextLine();

			List<String> args = List.of(line.split(" "));
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



	public static void main(String args[]) throws IOException, InterruptedException {
		/*
		Your main must get the input file as input.
		*/
//		List<List<Integer>> nodeArgsList = parseFile("text/ds_graph.txt");
		String filename = "text/ds_graph.txt";
		Network network = new Network();
		network.NetSimulator(filename);

//		NetSimulator("text/ds_graph.txt");
		}
	
	

	
}
