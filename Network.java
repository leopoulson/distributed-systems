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
	private int round;
	private int period = 20;
	private Map<Integer, List<String>> msgsToDeliver; //Integer for the id of the sender and String for the message

	private List<List<Integer>> nodeInfos;
	private HashMap<Integer, List<Integer>> electionInfos;
	private HashMap<Integer, Integer> failureInfos;
	
	public void NetSimulator(String fileName) throws IOException {
		/*
		Code to call methods for parsing the input file, initiating the system and producing the log can be added here.
		*/

		parseGraph(fileName);
		parseElection("text/ds_elect.txt");
		parseFailures("text/ds_fail.txt");

		nodes = createNodes(nodeInfos);
		buildRing();

		msgsToDeliver = new HashMap<Integer, List<String>>();
		for (Integer i : nodes.keySet()) {
			msgsToDeliver.put(i, new LinkedList<>());
		}


		for (int round = 50; round < 201; round++) {
			 System.out.println("\n\nRound " + round + ".\n");

			for (Node node : nodes.values()) {
				// Pick what to tell the node to do, based on if there's an election or not.
				Action action = Action.None;
				if (electionInfos.keySet().contains(round) && electionInfos.get(round).contains(node.getNodeId()))
					action = Action.StartElection;

				if (failureInfos.getOrDefault(round, -1).equals(node.getNodeId())) {
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
				if (node.outgoingMsg.isPresent()) {
					// That is, we now have a message from this node.
					Pair<Integer, String> message = node.outgoingMsg.get();
					System.out.println(message.x + "  " + message.y);
					msgsToDeliver.get(message.x).add(message.y);
				}
			}

			// At this point, we deliver the messages of the nodes.
			for (Map.Entry<Integer, List<String>> entry : msgsToDeliver.entrySet()) {
				if (entry.getValue().size() > 0) {
					String msg = entry.getValue().get(0);
					entry.getValue().remove(0);

					nodes.get(entry.getKey()).incomingMsg.add(msg);

					// TODO: I'm not sure if this is "synchronous" enough.
					// nodes.get(entry.getKey()).receiveMsg(msg);
				}
			}
 		}
	}

	private void elect(int round) {
		if (electionInfos.containsKey(round)){
			List<Integer> electors = electionInfos.get(round);
			for (Integer nodeId : electors) {
				nodes.get(nodeId).startElection();
			}
		}
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

		LinkedHashMap<Integer, Node> nodes = new LinkedHashMap<Integer, Node>();

		for (List<Integer> nodeInfo : nodeInfos) {
			Node node = new Node(nodeInfo.get(0));
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

	// Constructs the ring overlay on the network.
	// Currently, it's just in the order in which nodes were constructed.
	// This might have to change in the future?
	private void buildRing() {
		// TODO: Rewrite this
		List<Node> nodes = new ArrayList<>(this.nodes.values());
		for (int i = 0; i < nodes.size(); i++) {
			nodes.get(i).setPrevious(nodes.get((i - 1 + nodes.size()) % nodes.size()));
			nodes.get(i).setNext(nodes.get((i + 1) % nodes.size()));
		}
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



	public synchronized void addMessage(int id, String m) {
		/*
		At each round, the network collects all the messages that the nodes want to send to their neighbours. 
		Implement this logic here.
		*/
		}
	
	public synchronized void deliverMessages() {
		/*
		At each round, the network delivers all the messages that it has collected from the nodes.
		Implement this logic here.
		The network must ensure that a node can send only to its neighbours, one message per round per neighbour.
		*/
		}
		
	public synchronized void informNodeFailure(int id) {
		/*
		Method to inform the neighbours of a failed node about the event.
		*/
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
