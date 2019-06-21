package personal.gokul2411s.distributed_systems.paxos;

import java.util.Scanner;
import personal.gokul2411s.distributed_systems.common.Network;
import personal.gokul2411s.distributed_systems.common.Node;
import personal.gokul2411s.distributed_systems.common.Payload;

public class Simulation extends Node {

  private static final int SIMULATION_NODE_ID = 0;
  private static final double NETWORK_LOSS_RATIO = 0.01;
  private static final int NETWORK_BUFFER_SIZE = 100;
  private static final int NUM_PAXOS_NODES = 3;

  public static void main(String[] args) {

    System.out.println("Starting Paxos simulation...");

    Discovery discovery = new Discovery(NUM_PAXOS_NODES);
    Network network = new Network(NETWORK_LOSS_RATIO);

    for (int nodeId = 1; nodeId <= NUM_PAXOS_NODES; nodeId++) {
      PaxosNode paxosNode = new PaxosNode(nodeId, NETWORK_BUFFER_SIZE, discovery);
      discovery.registerNewNode(paxosNode);
      paxosNode.start();
      network.registerNewNode(paxosNode);
    }

    Simulation simulation = new Simulation(discovery, network);
    network.registerNewNode(simulation);

    network.start();
    simulation.start();
  }

  private final Discovery discovery;
  private final Network network;

  public Simulation(Discovery discovery, Network network) {
    super(SIMULATION_NODE_ID, NETWORK_BUFFER_SIZE);
    this.discovery = discovery;
    this.network = network;
  }

  @Override
  public void run() {

    Scanner input = new Scanner(System.in);
    while (input.hasNext()) {
      String line = input.nextLine();

      // Possible commands.
      // INIT <instance_id> <node_id> <multi-word proposal value>
      // KILL <node_id>
      // RESTART <node_id>

      if (false) {
      } else if (line.startsWith("INIT")) {
        String[] splitLine = line.split(" ", 4);
        int instanceId = Integer.valueOf(splitLine[1]);
        int nodeId = Integer.valueOf(splitLine[2]);
        String proposal = splitLine[3];

        InitMessage initMessage = new InitMessage(instanceId, proposal);
        Payload payload =
            Payload.builder()
                .senderId(id())
                .destinationId(nodeId)
                .message(initMessage)
                .build();
        attemptEnqueueOutgoing(payload);
      } else if (line.startsWith("KILL")) {
        String[] splitLine = line.split(" ", 2);
        int nodeId = Integer.valueOf(splitLine[1]);
        PaxosNode paxosNode = discovery.get(nodeId);
        if (paxosNode != null) {
          paxosNode.kill();
          discovery.deregisterNode(nodeId);
          network.deregisterNode(nodeId);
        }
      } else if (line.startsWith("RESTART")) {
        String[] splitLine = line.split(" ", 2);
        int nodeId = Integer.valueOf(splitLine[1]);
        PaxosNode paxosNode = discovery.get(nodeId);
        if (paxosNode == null) {
          paxosNode = new PaxosNode(nodeId, NETWORK_BUFFER_SIZE, discovery);
          discovery.registerNewNode(paxosNode);
          paxosNode.start();
          network.registerNewNode(paxosNode);
        }
      }
    }
  }
}
