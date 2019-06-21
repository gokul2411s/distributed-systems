package personal.gokul2411s.distributed_systems.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import lombok.extern.log4j.Log4j2;

/**
 * Simulates the universe of all networking functionality among a set of nodes.
 *
 * <p>A {@link Node} should only belong to a single network. For simulating multiple
 *    network interface cards on a single host, use multiple {@link Node} objects with
 *    possibly shared state.
 */
@Log4j2
public class Network extends Thread {

  private final Random random = new Random();

  private final double lossRatio;

  public Network(double lossRatio) {
    this.lossRatio = lossRatio;
  }

  private final Map<Integer, Node> nodes = new HashMap<>();

  public void registerNewNode(Node node) {
    synchronized (nodes) {
      nodes.put(node.id(), node);
    }
    log.info("Registered new node with ID {} to network", node.id());
  }

  public void deregisterNode(int id) {
    synchronized (nodes) {
      nodes.remove(id);
    }
    log.info("Deregistered node with ID {} from network", id);
  }

  @Override
  public void run() {

    while (true) {
      try {
        // 1] Simulate picking a node from which to read output payload buffer.
        Node sourceNode = null;
        synchronized (nodes) {
          Object[] nodeIds = nodes.keySet().toArray();
          if (nodeIds.length > 0) {
            sourceNode = nodes.get(nodeIds[random.nextInt(nodeIds.length)]);
          }
        }

        if (sourceNode == null) {
          Thread.sleep(1000);
          continue;
        }

        // 2] Read the output payload buffer of the node. Neglect if there are
        //    no payloads there.
        Payload payload = sourceNode.attemptDequeue();
        if (payload == null) {
          Thread.sleep(1);
          continue;
        }

        // 3] Decide if we want to simulate losing the payload on the network.
        if (random.nextDouble() < lossRatio) {
          log.error("Lost payload {} in network", payload.toString());
          Thread.sleep(1);
          continue;
        }

        // 4] Send payload to the destination node's input buffer.
        int destinationNodeId = payload.getDestinationId();
        synchronized (nodes) {
          Node destinationNode = nodes.get(destinationNodeId);
          if (destinationNode != null) {
            destinationNode.attemptEnqueue(payload);
          }
        }
      } catch (InterruptedException e) {
        interrupt();
        break;
      }
    }
  }
}
