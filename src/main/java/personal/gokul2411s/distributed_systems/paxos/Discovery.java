package personal.gokul2411s.distributed_systems.paxos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import personal.gokul2411s.distributed_systems.common.Network;

/**
 * Represents the service / node discovery function for Paxos execution.
 *
 * <p>For sake of simplicity, we don't rely on the {@link Network} for discovery.
 */
@Log4j2
public class Discovery {

  private final Map<Integer, PaxosNode> nodes = new HashMap<>();

  public void registerNewNode(PaxosNode node) {
    synchronized (nodes) {
      nodes.put(node.id(), node);
    }
    log.info("Made new node with ID {} discoverable", node.id());
  }

  public void deregisterNode(int id) {
    synchronized (nodes) {
      nodes.remove(id);
    }
    log.info("Made node with ID {} undiscoverable", id);
  }

  public PaxosNode get(int id) {
    synchronized (nodes) {
      return nodes.get(id);
    }
  }

  /**
   * Represents the total number of desired nodes in the Paxos cluster.
   *
   * <p>The number of registered nodes is expected to be no greater than this.
   */
  private final int totalNodes;

  public int totalNodes() {
    return totalNodes;
  }

  Discovery(int totalNodes) {
    this.totalNodes = totalNodes;
  }

  public List<Integer> allNodes() {
    List<Integer> output = new ArrayList<>();
    synchronized (nodes) {
      output.addAll(nodes.keySet().stream().collect(Collectors.toList()));
    }
    return output;
  }
}
