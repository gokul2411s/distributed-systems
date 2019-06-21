package personal.gokul2411s.distributed_systems.paxos;

import lombok.AllArgsConstructor;
import lombok.Value;

/**
 * Unique identifier for a round with the {@link #nodeId} to act as tie breaker.
 */
@Value
@AllArgsConstructor
public class RoundIdentifier implements Comparable<RoundIdentifier> {

  private final int roundNumber;

  private final int nodeId;

  @Override
  public int compareTo(RoundIdentifier o) {
    int x = Integer.compare(roundNumber, o.roundNumber);
    if (x != 0) {
      return x;
    } else {
      return Integer.compare(nodeId, o.nodeId);
    }
  }

  @Override
  public String toString() {
    return String.format("(%d, %d)", roundNumber, nodeId);
  }
}
