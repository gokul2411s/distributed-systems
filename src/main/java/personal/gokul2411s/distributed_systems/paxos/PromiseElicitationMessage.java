package personal.gokul2411s.distributed_systems.paxos;

import lombok.AllArgsConstructor;
import lombok.Value;

/**
 * Message sent by proposers expecting acceptor nodes to promise that they will not accept any
 * proposal for rounds less than {@link #round}.
 */
@Value
@AllArgsConstructor
public class PromiseElicitationMessage {

  /**
   * The original instance ID specified by the client.
   */
  private final int instanceId;

  private final RoundIdentifier round;

  @Override
  public String toString() {
    return String.format(
        "Instance = %d, Requesting promise for round %s", instanceId, round.toString());
  }
}
