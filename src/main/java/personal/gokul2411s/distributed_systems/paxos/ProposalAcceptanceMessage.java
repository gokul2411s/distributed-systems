package personal.gokul2411s.distributed_systems.paxos;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;

/**
 * Messages that acceptors to send to learners to indicate acceptance of a proposal.
 */
@Value
@RequiredArgsConstructor
public class ProposalAcceptanceMessage {

  /**
   * The original instance ID specified by the client.
   */
  private final int instanceId;

  private final int acceptorId;

  @NonNull
  private final String proposal;

  @Override
  public String toString() {
    return String.format(
        "Instance = %d, Accepting (%s)", instanceId, proposal);
  }
}
