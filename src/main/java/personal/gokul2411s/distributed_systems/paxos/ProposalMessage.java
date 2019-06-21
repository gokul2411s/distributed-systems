package personal.gokul2411s.distributed_systems.paxos;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;

/**
 * Message that proposers send to acceptors to make a proposal.
 */
@Value
@RequiredArgsConstructor
public class ProposalMessage {

  /**
   * The original instance ID specified by the client.
   */
  private final int instanceId;

  @NonNull
  private final RoundIdentifier round;

  @NonNull
  private final String proposal;

  @Override
  public String toString() {
    return String.format(
        "Instance = %d, Proposing (%s) in round %s", instanceId, proposal, round.toString());
  }
}
