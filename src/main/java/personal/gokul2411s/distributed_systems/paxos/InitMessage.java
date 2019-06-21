package personal.gokul2411s.distributed_systems.paxos;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;

/**
 * Message from a client (a non {@link PaxosNode}) to a proposer to initiate making proposals.
 *
 * <p>The {@link #instanceId} allows clients to initiate multiple Paxos instances
 * in parallel.
 */
@Value
@RequiredArgsConstructor
public class InitMessage {

  private final int instanceId;

  @NonNull
  private final String proposal;

  @Override
  public String toString() {
    return String.format(
        "Instance = %d, Initializing Paxos with proposal (%s)", instanceId, proposal);
  }
}
