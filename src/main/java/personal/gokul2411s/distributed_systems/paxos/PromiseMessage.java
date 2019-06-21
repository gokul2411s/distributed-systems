package personal.gokul2411s.distributed_systems.paxos;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Message sent by acceptors back to proposers indicating that they promise not to accept any
 * proposal for rounds less than {@link #round}.
 *
 * <p>The message also contains (a) the proposal accepted by this acceptor
 * in the highest round it has witnessed, and (b) that round.
 */
@Value
@Builder
public class PromiseMessage {

  /**
   * The original instance ID specified by the client.
   */
  private final int instanceId;

  @NonNull
  private final RoundIdentifier round;

  private final String acceptedProposal;

  private final RoundIdentifier roundInWhichAccepted;

  @Override
  public String toString() {
    return String.format(
        "Instance = %d, Promising in round %s%s",
        instanceId,
        round.toString(),
        acceptedProposal != null
            ? String.format(
            ". Accepted (%s) in round %s", acceptedProposal, roundInWhichAccepted.toString())
            : "");
  }
}
