package personal.gokul2411s.distributed_systems.common;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class Payload {

  private final int senderId;

  private final int destinationId;

  @NonNull
  private final Object message;

  @Override
  public String toString() {
    return String.format("(%s) from node %d to %d", message.toString(), senderId, destinationId);
  }
}
