package personal.gokul2411s.distributed_systems.common;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Represents a computing machine that reads payloads destined for self and produces payloads
 * destined for other nodes (including self).
 */
@Log4j2
public abstract class Node extends Thread {

  private static final Logger BLUE_LOGGER =
      LogManager.getLogger("personal.gokul2411s.distributed_systems.BlueLogger");

  /**
   * An identification for the node. No two nodes should have the same id.
   */
  private final int id;

  public int id() {
    return id;
  }

  /**
   * A queue of payloads for the node to consume.
   *
   * <p>It is expected that the network will independently attemptEnqueue payloads into the
   * queue using {@link #attemptEnqueue(Payload)}.
   *
   * <p>It is also possible for the node to send a payload to itself. In this special case
   * alone, the payload directly goes into this queue without traversing the network.
   *
   * <p>The incoming payload queue exists only so long as the node exists.
   */
  private final BlockingQueue<Payload> incomingPayloadQueue;

  /**
   * Attempts enqueuing a payload into the node's incoming payload queue.
   *
   * <p>This does not guarantee that the payload will make it into the queue,
   * because the queue may be full.
   */
  public void attemptEnqueue(Payload payload) {
    if (incomingPayloadQueue.offer(payload)) {
      log.info("Delivered payload {}", payload.toString());
    } else {
      log.error("Lost payload {} in network buffer", payload.toString());
    }
  }

  /**
   * A queue of payloads that the node produces.
   *
   * <p>It is expected that the network consumes payloads from this queue using
   * {@link #attemptDequeue()} and sends it to the appropriate destination node.
   *
   * <p>The outgoing payload queue exists only so long as the node exists.
   */
  private final BlockingQueue<Payload> outgoingPayloadQueue;

  /**
   * Attempts dequeueing a payload from the node's outgoing payload queue.
   *
   * <p>This does not guarantee that a payload will be dequeued, because the queue
   * may be empty.
   */
  public Payload attemptDequeue() {
    return outgoingPayloadQueue.poll();
  }

  /**
   * Attempts enqueueing a payload into the node's outgoing payload queue.
   *
   * <p>Only a node can enqueue into its outgoing payload queue.
   */
  protected void attemptEnqueueOutgoing(Payload payload) {
    if (outgoingPayloadQueue.offer(payload)) {
      BLUE_LOGGER.info("Buffered payload {}", payload.toString());
    } else {
      log.error("Lost payload {} in network buffer", payload.toString());
    }
  }

  /**
   * Kills the node.
   *
   * <p>This is not guaranteed to complete when the method returns. But the caller
   * can expect the node to perform no further action after it is done with the current action.
   */
  public void kill() {
    interrupt();
  }

  public Node(int id, int networkBufferSize) {
    super();
    this.id = id;
    this.incomingPayloadQueue = new ArrayBlockingQueue<>(networkBufferSize);
    this.outgoingPayloadQueue = new ArrayBlockingQueue<>(networkBufferSize);
  }

  @Override
  public void run() {

    while (true) {
      try {
        Payload inputPayload = incomingPayloadQueue.take();
        for (Payload outputPayload : process(inputPayload)) {
          if (id == outputPayload.getDestinationId()) {
            // It is not strictly necessary to do this, but we are trying our best to simulate
            // the behavior of sending messages to 'localhost', which do not go through the
            // network.
            attemptEnqueue(outputPayload);
          } else {
            attemptEnqueueOutgoing(outputPayload);
          }
        }
      } catch (InterruptedException e) {
        interrupt();
        break;
      }
    }

    // Cleanup.
    incomingPayloadQueue.clear();
    outgoingPayloadQueue.clear();
  }

  /**
   * Represents the crux of what a node does. It takes an incoming payload and produces zero or more
   * outgoing payloads.
   */
  public List<Payload> process(Payload payload) {
    throw new IllegalStateException("Invalid call");
  }
}
