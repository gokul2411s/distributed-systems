package personal.gokul2411s.distributed_systems.paxos;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import personal.gokul2411s.distributed_systems.common.Node;
import personal.gokul2411s.distributed_systems.common.Payload;

/**
 * Represents a homogenous Paxos node playing the roles of proposer, acceptor and learner.
 */
@Log4j2
public class PaxosNode extends Node {

  // TODO: Move the following state fields into {@link PaxosNodeState} and persist them.

  /**
   * A Paxos node making proposals needs to keep track of the highest round number previously used
   * for a given instance.
   *
   * <p>This is required because either (a) two clients may concurrently contact the same
   * proposer for a given instance ID, or (b) the proposer may go down after first contact with a
   * client, and the client may reinitiate the instance with the same proposer.
   */
  private final Map<Integer, Integer> highestUsedRoundNumberForInstance = new HashMap<>();

  /**
   * A Paxos node making proposals needs to keep track of promises made by other Paxos nodes in
   * order to find out if there is a majority interest in proceeding with making proposals for a
   * given instance and round.
   */
  private final Table<Integer, Integer, List<PromiseMessage>> promiseMessagesForInstanceForRound =
      HashBasedTable.create();

  /**
   * A Paxos node making proposals needs to keep track of the proposals it makes for a given
   * instance and round.
   */
  private final Table<Integer, Integer, String> proposalForInstanceForRound =
      HashBasedTable.create();

  /**
   * A Paxos node making proposals needs to keep track of the proposals accepted (and by which node)
   * for a given instance.
   */
  private final Map<Integer, List<ProposalAcceptanceMessage>> proposalAcceptancesForInstance =
      new HashMap<>();

  /**
   * A Paxos node making proposals needs to keep track of chosen proposals for a given instance.
   * i.e. those proposals that have been accepted by a majority of nodes.
   */
  private final Map<Integer, String> chosenProposalForInstance = new HashMap<>();

  /**
   * A Paxos node accepting proposals needs to be able to promise that it won't support (promise
   * against or accept) any proposals below a certain round number for a given instance.
   */
  private final Map<Integer, RoundIdentifier> lowestRoundToSupportForInstance = new HashMap<>();

  /**
   * A Paxos node accepting proposals needs to be able to provide information to proposer nodes
   * about the highest round numbered proposal that the accepting node has accepted in the past, for
   * a given instance.
   */
  private final Map<Integer, ProposalMessage> highestAcceptedProposalMessageForInstance = new HashMap<>();

  /**
   * All Paxos nodes need to be configured with some information about other Paxos nodes. This
   * reconfiguration could technically be implemented via Paxos itself, but we simplify this
   * by using a hardcoded configuration object.
   */
  private final Discovery discovery;

  public PaxosNode(int id, int networkBufferSize, Discovery discovery) {
    super(id, networkBufferSize);
    this.discovery = discovery;
  }

  @Override
  public List<Payload> process(Payload payload) {

    int senderId = payload.getSenderId();
    Object rawMessage = payload.getMessage();
    Class rawMessageClass = rawMessage.getClass();
    if (false) {
    } else if (rawMessageClass.equals(InitMessage.class)) {
      return handleMessage((InitMessage) rawMessage, senderId);
    } else if (rawMessageClass.equals(PromiseElicitationMessage.class)) {
      return handleMessage((PromiseElicitationMessage) rawMessage, senderId);
    } else if (rawMessageClass.equals(PromiseMessage.class)) {
      return handleMessage((PromiseMessage) rawMessage, senderId);
    } else if (rawMessageClass.equals(ProposalMessage.class)) {
      return handleMessage((ProposalMessage) rawMessage, senderId);
    } else if (rawMessageClass.equals(ProposalAcceptanceMessage.class)) {
      return handleMessage((ProposalAcceptanceMessage) rawMessage, senderId);
    }

    return new ArrayList<>();
  }

  private List<Payload> handleMessage(
      InitMessage message, int nodeId) {

    int instanceId = message.getInstanceId();

    // We might have already created rounds for this instance ID in the past. We must
    // create a new round now.
    Integer previousRoundNumber = highestUsedRoundNumberForInstance.get(instanceId);
    int roundNumber = previousRoundNumber != null ? previousRoundNumber + 1 : 0;
    highestUsedRoundNumberForInstance.put(message.getInstanceId(), roundNumber);
    RoundIdentifier roundIdentifier = new RoundIdentifier(roundNumber, id());

    // Note down proposal value for use when making proposals.
    proposalForInstanceForRound.put(instanceId, roundNumber, message.getProposal());

    // Send the promise elicitation message to all Paxos nodes (i.e. initiate Paxos
    // consensus for this instance ID).
    PromiseElicitationMessage promiseElicitationMessage =
        new PromiseElicitationMessage(instanceId, roundIdentifier);
    return discovery.allNodes().stream()
        .map(
            destinationNodeId ->
                Payload.builder()
                    .senderId(id())
                    .destinationId(destinationNodeId)
                    .message(promiseElicitationMessage)
                    .build())
        .collect(Collectors.toList());
  }

  private List<Payload> handleMessage(
      PromiseElicitationMessage message, int nodeId) {

    int instanceId = message.getInstanceId();
    RoundIdentifier roundId = message.getRound();

    // If we have promised not to support this round in this instance,
    // there is nothing to do.
    RoundIdentifier lowestRoundToSupport = lowestRoundToSupportForInstance.get(instanceId);
    if (lowestRoundToSupport != null && roundId.compareTo(lowestRoundToSupport) < 0) {
      return new ArrayList<>();
    }

    // Else, promise to support no smaller round for this instance.
    lowestRoundToSupportForInstance.put(instanceId, roundId);

    // Return back the most recently accepted proposal in this instance as part of the
    // promise back to the proposer Paxos node.
    PromiseMessage.PromiseMessageBuilder promiseMessageBuilder =
        PromiseMessage.builder()
            .instanceId(instanceId)
            .round(roundId);
    ProposalMessage highestAcceptedProposalMessage =
        highestAcceptedProposalMessageForInstance.get(instanceId);
    if (highestAcceptedProposalMessage != null) {
      promiseMessageBuilder
          .acceptedProposal(highestAcceptedProposalMessage.getProposal())
          .roundInWhichAccepted(highestAcceptedProposalMessage.getRound());
    }

    List<Payload> output = new ArrayList<>();
    output.add(
        Payload.builder()
            .senderId(id())
            .destinationId(nodeId)
            .message(promiseMessageBuilder.build())
            .build());
    return output;
  }

  private List<Payload> handleMessage(
      PromiseMessage message, int nodeId) {

    int instanceId = message.getInstanceId();
    RoundIdentifier roundId = message.getRound();
    int roundNumber = roundId.getRoundNumber();

    // Add promise to the list of promises obtained in this instance for this round.
    List<PromiseMessage> existingPromises =
        promiseMessagesForInstanceForRound.get(instanceId, roundNumber);
    if (existingPromises == null) {
      existingPromises = new ArrayList<>();
      promiseMessagesForInstanceForRound.put(instanceId, roundNumber, existingPromises);
    }
    existingPromises.add(message);

    // If received promises from a majority for this instance, then find out the highest round
    // proposal accepted by some Paxos node.
    String proposal = null;
    boolean foundMajority = false;
    if (existingPromises.size() > discovery.totalNodes() / 2) {
      foundMajority = true;
      RoundIdentifier highestRoundInWhichSomeAcceptorAccepted = null;
      for (PromiseMessage promiseMessage : existingPromises) {
        RoundIdentifier roundInWhichAcceptorAccepted = promiseMessage.getRoundInWhichAccepted();
        if (roundInWhichAcceptorAccepted == null) {
          continue;
        }
        if (highestRoundInWhichSomeAcceptorAccepted == null
            || roundInWhichAcceptorAccepted.compareTo(highestRoundInWhichSomeAcceptorAccepted)
            > 0) {
          proposal = promiseMessage.getAcceptedProposal();
          highestRoundInWhichSomeAcceptorAccepted = roundInWhichAcceptorAccepted;
        }
      }
    }

    if (!foundMajority) {
      // Of course, if a response is not obtained from a majority ever for a given round,
      // the proposer Paxos node won't make any proposals in that round.
      return new ArrayList<>();
    }

    // If none of the Paxos nodes have accepted a proposal, then in this instance and round,
    // we can propose what the client wanted.
    if (proposal == null) {
      proposal = proposalForInstanceForRound.get(instanceId, roundNumber);
    }

    ProposalMessage proposalMessage = new ProposalMessage(instanceId, roundId, proposal);
    return discovery.allNodes().stream()
        .map(
            destinationNodeId ->
                Payload.builder()
                    .senderId(id())
                    .destinationId(destinationNodeId)
                    .message(proposalMessage)
                    .build())
        .collect(Collectors.toList());
  }

  private List<Payload> handleMessage(
      ProposalMessage message, int nodeId) {

    int instanceId = message.getInstanceId();
    RoundIdentifier roundId = message.getRound();

    // If we have promised not to support this round in this instance,
    // there is nothing to do.
    RoundIdentifier lowestRoundToSupport = lowestRoundToSupportForInstance.get(instanceId);
    if (lowestRoundToSupport != null && roundId.compareTo(lowestRoundToSupport) < 0) {
      return new ArrayList<>();
    }

    // Else, let's accept the proposal.
    ProposalMessage highestAcceptedProposalMessage =
        highestAcceptedProposalMessageForInstance.get(instanceId);
    if (highestAcceptedProposalMessage == null
        || roundId.compareTo(highestAcceptedProposalMessage.getRound()) > 0) {
      highestAcceptedProposalMessageForInstance.put(instanceId, message);
    }

    // Send the acceptance to the proposer Paxos node.
    ProposalAcceptanceMessage proposalAcceptanceMessage =
        new ProposalAcceptanceMessage(instanceId, id(), message.getProposal());
    List<Payload> output = new ArrayList<>();
    output.add(
        Payload.builder()
            .senderId(id())
            .destinationId(nodeId)
            .message(proposalAcceptanceMessage)
            .build());
    return output;
  }

  private List<Payload> handleMessage(
      ProposalAcceptanceMessage message, int nodeId) {

    int instanceId = message.getInstanceId();

    // If a proposal is already chosen (known to be accepted by a majority of Paxos nodes)
    // in this instance, then there is nothing to do.
    if (chosenProposalForInstance.get(instanceId) != null) {
      return new ArrayList<>();
    }

    // Store proposal acceptance.
    List<ProposalAcceptanceMessage> existingAcceptances =
        proposalAcceptancesForInstance.get(instanceId);
    if (existingAcceptances == null) {
      existingAcceptances = new ArrayList<>();
      proposalAcceptancesForInstance.put(instanceId, existingAcceptances);
    }
    existingAcceptances.add(message);

    // If found acceptance for a proposal in an instance from a majority of acceptors, then
    // consder the proposal as chosen.
    long acceptanceCount =
        existingAcceptances.stream()
            .filter(a -> a.getProposal().equals(message.getProposal()))
            .map(a -> a.getAcceptorId())
            .distinct()
            .count();
    if (acceptanceCount > discovery.totalNodes() / 2) {
      chosenProposalForInstance.put(instanceId, message.getProposal());
    }

    return new ArrayList<>();
  }
}
