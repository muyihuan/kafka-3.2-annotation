/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.raft;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is responsible for managing the current state of this node and ensuring
 * only valid state transitions. Below we define the possible state transitions and
 * how they are triggered:
 *
 * Unattached|Resigned transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Voted: After granting a vote to a candidate
 *    Candidate: After expiration of the election timeout
 *    Follower: After discovering a leader with an equal or larger epoch
 *
 * Voted transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Candidate: After expiration of the election timeout
 *
 * Candidate transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Candidate: After expiration of the election timeout
 *    Leader: After receiving a majority of votes
 *
 * Leader transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Resigned: When shutting down gracefully
 *
 * Follower transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Candidate: After expiration of the fetch timeout
 *    Follower: After discovering a leader with a larger epoch
 *
 * Observers follow a simpler state machine. The Voted/Candidate/Leader/Resigned
 * states are not possible for observers, so the only transitions that are possible
 * are between Unattached and Follower.
 *
 * Unattached transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Follower: After discovering a leader with an equal or larger epoch
 *
 * Follower transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Follower: After discovering a leader with a larger epoch
 *
 */
/**
 *此类负责管理此节点的当前状态，并确保只有有效的状态转换。下面我们定义了可能的状态转换及其触发方式：
 *
 *独立|辞职过渡到：
 *独立：在了解到一次新的、更高时代的选举之后
 *投票：在给候选人投票后：在选举超时结束后：在发现具有相同或更大时代的领导人后
 *
 *投票过渡到：
 *独立：在了解到一次新的、更高时代的选举之后
 *候选人：选举超时结束后
 *
 *候选人过渡到：
 *独立：在了解到一次新的、更高时代的选举之后
 *候选人：选举超时结束后
 *领导人：获得多数票后
 *
 *领导者过渡到：
 *独立：在了解到一次新的、更高时代的选举之后
 *辞职：优雅地关机时
 *
 *跟随者过渡到：
 *独立：在了解到一次新的、更高时代的选举之后
 *候选：在提取超时过期之后
 *跟随者：在发现一个具有更大时代的领导者之后
 *
 *观察者遵循更简单的状态机。投票人/候选人/领导人/辞职
 *状态对于观察者来说是不可能的，因此只有可能的转换
 *介于未连接和跟随之间。
 *
 *未连接的过渡到：
 *独立：在了解到一次新的、更高时代的选举之后
 *跟随者：在发现具有相同或更大时代的领导者之后
 *
 *跟随者过渡到：
 *独立：在了解到一次新的、更高时代的选举之后
 *跟随者：在发现一个具有更大时代的领导者之后
 *
 */
public class QuorumState {
    private final OptionalInt localId;
    private final Time time;
    private final Logger log;
    private final QuorumStateStore store;
    private final Set<Integer> voters;
    private final Random random;
    private final int electionTimeoutMs;
    private final int fetchTimeoutMs;
    private final LogContext logContext;

    // 角色状态
    private volatile EpochState state;

    public QuorumState(OptionalInt localId,
                       Set<Integer> voters,
                       int electionTimeoutMs,
                       int fetchTimeoutMs,
                       QuorumStateStore store,
                       Time time,
                       LogContext logContext,
                       Random random) {
        this.localId = localId;
        this.voters = new HashSet<>(voters);
        this.electionTimeoutMs = electionTimeoutMs;
        this.fetchTimeoutMs = fetchTimeoutMs;
        this.store = store;
        this.time = time;
        this.log = logContext.logger(QuorumState.class);
        this.random = random;
        this.logContext = logContext;
    }

    public void initialize(OffsetAndEpoch logEndOffsetAndEpoch) throws IllegalStateException {
        // We initialize in whatever state we were in on shutdown. If we were a leader
        // or candidate, probably an election was held, but we will find out about it
        // when we send Vote or BeginEpoch requests.

        // 读取存储信息
        ElectionState election;
        try {
            election = store.readElectionState();
            if (election == null) {
                election = ElectionState.withUnknownLeader(0, voters);
            }
        } catch (final UncheckedIOException e) {
            // For exceptions during state file loading (missing or not readable),
            // we could assume the file is corrupted already and should be cleaned up.
            log.warn("Clearing local quorum state store after error loading state {}",
                store.toString(), e);
            store.clear();
            election = ElectionState.withUnknownLeader(0, voters);
        }

        // 初始化角色状态
        final EpochState initialState;
        // 投票人列表和之前存储的投票人列表不一致，抛异常启动失败
        if (!election.voters().isEmpty() && !voters.equals(election.voters())) {
            throw new IllegalStateException("Configured voter set: " + voters
                + " is different from the voter set read from the state file: " + election.voters()
                + ". Check if the quorum configuration is up to date, "
                + "or wipe out the local state file if necessary");
        }
        // 之前参与投票 本次不参与投票了 ，抛异常启动失败
        else if (election.hasVoted() && !isVoter()) {
            String localIdDescription = localId.isPresent() ?
                localId.getAsInt() + " is not a voter" :
                "is undefined";
            throw new IllegalStateException("Initialized quorum state " + election
                + " with a voted candidate, which indicates this node was previously "
                + " a voter, but the local id " + localIdDescription);
        }
        // 纪元不匹配，设置为可以投票但是未参与
        else if (election.epoch < logEndOffsetAndEpoch.epoch) {
            log.warn("Epoch from quorum-state file is {}, which is " +
                "smaller than last written epoch {} in the log",
                election.epoch, logEndOffsetAndEpoch.epoch);
            initialState = new UnattachedState(
                time,
                logEndOffsetAndEpoch.epoch,
                voters,
                Optional.empty(),
                randomElectionTimeoutMs(),
                logContext
            );
        }
        // 如果之前是领导者，那么设置为卸任状态 告知新的选举
        else if (localId.isPresent() && election.isLeader(localId.getAsInt())) {
            // If we were previously a leader, then we will start out as resigned
            // in the same epoch. This serves two purposes:
            // 1. It ensures that we cannot vote for another leader in the same epoch.
            // 2. It protects the invariant that each record is uniquely identified by
            //    offset and epoch, which might otherwise be violated if unflushed data
            //    is lost after restarting.
            initialState = new ResignedState(
                time,
                localId.getAsInt(),
                election.epoch,
                voters,
                randomElectionTimeoutMs(),
                Collections.emptyList(),
                logContext
            );
        }
        // 如果之前是参与者，继续为参与者
        else if (localId.isPresent() && election.isVotedCandidate(localId.getAsInt())) {
            initialState = new CandidateState(
                time,
                localId.getAsInt(),
                election.epoch,
                voters,
                Optional.empty(),
                1,
                randomElectionTimeoutMs(),
                logContext
            );
        }
        // 如果之前正在参与投票
        else if (election.hasVoted()) {
            initialState = new VotedState(
                time,
                election.epoch,
                election.votedId(),
                voters,
                Optional.empty(),
                randomElectionTimeoutMs(),
                logContext
            );
        }
        // 如果之前有领导者，那么设置为追随者
        else if (election.hasLeader()) {
            initialState = new FollowerState(
                time,
                election.epoch,
                election.leaderId(),
                voters,
                Optional.empty(),
                fetchTimeoutMs,
                logContext
            );
        }
        // 设置为可以投票但是未参与，最原始的状态
        else {
            initialState = new UnattachedState(
                time,
                election.epoch,
                voters,
                Optional.empty(),
                randomElectionTimeoutMs(),
                logContext
            );
        }

        // 转化为初始状态
        transitionTo(initialState);
    }

    public Set<Integer> remoteVoters() {
        return voters.stream().filter(voterId -> voterId != localIdOrSentinel()).collect(Collectors.toSet());
    }

    public int localIdOrSentinel() {
        return localId.orElse(-1);
    }

    public int localIdOrThrow() {
        return localId.orElseThrow(() -> new IllegalStateException("Required local id is not present"));
    }

    public OptionalInt localId() {
        return localId;
    }

    public int epoch() {
        return state.epoch();
    }

    public int leaderIdOrSentinel() {
        return leaderId().orElse(-1);
    }

    public Optional<LogOffsetMetadata> highWatermark() {
        return state.highWatermark();
    }

    public OptionalInt leaderId() {

        ElectionState election = state.election();
        if (election.hasLeader())
            return OptionalInt.of(state.election().leaderId());
        else
            return OptionalInt.empty();
    }

    public boolean hasLeader() {
        return leaderId().isPresent();
    }

    public boolean hasRemoteLeader() {
        return hasLeader() && leaderIdOrSentinel() != localIdOrSentinel();
    }

    public boolean isVoter() {
        return localId.isPresent() && voters.contains(localId.getAsInt());
    }

    public boolean isVoter(int nodeId) {
        return voters.contains(nodeId);
    }

    public boolean isObserver() {
        return !isVoter();
    }

    public void transitionToResigned(List<Integer> preferredSuccessors) {
        if (!isLeader()) {
            throw new IllegalStateException("Invalid transition to Resigned state from " + state);
        }

        // The Resigned state is a soft state which does not need to be persisted.
        // A leader will always be re-initialized in this state.
        int epoch = state.epoch();
        this.state = new ResignedState(
            time,
            localIdOrThrow(),
            epoch,
            voters,
            randomElectionTimeoutMs(),
            preferredSuccessors,
            logContext
        );
        log.info("Completed transition to {}", state);
    }

    /**
     * Transition to the "unattached" state. This means we have found an epoch greater than
     * or equal to the current epoch, but wo do not yet know of the elected leader.
     */
    public void transitionToUnattached(int epoch) {
        int currentEpoch = state.epoch();
        if (epoch <= currentEpoch) {
            throw new IllegalStateException("Cannot transition to Unattached with epoch= " + epoch +
                " from current state " + state);
        }

        final long electionTimeoutMs;
        if (isObserver()) {
            electionTimeoutMs = Long.MAX_VALUE;
        } else if (isCandidate()) {
            electionTimeoutMs = candidateStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        } else if (isVoted()) {
            electionTimeoutMs = votedStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        } else if (isUnattached()) {
            electionTimeoutMs = unattachedStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        } else {
            electionTimeoutMs = randomElectionTimeoutMs();
        }

        transitionTo(new UnattachedState(
            time,
            epoch,
            voters,
            state.highWatermark(),
            electionTimeoutMs,
            logContext
        ));
    }

    /**
     * Grant a vote to a candidate and become a follower for this epoch. We will remain in this
     * state until either the election timeout expires or a leader is elected. In particular,
     * we do not begin fetching until the election has concluded and {@link #transitionToFollower(int, int)}
     * is invoked.
     */
    public void transitionToVoted(
        int epoch,
        int candidateId
    ) {
        // 自己给自己投票不允许，自己给自己是默认的
        if (localId.isPresent() && candidateId == localId.getAsInt()) {
            throw new IllegalStateException("Cannot transition to Voted with votedId=" + candidateId +
                " and epoch=" + epoch + " since it matches the local broker.id");
        }
        // 如果是旁观者不参与
        else if (isObserver()) {
            throw new IllegalStateException("Cannot transition to Voted with votedId=" + candidateId +
                " and epoch=" + epoch + " since the local broker.id=" + localId + " is not a voter");
        }
        // 没有候选人列表
        else if (!isVoter(candidateId)) {
            throw new IllegalStateException("Cannot transition to Voted with voterId=" + candidateId +
                " and epoch=" + epoch + " since it is not one of the voters " + voters);
        }

        // 纪元过期
        int currentEpoch = state.epoch();
        if (epoch < currentEpoch) {
            throw new IllegalStateException("Cannot transition to Voted with votedId=" + candidateId +
                " and epoch=" + epoch + " since the current epoch " + currentEpoch + " is larger");
        }
        // 当前状态已不是未参与状态
        else if (epoch == currentEpoch && !isUnattached()) {
            throw new IllegalStateException("Cannot transition to Voted with votedId=" + candidateId +
                " and epoch=" + epoch + " from the current state " + state);
        }

        // Note that we reset the election timeout after voting for a candidate because we
        // know that the candidate has at least as good of a chance of getting elected as us

        // 请注意，我们在为候选人投票后重置了选举超时，因为我们知道候选人至少有与我们一样好的当选机会

        transitionTo(new VotedState(
            time,
            epoch,
            candidateId,
            voters,
            state.highWatermark(),
            randomElectionTimeoutMs(),
            logContext
        ));
    }

    /**
     * Become a follower of an elected leader so that we can begin fetching.
     */
    // 变更为追随者
    public void transitionToFollower(
        int epoch,
        int leaderId
    ) {
        if (localId.isPresent() && leaderId == localId.getAsInt()) {
            throw new IllegalStateException("Cannot transition to Follower with leaderId=" + leaderId +
                " and epoch=" + epoch + " since it matches the local broker.id=" + localId);
        } else if (!isVoter(leaderId)) {
            throw new IllegalStateException("Cannot transition to Follower with leaderId=" + leaderId +
                " and epoch=" + epoch + " since it is not one of the voters " + voters);
        }

        int currentEpoch = state.epoch();
        if (epoch < currentEpoch) {
            throw new IllegalStateException("Cannot transition to Follower with leaderId=" + leaderId +
                " and epoch=" + epoch + " since the current epoch " + currentEpoch + " is larger");
        } else if (epoch == currentEpoch
            && (isFollower() || isLeader())) {
            throw new IllegalStateException("Cannot transition to Follower with leaderId=" + leaderId +
                " and epoch=" + epoch + " from state " + state);
        }

        transitionTo(new FollowerState(
            time,
            epoch,
            leaderId,
            voters,
            state.highWatermark(),
            fetchTimeoutMs,
            logContext
        ));
    }

    public void transitionToCandidate() {
        // 如果是旁观者，只是单纯的broker节点或者不在投票人列表里，那么无法进行转换
        if (isObserver()) {
            throw new IllegalStateException("Cannot transition to Candidate since the local broker.id=" + localId +
                " is not one of the voters " + voters);
        }
        // 如果是领导者也无法转换
        else if (isLeader()) {
            throw new IllegalStateException("Cannot transition to Candidate since the local broker.id=" + localId +
                " since this node is already a Leader with state " + state);
        }

        int retries = isCandidate() ? candidateStateOrThrow().retries() + 1 : 1;
        int newEpoch = epoch() + 1;
        int electionTimeoutMs = randomElectionTimeoutMs();

        // 转化为候选者
        transitionTo(new CandidateState(
            time,
            localIdOrThrow(),
            newEpoch,
            voters,
            state.highWatermark(),
            retries,
            electionTimeoutMs,
            logContext
        ));
    }

    public <T> LeaderState<T> transitionToLeader(long epochStartOffset, BatchAccumulator<T> accumulator) {
        if (isObserver()) {
            throw new IllegalStateException("Cannot transition to Leader since the local broker.id="  + localId +
                " is not one of the voters " + voters);
        } else if (!isCandidate()) {
            throw new IllegalStateException("Cannot transition to Leader from current state " + state);
        }

        // 再判断是否候选者的选票超过半数
        CandidateState candidateState = candidateStateOrThrow();
        if (!candidateState.isVoteGranted())
            throw new IllegalStateException("Cannot become leader without majority votes granted");

        // Note that the leader does not retain the high watermark that was known
        // in the previous state. The reason for this is to protect the monotonicity
        // of the global high watermark, which is exposed through the leader. The
        // only way a new leader can be sure that the high watermark is increasing
        // monotonically is to wait until a majority of the voters have reached the
        // starting offset of the new epoch. The downside of this is that the local
        // state machine is temporarily stalled by the advancement of the global
        // high watermark even though it only depends on local monotonicity. We
        // could address this problem by decoupling the local high watermark, but
        // we typically expect the state machine to be caught up anyway.

        LeaderState<T> state = new LeaderState<>(
            localIdOrThrow(),
            epoch(),
            epochStartOffset,
            voters,
            candidateState.grantingVoters(),
            accumulator,
            logContext
        );
        transitionTo(state);
        return state;
    }

    private void transitionTo(EpochState state) {
        if (this.state != null) {
            try {
                this.state.close();
            } catch (IOException e) {
                throw new UncheckedIOException(
                    "Failed to transition from " + this.state.name() + " to " + state.name(), e);
            }
        }

        // 角色转换 -> 触发选举 -> 选举出领导者
        this.store.writeElectionState(state.election());
        this.state = state;
        log.info("Completed transition to {}", state);
    }

    private int randomElectionTimeoutMs() {
        if (electionTimeoutMs == 0)
            return 0;
        return electionTimeoutMs + random.nextInt(electionTimeoutMs);
    }

    public boolean canGrantVote(int candidateId, boolean isLogUpToDate) {
        return state.canGrantVote(candidateId, isLogUpToDate);
    }

    public FollowerState followerStateOrThrow() {
        if (isFollower())
            return (FollowerState) state;
        throw new IllegalStateException("Expected to be Follower, but the current state is " + state);
    }

    public VotedState votedStateOrThrow() {
        if (isVoted())
            return (VotedState) state;
        throw new IllegalStateException("Expected to be Voted, but current state is " + state);
    }

    public UnattachedState unattachedStateOrThrow() {
        if (isUnattached())
            return (UnattachedState) state;
        throw new IllegalStateException("Expected to be Unattached, but current state is " + state);
    }

    @SuppressWarnings("unchecked")
    public <T> LeaderState<T> leaderStateOrThrow() {
        if (isLeader())
            return (LeaderState<T>) state;
        throw new IllegalStateException("Expected to be Leader, but current state is " + state);
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<LeaderState<T>> maybeLeaderState() {
        EpochState state = this.state;
        if (state instanceof  LeaderState) {
            return Optional.of((LeaderState<T>) state);
        } else {
            return Optional.empty();
        }
    }

    public ResignedState resignedStateOrThrow() {
        if (isResigned())
            return (ResignedState) state;
        throw new IllegalStateException("Expected to be Resigned, but current state is " + state);
    }

    public CandidateState candidateStateOrThrow() {
        if (isCandidate())
            return (CandidateState) state;
        throw new IllegalStateException("Expected to be Candidate, but current state is " + state);
    }

    public LeaderAndEpoch leaderAndEpoch() {
        ElectionState election = state.election();
        return new LeaderAndEpoch(election.leaderIdOpt, election.epoch);
    }

    public boolean isFollower() {
        return state instanceof FollowerState;
    }

    public boolean isVoted() {
        return state instanceof VotedState;
    }

    public boolean isUnattached() {
        return state instanceof UnattachedState;
    }

    public boolean isLeader() {
        return state instanceof LeaderState;
    }

    public boolean isResigned() {
        return state instanceof ResignedState;
    }

    public boolean isCandidate() {
        return state instanceof CandidateState;
    }

}
