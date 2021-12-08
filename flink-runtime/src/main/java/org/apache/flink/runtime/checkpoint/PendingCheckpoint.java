/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV2;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pending checkpoint is a checkpoint that has been started, but has not been
 * acknowledged by all tasks that need to acknowledge it. Once all tasks have
 * acknowledged it, it becomes a {@link CompletedCheckpoint}.
 *
 * <p>Note that the pending checkpoint, as well as the successful checkpoint keep the
 * state handles always as serialized values, never as actual values.
 */
public class PendingCheckpoint {

	/**
	 * Result of the {@link PendingCheckpoint#acknowledgedTasks} method.
	 */
	public enum TaskAcknowledgeResult {
		SUCCESS, // successful acknowledge of the task
		DUPLICATE, // acknowledge message is a duplicate
		UNKNOWN, // unknown task acknowledged
		DISCARDED // pending checkpoint has been discarded
	}

	// ------------------------------------------------------------------------

	/** The PendingCheckpoint logs to the same logger as the CheckpointCoordinator. */
	private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

	private final Object lock = new Object();

	private final JobID jobId;

	private final long checkpointId;

	private final long checkpointTimestamp;

	private final Map<OperatorID, OperatorState> operatorStates;

	private final Map<ExecutionAttemptID, ExecutionVertex> notYetAcknowledgedTasks;

	private final List<MasterState> masterStates;

	private final Set<String> notYetAcknowledgedMasterStates;

	/** Set of acknowledged tasks. */
	private final Set<ExecutionAttemptID> acknowledgedTasks;

	/** The checkpoint properties. */
	private final CheckpointProperties props;

	/** Target storage location to persist the checkpoint metadata to. */
	private final CheckpointStorageLocation targetLocation;

	/** The promise to fulfill once the checkpoint has been completed. */
	private final CompletableFuture<CompletedCheckpoint> onCompletionPromise;

	/** The executor for potentially blocking I/O operations, like state disposal. */
	private final Executor executor;

	private int numAcknowledgedTasks;

	private boolean discarded;

	/** Optional stats tracker callback. */
	@Nullable
	private PendingCheckpointStats statsCallback;

	private volatile ScheduledFuture<?> cancellerHandle;

	// --------------------------------------------------------------------------------------------

	public PendingCheckpoint(
			JobID jobId,
			long checkpointId,
			long checkpointTimestamp,
			Map<ExecutionAttemptID, ExecutionVertex> verticesToConfirm,
			Collection<String> masterStateIdentifiers,
			CheckpointProperties props,
			CheckpointStorageLocation targetLocation,
			Executor executor) {

		checkArgument(verticesToConfirm.size() > 0,
				"Checkpoint needs at least one vertex that commits the checkpoint");

		this.jobId = checkNotNull(jobId);
		this.checkpointId = checkpointId;
		this.checkpointTimestamp = checkpointTimestamp;
		this.notYetAcknowledgedTasks = checkNotNull(verticesToConfirm);
		this.props = checkNotNull(props);
		this.targetLocation = checkNotNull(targetLocation);
		this.executor = Preconditions.checkNotNull(executor);

		this.operatorStates = new HashMap<>();
		this.masterStates = new ArrayList<>(masterStateIdentifiers.size());
		this.notYetAcknowledgedMasterStates = new HashSet<>(masterStateIdentifiers);
		this.acknowledgedTasks = new HashSet<>(verticesToConfirm.size());
		this.onCompletionPromise = new CompletableFuture<>();
	}

	// --------------------------------------------------------------------------------------------

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	public JobID getJobId() {
		return jobId;
	}

	public long getCheckpointId() {
		return checkpointId;
	}

	public long getCheckpointTimestamp() {
		return checkpointTimestamp;
	}

	public int getNumberOfNonAcknowledgedTasks() {
		return notYetAcknowledgedTasks.size();
	}

	public int getNumberOfAcknowledgedTasks() {
		return numAcknowledgedTasks;
	}

	public Map<OperatorID, OperatorState> getOperatorStates() {
		return operatorStates;
	}

	public List<MasterState> getMasterStates() {
		return masterStates;
	}

	public boolean areMasterStatesFullyAcknowledged() {
		return notYetAcknowledgedMasterStates.isEmpty() && !discarded;
	}

	public boolean areTasksFullyAcknowledged() {
		// todo 等待确认的任务全部完成,并且不是废弃的
		return notYetAcknowledgedTasks.isEmpty() && !discarded;
	}

	public boolean isAcknowledgedBy(ExecutionAttemptID executionAttemptId) {
		return !notYetAcknowledgedTasks.containsKey(executionAttemptId);
	}

	public boolean isDiscarded() {
		return discarded;
	}

	/**
	 * Checks whether this checkpoint can be subsumed or whether it should always continue, regardless
	 * of newer checkpoints in progress.
	 *
	 * @return True if the checkpoint can be subsumed, false otherwise.
	 */
	public boolean canBeSubsumed() {
		// If the checkpoint is forced, it cannot be subsumed.
		return !props.forceCheckpoint();
	}

	CheckpointProperties getProps() {
		return props;
	}

	/**
	 * Sets the callback for tracking this pending checkpoint.
	 *
	 * @param trackerCallback Callback for collecting subtask stats.
	 */
	void setStatsCallback(@Nullable PendingCheckpointStats trackerCallback) {
		this.statsCallback = trackerCallback;
	}

	/**
	 * Sets the handle for the canceller to this pending checkpoint. This method fails
	 * with an exception if a handle has already been set.
	 *
	 * @return true, if the handle was set, false, if the checkpoint is already disposed;
	 */
	public boolean setCancellerHandle(ScheduledFuture<?> cancellerHandle) {
		synchronized (lock) {
			if (this.cancellerHandle == null) {
				if (!discarded) {
					this.cancellerHandle = cancellerHandle;
					return true;
				} else {
					return false;
				}
			}
			else {
				throw new IllegalStateException("A canceller handle was already set");
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Progress and Completion
	// ------------------------------------------------------------------------

	/**
	 * Returns the completion future.
	 *
	 * @return A future to the completed checkpoint
	 */
	public CompletableFuture<CompletedCheckpoint> getCompletionFuture() {
		return onCompletionPromise;
	}

	/**
	 * todo
	 *	  调用pendingCheckpoint.finalizeCheckpoint() 将PendingCheckPoint转化为CompletedCheckpoint
	 *
	 */
	public CompletedCheckpoint finalizeCheckpoint() throws IOException {

		synchronized (lock) {
			checkState(areMasterStatesFullyAcknowledged(),
				"Pending checkpoint has not been fully acknowledged by master states yet.");
			checkState(areTasksFullyAcknowledged(),
				"Pending checkpoint has not been fully acknowledged by tasks yet.");

			// make sure we fulfill the promise with an exception if something fails
			try {
				// write out the metadata
				// todo 创建一个checkpointMetadata对象
				final Savepoint savepoint = new SavepointV2(checkpointId, operatorStates.values(), masterStates);

				final CompletedCheckpointStorageLocation finalizedLocation;

				// todo 获取CheckpointMetadataOutputStream
				//  将所有的状态句柄信息通过CheckpointMetadataOutputStream写入到存储系统中
				try (CheckpointMetadataOutputStream out = targetLocation.createMetadataOutputStream()) {
					Checkpoints.storeCheckpointMetadata(savepoint, out);
					// todo 在写入所有元数据后关闭流并完成检查点位置
					finalizedLocation = out.closeAndFinalizeCheckpoint();
				}
				// todo 创建一个CompletedCheckpoint对象
				//   CompletedCheckpoint描述在所有需要的任务确认之后的检查点
				CompletedCheckpoint completed = new CompletedCheckpoint(
						jobId,
						checkpointId,
						checkpointTimestamp,
						System.currentTimeMillis(),
						operatorStates,
						masterStates,
						props,
						finalizedLocation);

				// todo 任务完成
				onCompletionPromise.complete(completed);

				// to prevent null-pointers from concurrent modification, copy reference onto stack
				PendingCheckpointStats statsCallback = this.statsCallback;
				if (statsCallback != null) {
					// Finalize the statsCallback and give the completed checkpoint a
					// callback for discards.
					CompletedCheckpointStats.DiscardCallback discardCallback =
							statsCallback.reportCompletedCheckpoint(finalizedLocation.getExternalPointer());
					completed.setDiscardCallback(discardCallback);
				}

				// mark this pending checkpoint as disposed, but do NOT drop the state
				dispose(false);

				return completed;
			}
			catch (Throwable t) {
				onCompletionPromise.completeExceptionally(t);
				ExceptionUtils.rethrowIOException(t);
				return null; // silence the compiler
			}
		}
	}

	/**
	 * Acknowledges the task with the given execution attempt id and the given subtask state.
	 *
	 * @param executionAttemptId of the acknowledged task
	 * @param operatorSubtaskStates of the acknowledged task
	 * @param metrics Checkpoint metrics for the stats
	 * @return TaskAcknowledgeResult of the operation
	 */
	public TaskAcknowledgeResult acknowledgeTask(
			ExecutionAttemptID executionAttemptId,
			TaskStateSnapshot operatorSubtaskStates,
			CheckpointMetrics metrics) {

		synchronized (lock) {
			// todo 如果弃用, 返回弃用
			if (discarded) {
				return TaskAcknowledgeResult.DISCARDED;
			}
			/**
			 * todo
			 *  从notYetAcknowledgedTasks集合中移除已确认的task, notYetAcknowledgedTasks保存了所有未确认的task
			 *
			 */
			final ExecutionVertex vertex = notYetAcknowledgedTasks.remove(executionAttemptId);

			if (vertex == null) {
				// todo 如果notYetAcknowLedgeTasks中没有该task, 但是它在acknowledgedTasks(已确认的task)集合中, 返回重复确认:DUPLICATE
				if (acknowledgedTasks.contains(executionAttemptId)) {
					return TaskAcknowledgeResult.DUPLICATE;
				} else {
					// todo 其他情况返回未知
					return TaskAcknowledgeResult.UNKNOWN;
				}
			} else {
				// todo 如果 不等于null, 将其添加到已确认的task集合
				acknowledgedTasks.add(executionAttemptId);
			}

			List<OperatorID> operatorIDs = vertex.getJobVertex().getOperatorIDs();
			int subtaskIndex = vertex.getParallelSubtaskIndex();
			long ackTimestamp = System.currentTimeMillis();

			long stateSize = 0L;
			// todo 这段代码为保存各个operator的snapshot状态
			if (operatorSubtaskStates != null) {
				for (OperatorID operatorID : operatorIDs) {
					// todo 返回给定操作符id的子任务状态(如果不包含则返回null)
					OperatorSubtaskState operatorSubtaskState =
						operatorSubtaskStates.getSubtaskStateByOperatorID(operatorID);
					// todo 如果没有获取到operatorSubtaskState给一个空状态
					// if no real operatorSubtaskState was reported, we insert an empty state
					if (operatorSubtaskState == null) {
						operatorSubtaskState = new OperatorSubtaskState();
					}
					// todo 获取该operator的状态
					OperatorState operatorState = operatorStates.get(operatorID);

					if (operatorState == null) {
						// todo 如果operator有状态, 那么添加一个默认的状态
						operatorState = new OperatorState(
							operatorID,
							vertex.getTotalNumberOfParallelSubtasks(),
							vertex.getMaxParallelism());

						operatorStates.put(operatorID, operatorState);
					}
					// todo 对operator添加状态,
					operatorState.putState(subtaskIndex, operatorSubtaskState);
					stateSize += operatorSubtaskState.getStateSize();
				}
			}
			// todo 多少个确认的任务
			++numAcknowledgedTasks;

			// publish the checkpoint statistics
			// to prevent null-pointers from concurrent modification, copy reference onto stack
			// todo 这段代码为汇报所有子任务checkpoint状态
			final PendingCheckpointStats statsCallback = this.statsCallback;
			if (statsCallback != null) {
				// Do this in millis because the web frontend works with them
				long alignmentDurationMillis = metrics.getAlignmentDurationNanos() / 1_000_000;

				SubtaskStateStats subtaskStateStats = new SubtaskStateStats(
					subtaskIndex,
					ackTimestamp,
					stateSize,
					metrics.getSyncDurationMillis(),
					metrics.getAsyncDurationMillis(),
					metrics.getBytesBufferedInAlignment(),
					alignmentDurationMillis);

				statsCallback.reportSubtaskStats(vertex.getJobvertexId(), subtaskStateStats);
			}

			return TaskAcknowledgeResult.SUCCESS;
		}
	}

	/**
	 * Acknowledges a master state (state generated on the checkpoint coordinator) to
	 * the pending checkpoint.
	 *
	 * @param identifier The identifier of the master state
	 * @param state The state to acknowledge
	 */
	public void acknowledgeMasterState(String identifier, @Nullable MasterState state) {

		synchronized (lock) {
			if (!discarded) {
				if (notYetAcknowledgedMasterStates.remove(identifier) && state != null) {
					masterStates.add(state);
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Cancellation
	// ------------------------------------------------------------------------

	/**
	 * Aborts a checkpoint with reason and cause.
	 */
	public void abort(CheckpointFailureReason reason, @Nullable Throwable cause) {
		try {
			CheckpointException exception = new CheckpointException(reason, cause);
			onCompletionPromise.completeExceptionally(exception);
			reportFailedCheckpoint(exception);
			assertAbortSubsumedForced(reason);
		} finally {
			dispose(true);
		}
	}

	/**
	 * Aborts a checkpoint with reason and cause.
	 */
	public void abort(CheckpointFailureReason reason) {
		abort(reason, null);
	}

	private void assertAbortSubsumedForced(CheckpointFailureReason reason) {
		if (props.forceCheckpoint() && reason == CheckpointFailureReason.CHECKPOINT_SUBSUMED) {
			throw new IllegalStateException("Bug: forced checkpoints must never be subsumed, " +
				"the abort reason is : " + reason.message());
		}
	}

	private void dispose(boolean releaseState) {

		synchronized (lock) {
			try {
				numAcknowledgedTasks = -1;
				if (!discarded && releaseState) {
					executor.execute(new Runnable() {
						@Override
						public void run() {

							// discard the private states.
							// unregistered shared states are still considered private at this point.
							try {
								StateUtil.bestEffortDiscardAllStateObjects(operatorStates.values());
								targetLocation.disposeOnFailure();
							} catch (Throwable t) {
								LOG.warn("Could not properly dispose the private states in the pending checkpoint {} of job {}.",
									checkpointId, jobId, t);
							} finally {
								operatorStates.clear();
							}
						}
					});

				}
			} finally {
				discarded = true;
				notYetAcknowledgedTasks.clear();
				acknowledgedTasks.clear();
				cancelCanceller();
			}
		}
	}

	private void cancelCanceller() {
		try {
			final ScheduledFuture<?> canceller = this.cancellerHandle;
			if (canceller != null) {
				canceller.cancel(false);
			}
		}
		catch (Exception e) {
			// this code should not throw exceptions
			LOG.warn("Error while cancelling checkpoint timeout task", e);
		}
	}

	/**
	 * Reports a failed checkpoint with the given optional cause.
	 *
	 * @param cause The failure cause or <code>null</code>.
	 */
	private void reportFailedCheckpoint(Exception cause) {
		// to prevent null-pointers from concurrent modification, copy reference onto stack
		final PendingCheckpointStats statsCallback = this.statsCallback;
		if (statsCallback != null) {
			long failureTimestamp = System.currentTimeMillis();
			statsCallback.reportFailedCheckpoint(failureTimestamp, cause);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("Pending Checkpoint %d @ %d - confirmed=%d, pending=%d",
				checkpointId, checkpointTimestamp, getNumberOfAcknowledgedTasks(), getNumberOfNonAcknowledgedTasks());
	}
}
