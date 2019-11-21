/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.execution.scheduler;

import com.facebook.airlift.concurrent.SetThreadName;
import com.facebook.airlift.stats.TimeStat;
import com.facebook.presto.Session;
import com.facebook.presto.execution.BasicStageExecutionStats;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStateMachine;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.failureDetector.FailureDetector;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.NodePartitionMap;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SplitSourceFactory;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.facebook.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static com.facebook.airlift.concurrent.MoreFutures.whenAnyComplete;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.presto.SystemSessionProperties.getConcurrentLifespansPerNode;
import static com.facebook.presto.SystemSessionProperties.getMaxConcurrentMaterializations;
import static com.facebook.presto.SystemSessionProperties.getMaxTasksPerStage;
import static com.facebook.presto.SystemSessionProperties.getWriterMinSize;
import static com.facebook.presto.execution.BasicStageExecutionStats.aggregateBasicStageStats;
import static com.facebook.presto.execution.SqlStageExecution.createSqlStageExecution;
import static com.facebook.presto.execution.StageExecutionState.ABORTED;
import static com.facebook.presto.execution.StageExecutionState.CANCELED;
import static com.facebook.presto.execution.StageExecutionState.FAILED;
import static com.facebook.presto.execution.StageExecutionState.FINISHED;
import static com.facebook.presto.execution.StageExecutionState.PLANNED;
import static com.facebook.presto.execution.StageExecutionState.RUNNING;
import static com.facebook.presto.execution.StageExecutionState.SCHEDULED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createDiscardingOutputBuffers;
import static com.facebook.presto.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsStageScheduler;
import static com.facebook.presto.execution.scheduler.TableWriteInfo.createTableWriteInfo;
import static com.facebook.presto.spi.ConnectorId.isInternalSystemConnector;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.sql.planner.PlanFragmenter.ROOT_FRAGMENT_ID;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.collect.Streams.stream;
import static com.google.common.graph.Traverser.forTree;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class SqlQueryScheduler
{
    private static final ExchangeLocationsConsumer NON_ROOT_LOCATIONS_CONSUMER = discardingLocationConsumer();
    private static final Optional<int[]> ROOT_BUCKET_TO_PARTITION = Optional.of(new int[1]);
    private static final Optional<int[]> NON_ROOT_BUCKET_TO_PARTITION = Optional.empty();
    private static final OutputBuffers NON_ROOT_OUTPUT_BUFFERS = createDiscardingOutputBuffers();

    private final QueryStateMachine queryStateMachine;
    private final LocationFactory locationFactory;
    private final ExecutionPolicy executionPolicy;
    private final SubPlan plan;
    private final StreamingPlanSection sectionedPlan;
    private final Map<StageId, List<StageExecutionAndScheduler>> stageExecutions = new ConcurrentHashMap<>();
    private final ExecutorService executor;
    private final SplitSchedulerStats schedulerStats;
    private final boolean summarizeTaskInfo;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean scheduling = new AtomicBoolean();
    private final int maxConcurrentMaterializations;
    private final ExchangeLocationsConsumer rootLocationsConsumer;
    private final Metadata metadata;
    private final OutputBuffers rootOutputBuffers;
    private final NodeScheduler nodeScheduler;
    private final RemoteTaskFactory remoteTaskFactory;
    private final SplitSourceFactory splitSourceFactory;
    private final Session session;
    private final int splitBatchSize;
    private final NodePartitioningManager nodePartitioningManager;
    private final ScheduledExecutorService schedulerExecutor;
    private final FailureDetector failureDetector;
    private final NodeTaskMap nodeTaskMap;

    public static SqlQueryScheduler createSqlQueryScheduler(
            QueryStateMachine queryStateMachine,
            LocationFactory locationFactory,
            SubPlan plan,
            NodePartitioningManager nodePartitioningManager,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            Session session,
            boolean summarizeTaskInfo,
            int splitBatchSize,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            OutputBuffers rootOutputBuffers,
            NodeTaskMap nodeTaskMap,
            ExecutionPolicy executionPolicy,
            SplitSchedulerStats schedulerStats,
            Metadata metadata)
    {
        SqlQueryScheduler sqlQueryScheduler = new SqlQueryScheduler(
                queryStateMachine,
                locationFactory,
                plan,
                nodePartitioningManager,
                nodeScheduler,
                remoteTaskFactory,
                splitSourceFactory,
                session,
                summarizeTaskInfo,
                splitBatchSize,
                queryExecutor,
                schedulerExecutor,
                failureDetector,
                rootOutputBuffers,
                nodeTaskMap,
                executionPolicy,
                schedulerStats,
                metadata);
        sqlQueryScheduler.initialize();
        return sqlQueryScheduler;
    }

    private SqlQueryScheduler(
            QueryStateMachine queryStateMachine,
            LocationFactory locationFactory,
            SubPlan plan,
            NodePartitioningManager nodePartitioningManager,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            Session session,
            boolean summarizeTaskInfo,
            int splitBatchSize,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            OutputBuffers rootOutputBuffers,
            NodeTaskMap nodeTaskMap,
            ExecutionPolicy executionPolicy,
            SplitSchedulerStats schedulerStats,
            Metadata metadata)
    {
        this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.plan = requireNonNull(plan, "plan is null");
        this.executionPolicy = requireNonNull(executionPolicy, "schedulerPolicyFactory is null");
        this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.summarizeTaskInfo = requireNonNull(summarizeTaskInfo, "summarizeTaskInfo is null");

        OutputBufferId rootBufferId = getOnlyElement(rootOutputBuffers.getBuffers().keySet());
        this.rootLocationsConsumer = (fragmentId, tasks, noMoreExchangeLocations) -> updateQueryOutputLocations(queryStateMachine, rootBufferId, tasks, noMoreExchangeLocations);
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.rootOutputBuffers = requireNonNull(rootOutputBuffers, "rootOutputBuffers is null");
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
        this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");
        this.session = requireNonNull(session, "session is null");
        this.splitBatchSize = requireNonNull(splitBatchSize, "splitBatchSize is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.schedulerExecutor = requireNonNull(schedulerExecutor, "schedulerExecutor is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.sectionedPlan = extractStreamingSections(plan);
        this.executor = requireNonNull(queryExecutor, "queryExecutor is null");
        this.maxConcurrentMaterializations = getMaxConcurrentMaterializations(session);
    }

    // this is a separate method to ensure that the `this` reference is not leaked during construction
    private void initialize()
    {
        // when query is done or any time a stage completes, attempt to transition query to "final query info ready"
        queryStateMachine.addStateChangeListener(newState ->

        {
            if (newState.isDone()) {
                queryStateMachine.updateQueryInfo(Optional.of(getStageInfo()));
            }
        });
    }

    private void addStageStateChangeListeners(SqlStageExecution stageExecution)
    {
        if (isRoot(stageExecution.getFragment())) {
            stageExecution.addStateChangeListener(state -> {
                if (state == FINISHED) {
                    queryStateMachine.transitionToFinishing();
                }
                else if (state == CANCELED) {
                    // output stage was canceled
                    queryStateMachine.transitionToCanceled();
                }
            });
        }

        stageExecution.addStateChangeListener(state -> {
            if (queryStateMachine.isDone()) {
                return;
            }
            if (state == FAILED) {
                queryStateMachine.transitionToFailed(stageExecution.getStageExecutionInfo().getFailureCause().get().toException());
            }
            else if (state == ABORTED) {
                // this should never happen, since abort can only be triggered in query clean up after the query is finished
                queryStateMachine.transitionToFailed(new PrestoException(GENERIC_INTERNAL_ERROR, "Query stage was aborted"));
            }
            else if (state == FINISHED) {
                // checks if there's any new sections available for execution and starts the scheduling if any
                startScheduling();
            }
            else if (queryStateMachine.getQueryState() == QueryState.STARTING) {
                // if the stage has at least one task, we are running
                if (stageExecution.hasTasks()) {
                    queryStateMachine.transitionToRunning();
                }
            }
        });
        stageExecution.addFinalStageInfoListener(status -> queryStateMachine.updateQueryInfo(Optional.of(getStageInfo())));
    }

    private boolean isRoot(PlanFragment fragment)
    {
        return fragment.getId().getId() == ROOT_FRAGMENT_ID;
    }

    private static void updateQueryOutputLocations(QueryStateMachine queryStateMachine, OutputBufferId rootBufferId, Set<RemoteTask> tasks, boolean noMoreExchangeLocations)
    {
        Map<URI, TaskId> bufferLocations = tasks.stream()
                .collect(toImmutableMap(
                        task -> getBufferLocation(task, rootBufferId),
                        RemoteTask::getTaskId));
        queryStateMachine.updateOutputLocations(bufferLocations, noMoreExchangeLocations);
    }

    private static URI getBufferLocation(RemoteTask remoteTask, OutputBufferId rootBufferId)
    {
        URI location = remoteTask.getTaskStatus().getSelf();
        return uriBuilderFrom(location).appendPath("results").appendPath(rootBufferId.toString()).build();
    }

    /**
     * returns a List of StageExecutionAndSchedulers in a postorder representation of the tree
     */
    private List<StageExecutionAndScheduler> createSectionStageExecutions(StreamingPlanSection section)
    {
        // Only fetch a distribution once per section to ensure all stages see the same machine assignments
        Map<PartitioningHandle, NodePartitionMap> partitioningCache = new HashMap<>();
        TableWriteInfo tableWriteInfo = createTableWriteInfo(section.getPlan(), metadata, session);

        ExchangeLocationsConsumer locationsConsumer;
        Optional<int[]> bucketToPartition;
        OutputBuffers outputBuffers;

        if (isRoot(section.getPlan().getFragment())) {
            locationsConsumer = rootLocationsConsumer;
            bucketToPartition = ROOT_BUCKET_TO_PARTITION;
            outputBuffers = rootOutputBuffers;
        }
        else {
            locationsConsumer = NON_ROOT_LOCATIONS_CONSUMER;
            bucketToPartition = NON_ROOT_BUCKET_TO_PARTITION;
            outputBuffers = NON_ROOT_OUTPUT_BUFFERS;
        }

        List<StageExecutionAndScheduler> sectionStages = createStreamingLinkedStageExecutions(
                locationsConsumer,
                section.getPlan().withBucketToPartition(bucketToPartition),
                nodeScheduler,
                remoteTaskFactory,
                splitSourceFactory,
                session,
                splitBatchSize,
                partitioningHandle -> partitioningCache.computeIfAbsent(partitioningHandle, handle -> nodePartitioningManager.getNodePartitioningMap(session, handle)),
                nodePartitioningManager,
                executor,
                schedulerExecutor,
                failureDetector,
                nodeTaskMap,
                tableWriteInfo,
                Optional.empty());
        Iterables.getLast(sectionStages)
                .getStageExecution()
                .setOutputBuffers(outputBuffers);
        return sectionStages;
    }

    /**
     * returns a List of StageExecutionAndSchedulers in a postorder representation of the tree
     */
    private List<StageExecutionAndScheduler> createStreamingLinkedStageExecutions(
            ExchangeLocationsConsumer parent,
            StreamingSubPlan plan,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            Session session,
            int splitBatchSize,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            NodePartitioningManager nodePartitioningManager,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            NodeTaskMap nodeTaskMap,
            TableWriteInfo tableWriteInfo,
            Optional<SqlStageExecution> parentStageExecution)
    {
        ImmutableList.Builder<StageExecutionAndScheduler> stageExecutionAndSchedulers = ImmutableList.builder();

        PlanFragmentId fragmentId = plan.getFragment().getId();
        StageId stageId = getStageId(fragmentId);
        SqlStageExecution stageExecution = createSqlStageExecution(
                new StageExecutionId(stageId, 0),
                plan.getFragment(),
                remoteTaskFactory,
                session,
                summarizeTaskInfo,
                nodeTaskMap,
                queryExecutor,
                failureDetector,
                schedulerStats,
                tableWriteInfo);

        addStageStateChangeListeners(stageExecution);

        PartitioningHandle partitioningHandle = plan.getFragment().getPartitioning();
        List<RemoteSourceNode> remoteSourceNodes = plan.getFragment().getRemoteSourceNodes();
        Optional<int[]> bucketToPartition = getBucketToPartition(partitioningHandle, partitioningCache, plan.getFragment().getRoot(), remoteSourceNodes);

        // create child stages
        ImmutableSet.Builder<SqlStageExecution> childStagesBuilder = ImmutableSet.builder();
        for (StreamingSubPlan stagePlan : plan.getChildren()) {
            List<StageExecutionAndScheduler> subTree = createStreamingLinkedStageExecutions(
                    stageExecution::addExchangeLocations,
                    stagePlan.withBucketToPartition(bucketToPartition),
                    nodeScheduler,
                    remoteTaskFactory,
                    splitSourceFactory,
                    session,
                    splitBatchSize,
                    partitioningCache,
                    nodePartitioningManager,
                    queryExecutor,
                    schedulerExecutor,
                    failureDetector,
                    nodeTaskMap,
                    tableWriteInfo,
                    Optional.of(stageExecution));
            stageExecutionAndSchedulers.addAll(subTree);
            childStagesBuilder.add(Iterables.getLast(subTree).getStageExecution());
        }
        Set<SqlStageExecution> childStageExecutions = childStagesBuilder.build();
        stageExecution.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                childStageExecutions.forEach(SqlStageExecution::cancel);
            }
        });

        StageLinkage stageLinkage = new StageLinkage(fragmentId, parent, childStageExecutions);
        StageScheduler stageScheduler = createStageScheduler(
                plan,
                nodeScheduler,
                session,
                splitBatchSize,
                partitioningCache,
                nodePartitioningManager,
                schedulerExecutor,
                parentStageExecution,
                stageId,
                stageExecution,
                partitioningHandle,
                splitSourceFactory,
                tableWriteInfo,
                childStageExecutions);
        stageExecutionAndSchedulers.add(new StageExecutionAndScheduler(
                stageExecution,
                stageLinkage,
                stageScheduler));

        return stageExecutionAndSchedulers.build();
    }

    private StageScheduler createStageScheduler(
            StreamingSubPlan plan,
            NodeScheduler nodeScheduler,
            Session session,
            int splitBatchSize,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            NodePartitioningManager nodePartitioningManager,
            ScheduledExecutorService schedulerExecutor,
            Optional<SqlStageExecution> parentStageExecution,
            StageId stageId, SqlStageExecution stageExecution,
            PartitioningHandle partitioningHandle,
            SplitSourceFactory splitSourceFactory,
            TableWriteInfo tableWriteInfo,
            Set<SqlStageExecution> childStageExecutions)
    {
        Map<PlanNodeId, SplitSource> splitSources = splitSourceFactory.createSplitSources(plan.getFragment(), session, tableWriteInfo);
        int maxTasksPerStage = getMaxTasksPerStage(session);
        if (partitioningHandle.equals(SOURCE_DISTRIBUTION)) {
            // nodes are selected dynamically based on the constraints of the splits and the system load
            Entry<PlanNodeId, SplitSource> entry = getOnlyElement(splitSources.entrySet());
            PlanNodeId planNodeId = entry.getKey();
            SplitSource splitSource = entry.getValue();
            ConnectorId connectorId = splitSource.getConnectorId();
            if (isInternalSystemConnector(connectorId)) {
                connectorId = null;
            }

            NodeSelector nodeSelector = nodeScheduler.createNodeSelector(connectorId, maxTasksPerStage);
            SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeSelector, stageExecution::getAllTasks);

            checkArgument(!plan.getFragment().getStageExecutionDescriptor().isStageGroupedExecution());
            return newSourcePartitionedSchedulerAsStageScheduler(stageExecution, planNodeId, splitSource, placementPolicy, splitBatchSize);
        }
        else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            Supplier<Collection<TaskStatus>> sourceTasksProvider = () -> childStageExecutions.stream()
                    .map(SqlStageExecution::getAllTasks)
                    .flatMap(Collection::stream)
                    .map(RemoteTask::getTaskStatus)
                    .collect(toList());

            Supplier<Collection<TaskStatus>> writerTasksProvider = () -> stageExecution.getAllTasks().stream()
                    .map(RemoteTask::getTaskStatus)
                    .collect(toList());

            ScaledWriterScheduler scheduler = new ScaledWriterScheduler(
                    stageExecution,
                    sourceTasksProvider,
                    writerTasksProvider,
                    nodeScheduler.createNodeSelector(null),
                    schedulerExecutor,
                    getWriterMinSize(session));
            whenAllStages(childStageExecutions, StageExecutionState::isDone)
                    .addListener(scheduler::finish, directExecutor());
            return scheduler;
        }
        else {
            if (!splitSources.isEmpty()) {
                // contains local source
                List<PlanNodeId> schedulingOrder = plan.getFragment().getTableScanSchedulingOrder();
                ConnectorId connectorId = partitioningHandle.getConnectorId().orElseThrow(IllegalStateException::new);
                List<ConnectorPartitionHandle> connectorPartitionHandles;
                boolean groupedExecutionForStage = plan.getFragment().getStageExecutionDescriptor().isStageGroupedExecution();
                if (groupedExecutionForStage) {
                    connectorPartitionHandles = nodePartitioningManager.listPartitionHandles(session, partitioningHandle);
                    checkState(!ImmutableList.of(NOT_PARTITIONED).equals(connectorPartitionHandles));
                }
                else {
                    connectorPartitionHandles = ImmutableList.of(NOT_PARTITIONED);
                }

                BucketNodeMap bucketNodeMap;
                List<InternalNode> stageNodeList;
                if (plan.getFragment().getRemoteSourceNodes().stream().allMatch(node -> node.getExchangeType() == REPLICATE)) {
                    // no non-replicated remote source
                    boolean dynamicLifespanSchedule = plan.getFragment().getStageExecutionDescriptor().isDynamicLifespanSchedule();
                    bucketNodeMap = nodePartitioningManager.getBucketNodeMap(session, partitioningHandle, dynamicLifespanSchedule);

                    // verify execution is consistent with planner's decision on dynamic lifespan schedule
                    verify(bucketNodeMap.isDynamic() == dynamicLifespanSchedule);

                    if (!bucketNodeMap.isDynamic()) {
                        stageNodeList = ((FixedBucketNodeMap) bucketNodeMap).getBucketToNode().stream()
                                .distinct()
                                .collect(toImmutableList());
                    }
                    else {
                        stageNodeList = new ArrayList<>(nodeScheduler.createNodeSelector(connectorId).selectRandomNodes(maxTasksPerStage));
                    }
                }
                else {
                    // cannot use dynamic lifespan schedule
                    verify(!plan.getFragment().getStageExecutionDescriptor().isDynamicLifespanSchedule());

                    // remote source requires nodePartitionMap
                    NodePartitionMap nodePartitionMap = partitioningCache.apply(plan.getFragment().getPartitioning());
                    if (groupedExecutionForStage) {
                        checkState(connectorPartitionHandles.size() == nodePartitionMap.getBucketToPartition().length);
                    }
                    stageNodeList = nodePartitionMap.getPartitionToNode();
                    bucketNodeMap = nodePartitionMap.asBucketNodeMap();
                }

                FixedSourcePartitionedScheduler fixedSourcePartitionedScheduler = new FixedSourcePartitionedScheduler(
                        stageExecution,
                        splitSources,
                        plan.getFragment().getStageExecutionDescriptor(),
                        schedulingOrder,
                        stageNodeList,
                        bucketNodeMap,
                        splitBatchSize,
                        getConcurrentLifespansPerNode(session),
                        nodeScheduler.createNodeSelector(connectorId),
                        connectorPartitionHandles);
                if (plan.getFragment().getStageExecutionDescriptor().isRecoverableGroupedExecution()) {
                    stageExecution.registerStageTaskRecoveryCallback(taskId -> {
                        checkArgument(taskId.getStageExecutionId().getStageId().equals(stageId), "The task did not execute this stage");
                        checkArgument(parentStageExecution.isPresent(), "Parent stage execution must exist");
                        checkArgument(parentStageExecution.get().getAllTasks().size() == 1, "Parent stage should only have one task for recoverable grouped execution");

                        parentStageExecution.get().removeRemoteSourceIfSingleTaskStage(taskId);
                        fixedSourcePartitionedScheduler.recover(taskId);
                    });
                }
                return fixedSourcePartitionedScheduler;
            }

            else {
                // all sources are remote
                NodePartitionMap nodePartitionMap = partitioningCache.apply(plan.getFragment().getPartitioning());
                List<InternalNode> partitionToNode = nodePartitionMap.getPartitionToNode();
                // todo this should asynchronously wait a standard timeout period before failing
                checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
                return new FixedCountScheduler(stageExecution, partitionToNode);
            }
        }
    }

    private Optional<int[]> getBucketToPartition(
            PartitioningHandle partitioningHandle,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            PlanNode fragmentRoot,
            List<RemoteSourceNode> remoteSourceNodes)
    {
        if (partitioningHandle.equals(SOURCE_DISTRIBUTION) || partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            return Optional.of(new int[1]);
        }
        else if (PlanNodeSearcher.searchFrom(fragmentRoot).where(node -> node instanceof TableScanNode).findFirst().isPresent()) {
            if (remoteSourceNodes.stream().allMatch(node -> node.getExchangeType() == REPLICATE)) {
                return Optional.empty();
            }
            else {
                // remote source requires nodePartitionMap
                NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle);
                return Optional.of(nodePartitionMap.getBucketToPartition());
            }
        }
        else {
            NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle);
            List<InternalNode> partitionToNode = nodePartitionMap.getPartitionToNode();
            // todo this should asynchronously wait a standard timeout period before failing
            checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
            return Optional.of(nodePartitionMap.getBucketToPartition());
        }
    }

    // TODO: determine how all stats should be computed
    public BasicStageExecutionStats getBasicStageStats()
    {
        List<BasicStageExecutionStats> stageStats = stageExecutions.values().stream()
                .map(stageExecutionAndSchedulers -> getLast(stageExecutionAndSchedulers).getStageExecution().getBasicStageStats())
                .collect(toImmutableList());

        return aggregateBasicStageStats(stageStats);
    }

    public StageInfo getStageInfo()
    {
        // TODO this one pass in all
        Map<StageId, StageExecutionInfo> stageInfos = stageExecutions.values().stream()
                .map(stageExecutionAndSchedulers -> getLast(stageExecutionAndSchedulers).getStageExecution().getStageExecutionInfo())
                .collect(toImmutableMap(execution -> execution.getStageExecutionId().getStageId(), identity()));

        return buildStageInfo(plan, stageInfos);
    }

    private StageInfo buildStageInfo(SubPlan subPlan, Map<StageId, StageExecutionInfo> stageExecutionInfos)
    {
        StageId stageId = getStageId(subPlan.getFragment().getId());
        List<StageExecutionAndScheduler> stageExecutionAndSchedulers = stageExecutions.get(stageId);
        Optional<StageExecutionInfo> latestAttemptInfo = stageExecutionAndSchedulers == null ?
                Optional.empty() :
                Optional.of(getLast(stageExecutionAndSchedulers).getStageExecution().getStageExecutionInfo());

        List<StageExecutionInfo> previousAttemptInfos = stageExecutionAndSchedulers == null ?
                ImmutableList.of() :
                stageExecutionAndSchedulers.subList(0, stageExecutionAndSchedulers.size() - 2).stream()
                        .map(StageExecutionAndScheduler::getStageExecution)
                        .map(SqlStageExecution::getStageExecutionInfo)
                        .collect(toImmutableList());
        StageExecutionInfo stageExecutionInfo = stageExecutionInfos.get(stageId);
        return new StageInfo(
                stageId,
                locationFactory.createStageLocation(stageId),
                Optional.of(subPlan.getFragment()),
                latestAttemptInfo,
                previousAttemptInfos,
                subPlan.getChildren().stream()
                        .map(plan -> buildStageInfo(plan, stageExecutionInfos))
                        .collect(toImmutableList()));
    }

    public long getUserMemoryReservation()
    {
        return stageExecutions.values().stream()
                .mapToLong(stageExecutionAndSchedulers -> getLast(stageExecutionAndSchedulers).getStageExecution().getUserMemoryReservation())
                .sum();
    }

    public long getTotalMemoryReservation()
    {
        return stageExecutions.values().stream()
                .mapToLong(stageExecutionAndSchedulers -> getLast(stageExecutionAndSchedulers).getStageExecution().getTotalMemoryReservation())
                .sum();
    }

    public Duration getTotalCpuTime()
    {
        long millis = stageExecutions.values().stream()
                .mapToLong(stageExecutionAndSchedulers -> getLast(stageExecutionAndSchedulers).getStageExecution().getTotalCpuTime().toMillis())
                .sum();
        return new Duration(millis, MILLISECONDS);
    }

    public void start()
    {
        if (started.compareAndSet(false, true)) {
            startScheduling();
        }
    }

    private void startScheduling()
    {
        requireNonNull(stageExecutions);
        // still scheduling the previous batch of stages
        if (scheduling.get()) {
            return;
        }
        executor.submit(this::schedule);
    }

    private void schedule()
    {
        if (!scheduling.compareAndSet(false, true)) {
            // still scheduling the previous batch of stages
            return;
        }

        List<StageExecutionAndScheduler> scheduledStageExecutions = new ArrayList<>();

        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            Set<StageId> completedStages = new HashSet<>();

            List<ExecutionSchedule> sectionExecutionSchedules = new LinkedList<>();

            while (!Thread.currentThread().isInterrupted()) {
                // remove finished section
                sectionExecutionSchedules.removeIf(ExecutionSchedule::isFinished);

                // try to pull more section that are ready to be run
                List<StreamingPlanSection> sectionsReadyForExecution = getSectionsReadyForExecution();

                // all finished
                if (sectionsReadyForExecution.isEmpty() && sectionExecutionSchedules.isEmpty()) {
                    break;
                }

                List<List<StageExecutionAndScheduler>> sectionStageExecutions = getOrCreateStageExecutions(sectionsReadyForExecution);
                sectionStageExecutions.forEach(scheduledStageExecutions::addAll);
                sectionStageExecutions.stream()
                        .map(executionInfos -> executionInfos.stream()
                                .map(StageExecutionAndScheduler::getStageExecution)
                                .collect(toImmutableList()))
                        .map(executionPolicy::createExecutionSchedule)
                        .forEach(sectionExecutionSchedules::add);

                while (sectionExecutionSchedules.stream().noneMatch(ExecutionSchedule::isFinished)) {
                    List<ListenableFuture<?>> blockedStages = new ArrayList<>();

                    List<SqlStageExecution> executionsToSchedule = sectionExecutionSchedules.stream()
                            .flatMap(schedule -> schedule.getStagesToSchedule().stream())
                            .collect(toImmutableList());

                    for (SqlStageExecution stageExecution : executionsToSchedule) {
                        StageId stageId = stageExecution.getStageExecutionId().getStageId();
                        stageExecution.beginScheduling();

                        // perform some scheduling work
                        ScheduleResult result = getLast(stageExecutions.get(stageId)).getStageScheduler()
                                .schedule();

                        // modify parent and children based on the results of the scheduling
                        if (result.isFinished()) {
                            stageExecution.schedulingComplete();
                        }
                        else if (!result.getBlocked().isDone()) {
                            blockedStages.add(result.getBlocked());
                        }
                        getLast(stageExecutions.get(stageId)).getStageLinkage()
                                .processScheduleResults(stageExecution.getState(), result.getNewTasks());
                        schedulerStats.getSplitsScheduledPerIteration().add(result.getSplitsScheduled());
                        if (result.getBlockedReason().isPresent()) {
                            switch (result.getBlockedReason().get()) {
                                case WRITER_SCALING:
                                    // no-op
                                    break;
                                case WAITING_FOR_SOURCE:
                                    schedulerStats.getWaitingForSource().update(1);
                                    break;
                                case SPLIT_QUEUES_FULL:
                                    schedulerStats.getSplitQueuesFull().update(1);
                                    break;
                                case MIXED_SPLIT_QUEUES_FULL_AND_WAITING_FOR_SOURCE:
                                    schedulerStats.getMixedSplitQueuesFullAndWaitingForSource().update(1);
                                    break;
                                case NO_ACTIVE_DRIVER_GROUP:
                                    schedulerStats.getNoActiveDriverGroup().update(1);
                                    break;
                                default:
                                    throw new UnsupportedOperationException("Unknown blocked reason: " + result.getBlockedReason().get());
                            }
                        }
                    }

                    // make sure to update stage linkage at least once per loop to catch async state changes (e.g., partial cancel)
                    boolean stageFinishedExecution = false;
                    for (StageExecutionAndScheduler stageExecutionAndScheduler : scheduledStageExecutions) {
                        SqlStageExecution stageExecution = stageExecutionAndScheduler.getStageExecution();
                        StageId stageId = stageExecution.getStageExecutionId().getStageId();
                        if (!completedStages.contains(stageId) && stageExecution.getState().isDone()) {
                            stageExecutionAndScheduler.getStageLinkage()
                                    .processScheduleResults(stageExecution.getState(), ImmutableSet.of());
                            completedStages.add(stageId);
                            stageFinishedExecution = true;
                        }
                    }

                    // if any stage has just finished execution try to pull more sections for scheduling
                    if (stageFinishedExecution) {
                        break;
                    }

                    // wait for a state change and then schedule again
                    if (!blockedStages.isEmpty()) {
                        try (TimeStat.BlockTimer timer = schedulerStats.getSleepTime().time()) {
                            tryGetFutureValue(whenAnyComplete(blockedStages), 1, SECONDS);
                        }
                        for (ListenableFuture<?> blockedStage : blockedStages) {
                            blockedStage.cancel(true);
                        }
                    }
                }
            }

            for (StageExecutionAndScheduler stageExecutionAndScheduler : scheduledStageExecutions) {
                StageExecutionState state = stageExecutionAndScheduler.getStageExecution().getState();
                if (state != SCHEDULED && state != RUNNING && !state.isDone()) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Scheduling is complete, but stage execution %s is in state %s", stageExecutionAndScheduler.getStageExecution().getStageExecutionId(), state));
                }
            }

            scheduling.set(false);

            if (!getSectionsReadyForExecution().isEmpty()) {
                startScheduling();
            }
        }
        catch (Throwable t) {
            scheduling.set(false);
            queryStateMachine.transitionToFailed(t);
            throw t;
        }
        finally {
            RuntimeException closeError = new RuntimeException();
            for (StageExecutionAndScheduler stageExecutionAndScheduler : scheduledStageExecutions) {
                try {
                    stageExecutionAndScheduler.getStageScheduler().close();
                }
                catch (Throwable t) {
                    queryStateMachine.transitionToFailed(t);
                    // Self-suppression not permitted
                    if (closeError != t) {
                        closeError.addSuppressed(t);
                    }
                }
            }
            if (closeError.getSuppressed().length > 0) {
                throw closeError;
            }
        }
    }

    private List<StreamingPlanSection> getSectionsReadyForExecution()
    {
        long runningPlanSections =
                stream(forTree(StreamingPlanSection::getChildren).depthFirstPreOrder(sectionedPlan))
                        .map(section -> getLatestStageExecution(section.getPlan().getFragment().getId()))
                        .filter(Optional::isPresent)
                        .map(stageExecution -> stageExecution.get().getState())
                        .filter(state -> !state.isDone() && state != PLANNED)
                        .count();
        return stream(forTree(StreamingPlanSection::getChildren).depthFirstPreOrder(sectionedPlan))
                // get all sections ready for execution
                .filter(this::isReadyForExecution)
                .limit(maxConcurrentMaterializations - runningPlanSections)
                .collect(toImmutableList());
    }

    private boolean isReadyForExecution(StreamingPlanSection section)
    {
        Optional<SqlStageExecution> stageExecution = getLatestStageExecution(section.getPlan().getFragment().getId());
        if (stageExecution.isPresent() && stageExecution.get().getState() != PLANNED) {
            // already scheduled
            return false;
        }
        for (StreamingPlanSection child : section.getChildren()) {
            Optional<SqlStageExecution> rootStageExecution = getLatestStageExecution(child.getPlan().getFragment().getId());
            if (!rootStageExecution.isPresent() || rootStageExecution.get().getState() != FINISHED) {
                return false;
            }
        }
        return true;
    }

    private List<List<StageExecutionAndScheduler>> getOrCreateStageExecutions(List<StreamingPlanSection> sections)
    {
        ImmutableList.Builder<List<StageExecutionAndScheduler>> stageExecutionAndSchedulers = ImmutableList.builder();

        for (StreamingPlanSection section : sections) {
            if (!getLatestStageExecution(section.getPlan().getFragment().getId()).isPresent()) {
                createSectionStageExecutions(section).stream()
                        .forEach(stageExecutionAndScheduler -> stageExecutions.put(
                                stageExecutionAndScheduler.getStageExecution().getStageExecutionId().getStageId(),
                                ImmutableList.of(stageExecutionAndScheduler)));
            }
            stageExecutionAndSchedulers.add(
                    stream(forTree(StreamingSubPlan::getChildren).depthFirstPreOrder(section.getPlan()))
                            .map(StreamingSubPlan::getFragment)
                            .map(PlanFragment::getId)
                            .map(this::getLatestStageExecutionAndScheduler)
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .collect(toImmutableList()));
        }
        return stageExecutionAndSchedulers.build();
    }

    private Optional<SqlStageExecution> getLatestStageExecution(PlanFragmentId planFragmentId)
    {
        return getLatestStageExecutionAndScheduler(planFragmentId).map(StageExecutionAndScheduler::getStageExecution);
    }

    private Optional<StageExecutionAndScheduler> getLatestStageExecutionAndScheduler(PlanFragmentId planFragmentId)
    {
        List<StageExecutionAndScheduler> stageExecutionAndSchedulers = stageExecutions.get(getStageId(planFragmentId));
        return stageExecutionAndSchedulers == null ? Optional.empty() : Optional.of(getLast(stageExecutionAndSchedulers));
    }

    private StageId getStageId(PlanFragmentId fragmentId)
    {
        return new StageId(queryStateMachine.getQueryId(), fragmentId.getId());
    }

    // TODO: synchronize everything
    //TODO check if stageId is present to cancel
    public void cancelStage(StageId stageId)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            SqlStageExecution execution = getLast(stageExecutions.get(stageId)).getStageExecution();
            SqlStageExecution stage = requireNonNull(execution, () -> format("Stage %s does not exist", stageId));
            stage.cancel();
        }
    }

    public void abort()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            stageExecutions.values().forEach(stageExecutionAndSchedulers -> getLast(stageExecutionAndSchedulers).getStageExecution().abort());
        }
    }

    private static ListenableFuture<?> whenAllStages(Collection<SqlStageExecution> stageExecutions, Predicate<StageExecutionState> predicate)
    {
        checkArgument(!stageExecutions.isEmpty(), "stageExecutions is empty");
        Set<StageExecutionId> stageIds = newConcurrentHashSet(stageExecutions.stream()
                .map(SqlStageExecution::getStageExecutionId)
                .collect(toSet()));
        SettableFuture<?> future = SettableFuture.create();

        for (SqlStageExecution stage : stageExecutions) {
            stage.addStateChangeListener(state -> {
                if (predicate.test(state) && stageIds.remove(stage.getStageExecutionId()) && stageIds.isEmpty()) {
                    future.set(null);
                }
            });
        }

        return future;
    }

    public static StreamingPlanSection extractStreamingSections(SubPlan subPlan)
    {
        ImmutableList.Builder<SubPlan> materializedExchangeChildren = ImmutableList.builder();
        StreamingSubPlan streamingSection = extractStreamingSection(subPlan, materializedExchangeChildren);
        return new StreamingPlanSection(
                streamingSection,
                materializedExchangeChildren.build().stream()
                        .map(SqlQueryScheduler::extractStreamingSections)
                        .collect(toImmutableList()));
    }

    private static StreamingSubPlan extractStreamingSection(SubPlan subPlan, ImmutableList.Builder<SubPlan> materializedExchangeChildren)
    {
        ImmutableList.Builder<StreamingSubPlan> streamingSources = ImmutableList.builder();
        Set<PlanFragmentId> streamingFragmentIds = subPlan.getFragment().getRemoteSourceNodes().stream()
                .map(RemoteSourceNode::getSourceFragmentIds)
                .flatMap(List::stream)
                .collect(toImmutableSet());
        for (SubPlan child : subPlan.getChildren()) {
            if (streamingFragmentIds.contains(child.getFragment().getId())) {
                streamingSources.add(extractStreamingSection(child, materializedExchangeChildren));
            }
            else {
                materializedExchangeChildren.add(child);
            }
        }
        return new StreamingSubPlan(subPlan.getFragment(), streamingSources.build());
    }

    private interface ExchangeLocationsConsumer
    {
        void addExchangeLocations(PlanFragmentId fragmentId, Set<RemoteTask> tasks, boolean noMoreExchangeLocations);
    }

    private static ExchangeLocationsConsumer discardingLocationConsumer()
    {
        return (fragmentId, tasks, noMoreExchangeLocations) -> {};
    }

    private static class StageLinkage
    {
        private final PlanFragmentId currentStageFragmentId;
        private final ExchangeLocationsConsumer parent;
        private final Set<OutputBufferManager> childOutputBufferManagers;

        public StageLinkage(PlanFragmentId fragmentId, ExchangeLocationsConsumer parent, Set<SqlStageExecution> children)
        {
            this.currentStageFragmentId = fragmentId;
            this.parent = parent;
            this.childOutputBufferManagers = children.stream()
                    .map(childStage -> {
                        PartitioningHandle partitioningHandle = childStage.getFragment().getPartitioningScheme().getPartitioning().getHandle();
                        if (partitioningHandle.equals(FIXED_BROADCAST_DISTRIBUTION)) {
                            return new BroadcastOutputBufferManager(childStage::setOutputBuffers);
                        }
                        else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
                            return new ScaledOutputBufferManager(childStage::setOutputBuffers);
                        }
                        else {
                            int partitionCount = Ints.max(childStage.getFragment().getPartitioningScheme().getBucketToPartition().get()) + 1;
                            return new PartitionedOutputBufferManager(partitioningHandle, partitionCount, childStage::setOutputBuffers);
                        }
                    })
                    .collect(toImmutableSet());
        }

        public void processScheduleResults(StageExecutionState newState, Set<RemoteTask> newTasks)
        {
            boolean noMoreTasks = false;
            switch (newState) {
                case PLANNED:
                case SCHEDULING:
                    // workers are still being added to the query
                    break;
                case FINISHED_TASK_SCHEDULING:
                case SCHEDULING_SPLITS:
                case SCHEDULED:
                case RUNNING:
                case FINISHED:
                case CANCELED:
                    // no more workers will be added to the query
                    noMoreTasks = true;
                case ABORTED:
                case FAILED:
                    // DO NOT complete a FAILED or ABORTED stage.  This will cause the
                    // stage above to finish normally, which will result in a query
                    // completing successfully when it should fail..
                    break;
            }

            // Add an exchange location to the parent stage for each new task
            parent.addExchangeLocations(currentStageFragmentId, newTasks, noMoreTasks);

            if (!childOutputBufferManagers.isEmpty()) {
                // Add an output buffer to the child stages for each new task
                List<OutputBufferId> newOutputBuffers = newTasks.stream()
                        .map(task -> new OutputBufferId(task.getTaskId().getId()))
                        .collect(toImmutableList());
                for (OutputBufferManager child : childOutputBufferManagers) {
                    child.addOutputBuffers(newOutputBuffers, noMoreTasks);
                }
            }
        }
    }

    public static class StreamingPlanSection
    {
        private final StreamingSubPlan plan;
        // materialized exchange children
        private final List<StreamingPlanSection> children;

        public StreamingPlanSection(StreamingSubPlan plan, List<StreamingPlanSection> children)
        {
            this.plan = requireNonNull(plan, "plan is null");
            this.children = ImmutableList.copyOf(requireNonNull(children, "children is null"));
        }

        public StreamingSubPlan getPlan()
        {
            return plan;
        }

        public List<StreamingPlanSection> getChildren()
        {
            return children;
        }
    }

    /**
     * StreamingSubPlan is similar to SubPlan but only contains streaming children
     */
    public static class StreamingSubPlan
    {
        private final PlanFragment fragment;
        // streaming children
        private final List<StreamingSubPlan> children;

        public StreamingSubPlan(PlanFragment fragment, List<StreamingSubPlan> children)
        {
            this.fragment = requireNonNull(fragment, "fragment is null");
            this.children = ImmutableList.copyOf(requireNonNull(children, "children is null"));
        }

        public PlanFragment getFragment()
        {
            return fragment;
        }

        public List<StreamingSubPlan> getChildren()
        {
            return children;
        }

        public StreamingSubPlan withBucketToPartition(Optional<int[]> bucketToPartition)
        {
            return new StreamingSubPlan(fragment.withBucketToPartition(bucketToPartition), children);
        }
    }

    private static class StageExecutionAndScheduler
    {
        private final SqlStageExecution stageExecution;
        private final StageLinkage stageLinkage;
        private final StageScheduler stageScheduler;

        private StageExecutionAndScheduler(SqlStageExecution stageExecution, StageLinkage stageLinkage, StageScheduler stageScheduler)
        {
            this.stageExecution = requireNonNull(stageExecution, "stageExecution is null");
            this.stageLinkage = requireNonNull(stageLinkage, "stageLinkage is null");
            this.stageScheduler = requireNonNull(stageScheduler, "stageScheduler is null");
        }

        public SqlStageExecution getStageExecution()
        {
            return stageExecution;
        }

        public StageLinkage getStageLinkage()
        {
            return stageLinkage;
        }

        public StageScheduler getStageScheduler()
        {
            return stageScheduler;
        }
    }
}
