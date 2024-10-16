/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.management.cache.IdleVerifyResultV2;
import org.apache.ignite.internal.management.cache.PartitionKeyV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CHECK_SNAPSHOT_METAS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CHECK_SNAPSHOT_PARTS;

/** Distributed process of snapshot checking (with the partition hashes). */
public class SnapshotCheckProcess {
    /** */
    private final IgniteLogger log;

    /** */
    private final GridKernalContext kctx;

    /** Operation contexts by name. */
    private final Map<String, SnapshotCheckContext> contexts = new ConcurrentHashMap<>();

    /** Cluster-wide operation futures per snapshot called from current node. */
    private final Map<UUID, GridFutureAdapter<SnapshotPartitionsVerifyResult>> clusterOpFuts = new ConcurrentHashMap<>();

    /** Check metas first phase subprocess. */
    private final DistributedProcess<SnapshotCheckProcessRequest, SnapshotCheckMetasResponse> phase1CheckMetas;

    /** Partition hashes second phase subprocess.  */
    private final DistributedProcess<SnapshotCheckProcessRequest, AbstractSnapshotCheckResponse> phase2PartsHashes;

    /** Stop node lock. */
    private boolean nodeStopping;

    /** */
    public SnapshotCheckProcess(GridKernalContext kctx) {
        this.kctx = kctx;

        log = kctx.log(getClass());

        phase1CheckMetas = new DistributedProcess<>(kctx, CHECK_SNAPSHOT_METAS, this::prepareAndCheckMetas,
            this::reducePreparationAndMetasCheck);

        phase2PartsHashes = new DistributedProcess<>(kctx, CHECK_SNAPSHOT_PARTS, this::validateParts,
            this::reduceValidatePartsAndFinish);

        kctx.event().addLocalEventListener(evt -> {
            UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

            Throwable err = new ClusterTopologyCheckedException("Snapshot validation stopped. A required node left " +
                "the cluster [nodeId=" + nodeId + ']');

            contexts.values().forEach(ctx -> {
                if (ctx.req.nodes().contains(nodeId)) {
                    ctx.locProcFut.onDone(err);

                    // We have no a guaranty that a node-left-event is processed strictly before the 1st phase reduce which
                    // can handle this error.
                    GridFutureAdapter<?> clusterFut = clusterOpFuts.get(ctx.req.requestId());

                    if (clusterFut != null)
                        clusterFut.onDone(err);
                }
            });
        }, EVT_NODE_FAILED, EVT_NODE_LEFT);
    }

    /**
     * Stops all the processes with the passed exception.
     *
     * @param err The interrupt reason.
     */
    void interrupt(Throwable err) {
        // Prevents starting new processes in #prepareAndCheckMetas.
        synchronized (contexts) {
            nodeStopping = true;
        }

        contexts.forEach((snpName, ctx) -> ctx.locProcFut.onDone(err));

        clusterOpFuts.forEach((reqId, fut) -> fut.onDone(err));
    }

    /** Phase 2 and process finish. */
    private IgniteInternalFuture<?> reduceValidatePartsAndFinish(
        UUID reqId,
        Map<UUID, AbstractSnapshotCheckResponse> results,
        Map<UUID, Throwable> errors
    ) {
        SnapshotCheckContext ctx = context(null, reqId);

        if (ctx == null)
            return new GridFinishedFuture<>();

        contexts.remove(ctx.req.snapshotName());

        if (log.isInfoEnabled())
            log.info("Finished snapshot validation [req=" + ctx.req + ']');

        GridFutureAdapter<SnapshotPartitionsVerifyResult> clusterOpFut = clusterOpFuts.get(reqId);

        if (clusterOpFut == null)
            return new GridFinishedFuture<>();

        if (ctx.req.incrementalIndex() > 0)
            reduceIncrementalResults(ctx.req.nodes(), ctx.clusterMetas, results, errors, clusterOpFut);
        else if (ctx.req.allRestoreHandlers())
            reduceCustomHandlersResults(ctx, results, errors, clusterOpFut);
        else
            reducePartitionsHashesResults(ctx.clusterMetas, results, errors, clusterOpFut);

        return new GridFinishedFuture<>();
    }

    /** */
    private void reduceIncrementalResults(
        Set<UUID> requiredNodes,
        Map<ClusterNode, List<SnapshotMetadata>> clusterMetas,
        Map<UUID, AbstractSnapshotCheckResponse> results,
        Map<UUID, Throwable> errors,
        GridFutureAdapter<SnapshotPartitionsVerifyResult> fut
    ) {
        SnapshotChecker checker = kctx.cache().context().snapshotMgr().checker();

        Map<ClusterNode, List<IncrementalSnapshotCheckResult>> reduced = new HashMap<>();

        for (Map.Entry<UUID, AbstractSnapshotCheckResponse> resE : results.entrySet()) {
            UUID nodeId = resE.getKey();
            SnapshotCheckIncrementalResponse incResp = (SnapshotCheckIncrementalResponse)resE.getValue();

            if (incResp == null || !requiredNodes.contains(nodeId))
                continue;

            ClusterNode node = kctx.cluster().get().node(nodeId);

            incResp.incremenlatSnapshotResults().forEach((consId, res) -> reduced.computeIfAbsent(node, nid -> new ArrayList<>())
                .add(res));

            if (F.isEmpty(incResp.exceptions()))
                continue;

            errors.putIfAbsent(nodeId, asException(F.firstValue(incResp.exceptions())));
        }

        IdleVerifyResultV2 chkRes = checker.reduceIncrementalResults(reduced, mapErrors(errors));

        fut.onDone(new SnapshotPartitionsVerifyResult(clusterMetas, chkRes));
    }

    /** */
    private void reduceCustomHandlersResults(
        SnapshotCheckContext ctx,
        Map<UUID, AbstractSnapshotCheckResponse> results,
        Map<UUID, Throwable> errors,
        GridFutureAdapter<SnapshotPartitionsVerifyResult> fut
    ) {
        try {
            if (!errors.isEmpty())
                throw F.firstValue(errors);

            // Check responses: node -> consistentId -> handler name -> handler result.
            Map<ClusterNode, Map<Object, Map<String, SnapshotHandlerResult<?>>>> reduced = new HashMap<>();

            for (Map.Entry<UUID, AbstractSnapshotCheckResponse> respEntry : results.entrySet()) {
                SnapshotCheckCustomHandlersResponse nodeResp = (SnapshotCheckCustomHandlersResponse)respEntry.getValue();

                if (nodeResp == null)
                    continue;

                if (!F.isEmpty(nodeResp.exceptions()))
                    throw F.firstValue(nodeResp.exceptions());

                UUID nodeId = respEntry.getKey();

                // Node's response results: consistent id -> map of the handlers results per consistent id.
                nodeResp.customHandlersResults().forEach((consId, respPerConsIdMap) -> {
                    // Reduced map of the handlers results per consistent id for certain node.
                    Map<Object, Map<String, SnapshotHandlerResult<?>>> nodePerConsIdResultMap
                        = reduced.computeIfAbsent(kctx.cluster().get().node(nodeId), n -> new HashMap<>());

                    respPerConsIdMap.forEach((hndId, hndRes) ->
                        nodePerConsIdResultMap.computeIfAbsent(consId, cstId -> new HashMap<>()).put(hndId, hndRes));
                });
            }

            kctx.cache().context().snapshotMgr().checker().checkCustomHandlersResults(ctx.req.snapshotName(), reduced);

            fut.onDone(new SnapshotPartitionsVerifyResult(ctx.clusterMetas, null));
        }
        catch (Throwable err) {
            fut.onDone(err);
        }
    }

    /** */
    private void reducePartitionsHashesResults(
        Map<ClusterNode, List<SnapshotMetadata>> clusterMetas,
        Map<UUID, AbstractSnapshotCheckResponse> results,
        Map<UUID, Throwable> errors,
        GridFutureAdapter<SnapshotPartitionsVerifyResult> fut
    ) {
        Map<ClusterNode, Exception> errors0 = mapErrors(errors);

        if (!results.isEmpty()) {
            Map<ClusterNode, Map<PartitionKeyV2, List<PartitionHashRecordV2>>> reduced = new HashMap<>();

            for (Map.Entry<UUID, AbstractSnapshotCheckResponse> respEntry : results.entrySet()) {
                SnapshotCheckPartitionsHashesResponse resp = (SnapshotCheckPartitionsHashesResponse)respEntry.getValue();

                if (resp == null)
                    continue;

                ClusterNode node = kctx.cluster().get().node(respEntry.getKey());

                if (!F.isEmpty(resp.exceptions()))
                    errors0.putIfAbsent(node, asException(F.firstValue(resp.exceptions())));

                // Partitions results map per consistent id for certain node responded.
                resp.partitionsHashes().forEach((consId, partsMapPerConsId) -> {
                    // Reduced node's hashes on certain responded node for certain consistent id.
                    Map<PartitionKeyV2, List<PartitionHashRecordV2>> nodeHashes = reduced.computeIfAbsent(node, map -> new HashMap<>());

                    partsMapPerConsId.forEach((partKey, partHash) -> nodeHashes.computeIfAbsent(partKey, k -> new ArrayList<>())
                        .add(partHash));
                });
            }

            IdleVerifyResultV2 chkRes = SnapshotChecker.reduceHashesResults(reduced, errors0);

            fut.onDone(new SnapshotPartitionsVerifyResult(clusterMetas, chkRes));
        }
        else
            fut.onDone(new IgniteSnapshotVerifyException(errors0));
    }

    /** Phase 2 beginning.  */
    private IgniteInternalFuture<AbstractSnapshotCheckResponse> validateParts(SnapshotCheckProcessRequest req) {
        if (!req.nodes().contains(kctx.localNodeId()))
            return new GridFinishedFuture<>();

        SnapshotCheckContext ctx = context(req.snapshotName(), req.requestId());

        assert ctx != null;

        if (F.isEmpty(ctx.metas))
            return new GridFinishedFuture<>();

        GridFutureAdapter<AbstractSnapshotCheckResponse> phaseFut = ctx.phaseFuture();

        // Might be already finished by asynchronous leave of a required node.
        if (!phaseFut.isDone()) {
            CompletableFuture<? extends AbstractSnapshotCheckResponse> workingFut;

            if (req.incrementalIndex() > 0) {
                assert !req.allRestoreHandlers() : "Snapshot handlers aren't supported for incremental snapshot.";

                workingFut = incrementalFuture(ctx);
            }
            else if (req.allRestoreHandlers())
                workingFut = allHandlersFuture(ctx);
            else
                workingFut = partitionsHashesFuture(ctx);

            workingFut.whenComplete((res, err) -> {
                if (err != null)
                    phaseFut.onDone(err);
                else
                    phaseFut.onDone(res);
            });
        }

        return phaseFut;
    }

    /** @return A composed future of increment checks for each meta/consistent id regarding {@link SnapshotCheckContext#metas}. */
    private CompletableFuture<SnapshotCheckIncrementalResponse> incrementalFuture(SnapshotCheckContext ctx) {
        assert !F.isEmpty(ctx.metas);

        // Per metas result: consistent id -> check result.
        Map<String, IncrementalSnapshotCheckResult> perMetaResults = new ConcurrentHashMap<>(ctx.metas.size(), 1.0f);
        // Exceptions: consistent id -> exceptions.
        Map<String, Throwable> exceptions = new ConcurrentHashMap<>(ctx.metas.size(), 1.0f);
        AtomicInteger metasProcessed = new AtomicInteger(ctx.metas.size());
        CompletableFuture<SnapshotCheckIncrementalResponse> composedFut = new CompletableFuture<>();

        for (SnapshotMetadata meta : ctx.metas) {
            CompletableFuture<IncrementalSnapshotCheckResult> workingFut = kctx.cache().context().snapshotMgr().checker()
                .checkIncrementalSnapshot(ctx.req.snapshotName(),
                    ctx.req.snapshotPath(), ctx.req.incrementalIndex());

            workingFut.whenComplete((res, err) -> {
                if (err != null)
                    exceptions.put(meta.consistentId(), err);
                else
                    perMetaResults.put(meta.consistentId(), res);

                if (metasProcessed.decrementAndGet() == 0)
                    composedFut.complete( new SnapshotCheckIncrementalResponse(perMetaResults, exceptions));
            });
        }

        return composedFut;
    }

    /** @return A composed future of partitions checks for each meta/consistent id regarding {@link SnapshotCheckContext#metas}. */
    private CompletableFuture<SnapshotCheckPartitionsHashesResponse> partitionsHashesFuture(SnapshotCheckContext ctx) {
        assert !F.isEmpty(ctx.metas);

        // Per metas result: consistent id -> check results per partition key.
        Map<String, Map<PartitionKeyV2, PartitionHashRecordV2>> perMetaResults = new ConcurrentHashMap<>(ctx.metas.size(), 1.0f);
        // Exceptions: consistent id -> exceptions.
        Map<String, Throwable> exceptions = new ConcurrentHashMap<>(ctx.metas.size(), 1.0f);
        CompletableFuture<SnapshotCheckPartitionsHashesResponse> composedFut = new CompletableFuture<>();
        AtomicInteger metasProcessed = new AtomicInteger(ctx.metas.size());

        IgniteSnapshotManager snpMgr = kctx.cache().context().snapshotMgr();

        for (SnapshotMetadata meta : ctx.metas) {
            CompletableFuture<Map<PartitionKeyV2, PartitionHashRecordV2>> metaFut = snpMgr.checker().checkPartitions(
                meta,
                snpMgr.snapshotLocalDir(ctx.req.snapshotName(), ctx.req.snapshotPath()),
                ctx.req.groups(),
                false,
                ctx.req.fullCheck(),
                false
            );

            metaFut.whenComplete((res, err) -> {
                if (err != null)
                    exceptions.put(meta.consistentId(), err);
                else if (!F.isEmpty(res))
                    perMetaResults.put(meta.consistentId(), res);

                if (metasProcessed.decrementAndGet() == 0)
                    composedFut.complete(new SnapshotCheckPartitionsHashesResponse(perMetaResults, exceptions));
            });
        }

        return composedFut;
    }

    /**
     * @return A composed future of all the snapshot handlers for each meta/consistent id regarding {@link SnapshotCheckContext#metas}.
     * @see IgniteSnapshotManager#handlers()
     */
    private CompletableFuture<SnapshotCheckCustomHandlersResponse> allHandlersFuture(SnapshotCheckContext ctx) {
        assert !F.isEmpty(ctx.metas);

        // Per metas result: consistent id -> check result per handler id.
        Map<String, Map<String, SnapshotHandlerResult<Object>>> perMetaResults = new ConcurrentHashMap<>(ctx.metas.size(), 1.0f);
        // Exceptions: consistent id -> exceptions.
        Map<String, Throwable> exceptions = new ConcurrentHashMap<>(ctx.metas.size(), 1.0f);
        CompletableFuture<SnapshotCheckCustomHandlersResponse> composedFut = new CompletableFuture<>();
        AtomicInteger metasProcessed = new AtomicInteger(ctx.metas.size());

        for (SnapshotMetadata meta : ctx.metas) {
            CompletableFuture<Map<String, SnapshotHandlerResult<Object>>> metaFut = kctx.cache().context().snapshotMgr().checker()
                .invokeCustomHandlers(meta, ctx.req.snapshotPath(), ctx.req.groups(), true);

            metaFut.whenComplete((res, err) -> {
                if (err != null)
                    exceptions.put(meta.consistentId(), err);
                else if (!F.isEmpty(res))
                    perMetaResults.put(meta.consistentId(), res);

                if (metasProcessed.decrementAndGet() == 0)
                    composedFut.complete(new SnapshotCheckCustomHandlersResponse(perMetaResults, exceptions));
            });
        }

        return composedFut;
    }

    /** */
    private Map<ClusterNode, Exception> mapErrors(Map<UUID, Throwable> errors) {
        return errors.entrySet().stream()
            .collect(Collectors.toMap(e -> kctx.cluster().get().node(e.getKey()), e -> asException(e.getValue())));
    }

    /** */
    private static Exception asException(Throwable th) {
        return th instanceof Exception ? (Exception)th : new IgniteException(th);
    }

    /**
     * @param snpName Snapshot name. If {@code null}, ignored.
     * @param reqId If {@code ctxId} is {@code null}, is used to find the operation context.
     * @return Current snapshot checking context by {@code ctxId} or {@code reqId}.
     */
    private @Nullable SnapshotCheckContext context(@Nullable String snpName, UUID reqId) {
        return snpName == null
            ? contexts.values().stream().filter(ctx0 -> ctx0.req.requestId().equals(reqId)).findFirst().orElse(null)
            : contexts.get(snpName);
    }

    /** Phase 1 beginning: prepare, collect and check local metas. */
    private IgniteInternalFuture<SnapshotCheckMetasResponse> prepareAndCheckMetas(SnapshotCheckProcessRequest req) {
        if (!req.nodes().contains(kctx.localNodeId()))
            return new GridFinishedFuture<>();

        SnapshotCheckContext ctx;

        // Sync. with stopping in #interrupt.
        synchronized (contexts) {
            if (nodeStopping)
                return new GridFinishedFuture<>(new NodeStoppingException("The node is stopping: " + kctx.localNodeId()));

            ctx = contexts.computeIfAbsent(req.snapshotName(), snpName -> new SnapshotCheckContext(req));
        }

        if (!ctx.req.requestId().equals(req.requestId())) {
            return new GridFinishedFuture<>(new IllegalStateException("Validation of snapshot '" + req.snapshotName()
                + "' has already started. Request=" + ctx + '.'));
        }

        // Excludes non-baseline initiator.
        if (!baseline(kctx.localNodeId()))
            return new GridFinishedFuture<>();

        IgniteSnapshotManager snpMgr = kctx.cache().context().snapshotMgr();

        Collection<Integer> grpIds = F.isEmpty(req.groups()) ? null : F.viewReadOnly(req.groups(), CU::cacheId);

        GridFutureAdapter<SnapshotCheckMetasResponse> phaseFut = ctx.phaseFuture();

        // Might be already finished by asynchronous leave of a required node.
        if (!phaseFut.isDone()) {
            snpMgr.checker().checkLocalMetas(
                snpMgr.snapshotLocalDir(req.snapshotName(), req.snapshotPath()),
                req.incrementalIndex(),
                grpIds,
                kctx.cluster().get().localNode().consistentId()
            ).whenComplete((locMetas, err) -> {
                if (err != null)
                    phaseFut.onDone(err);
                else
                    phaseFut.onDone(new SnapshotCheckMetasResponse(locMetas));
            });
        }

        return phaseFut;
    }

    /** Phase 1 end. */
    private void reducePreparationAndMetasCheck(
        UUID reqId,
        Map<UUID, SnapshotCheckMetasResponse> results,
        Map<UUID, Throwable> errors
    ) {
        SnapshotCheckContext ctx = context(null, reqId);

        // The context is not stored in the case of concurrent check of the same snapshot but the operation future is registered.
        GridFutureAdapter<SnapshotPartitionsVerifyResult> clusterOpFut = clusterOpFuts.get(reqId);

        try {
            if (!errors.isEmpty())
                throw new IgniteSnapshotVerifyException(mapErrors(errors));

            if (ctx == null) {
                assert clusterOpFut == null;

                return;
            }

            if (ctx.locProcFut.error() != null)
                throw ctx.locProcFut.error();

            Map<ClusterNode, List<SnapshotMetadata>> metas = new HashMap<>();

            results.forEach((nodeId, nodeRes) -> {
                // A node might be not required. It gives null result. But a required node might have invalid empty result
                // which must be validated.
                if (ctx.req.nodes().contains(nodeId) && baseline(nodeId) && !F.isEmpty(nodeRes.metas())) {
                    assert nodeRes != null;

                    metas.put(kctx.cluster().get().node(nodeId), nodeRes.metas());
                }
            });

            Map<ClusterNode, Exception> metasCheck = SnapshotChecker.reduceMetasResults(ctx.req.snapshotName(), ctx.req.snapshotPath(),
                metas, null, kctx.cluster().get().localNode().consistentId());

            if (!metasCheck.isEmpty())
                throw new IgniteSnapshotVerifyException(metasCheck);

            // If the topology is lesser that the snapshot's, we have to check partitions not only of current node.
            ctx.metas = assingMetasToWork(metas);

            if (clusterOpFut != null)
                ctx.clusterMetas = metas;

            if (U.isLocalNodeCoordinator(kctx.discovery()))
                phase2PartsHashes.start(reqId, ctx.req);
        }
        catch (Throwable th) {
            if (ctx != null) {
                contexts.remove(ctx.req.snapshotName());

                if (log.isInfoEnabled())
                    log.info("Finished snapshot validation [req=" + ctx.req + ']');
            }

            if (clusterOpFut != null)
                clusterOpFut.onDone(th);
        }
    }

    /**
     * Assigns metadatas to process. A snapshot can be checked on a smaller topology compared to the original one. In this case,
     * some node has to check not only own partitions.
     *
     * @return Metadatas to process on current node if more than one found.
     */
    private @Nullable List<SnapshotMetadata> assingMetasToWork(Map<ClusterNode, List<SnapshotMetadata>> clusterMetas) {
        ClusterNode locNode = kctx.cluster().get().localNode();
        List<SnapshotMetadata> locMetas = clusterMetas.get(locNode);

        if (F.isEmpty(locMetas))
            return null;

        // Nodes sorted by lessser order.
        Map<String, Collection<ClusterNode>> metasPerRespondedNodes = new HashMap<>();

        Set<String> onlineNodesConstIdsStr = new HashSet<>(clusterMetas.size());

        clusterMetas.forEach((node, nodeMetas) -> {
            if (!F.isEmpty(nodeMetas)) {
                onlineNodesConstIdsStr.add(node.consistentId().toString());

                nodeMetas.forEach(nodeMeta -> metasPerRespondedNodes.computeIfAbsent(nodeMeta.consistentId(),
                    m -> new TreeSet<>(Comparator.comparingLong(ClusterNode::order))).add(node));
            }
        });

        String locNodeConsIdStr = locNode.consistentId().toString();

        List<SnapshotMetadata> metasToProc = new ArrayList<>(1);

        for (SnapshotMetadata locMeta : locMetas) {
            if (locMeta.consistentId().equals(locNodeConsIdStr)) {
                assert !metasToProc.contains(locMeta) : "Local snapshot metadata is already assigned to process";

                metasToProc.add(locMeta);

                continue;
            }

            if (!onlineNodesConstIdsStr.contains(locMeta.consistentId())
                && F.first(metasPerRespondedNodes.get(locMeta.consistentId())).id().equals(kctx.localNodeId()))
                metasToProc.add(locMeta);
        }

        return metasToProc;
    }

    /**
     * Starts the snapshot validation process.
     *
     * @param snpName Snapshot name.
     * @param snpPath Snapshot directory path.
     * @param grpNames List of cache group names.
     * @param fullCheck If {@code true}, additionally calculates partition hashes. Otherwise, checks only snapshot integrity
     *                  and partition counters.
     * @param incIdx Incremental snapshot index. If not positive, snapshot is not considered as incremental.
     * @param allRestoreHandlers If {@code true}, all the registered {@link IgniteSnapshotManager#handlers()} of type
     *                    {@link SnapshotHandlerType#RESTORE} are invoked. Otherwise, only snapshot metadatas and partition
     *                    hashes are validated.
     */
    public IgniteInternalFuture<SnapshotPartitionsVerifyResult> start(
        String snpName,
        @Nullable String snpPath,
        @Nullable Collection<String> grpNames,
        boolean fullCheck,
        int incIdx,
        boolean allRestoreHandlers
    ) {
        assert !F.isEmpty(snpName);

        UUID reqId = UUID.randomUUID();

        Set<UUID> requiredNodes = new HashSet<>(F.viewReadOnly(kctx.discovery().discoCache().aliveBaselineNodes(), F.node2id()));

        // Initiator is also a required node. It collects the final operation result.
        requiredNodes.add(kctx.localNodeId());

        SnapshotCheckProcessRequest req = new SnapshotCheckProcessRequest(
            reqId,
            requiredNodes,
            snpName,
            snpPath,
            grpNames,
            fullCheck,
            incIdx,
            allRestoreHandlers
        );

        GridFutureAdapter<SnapshotPartitionsVerifyResult> clusterOpFut = new GridFutureAdapter<>();

        clusterOpFut.listen(fut -> {
            clusterOpFuts.remove(reqId);

            if (log.isInfoEnabled())
                log.info("Finished snapshot validation process [req=" + req + ']');
        });

        clusterOpFuts.put(reqId, clusterOpFut);

        phase1CheckMetas.start(req.requestId(), req);

        return clusterOpFut;
    }

    /** @return {@code True} if node with the provided id is in the cluster and is a baseline node. {@code False} otherwise. */
    private boolean baseline(UUID nodeId) {
        ClusterNode node = kctx.cluster().get().node(nodeId);

        return node != null && CU.baselineNode(node, kctx.state().clusterState());
    }

    /** Operation context. */
    private static final class SnapshotCheckContext {
        /** Request. */
        private final SnapshotCheckProcessRequest req;

        /** Current process' future. Listens error, stop requests, etc. */
        private final GridFutureAdapter<AbstractSnapshotCheckResponse> locProcFut = new GridFutureAdapter<>();

        /**
         * Metadatas to process on this node. Also indicates the snapshot parts to check on this node.
         * @see #partitionsHashesFuture(SnapshotCheckContext)
         */
        @Nullable private List<SnapshotMetadata> metas;

        /** All the snapshot metadatas. */
        @Nullable private Map<ClusterNode, List<SnapshotMetadata>> clusterMetas;

        /** Creates operation context. */
        private SnapshotCheckContext(SnapshotCheckProcessRequest req) {
            this.req = req;
        }

        /** Gives a future for current process phase. The future can be stopped by asynchronous leave of a required node. */
        private <T> GridFutureAdapter<T> phaseFuture() {
            GridFutureAdapter<T> fut = new GridFutureAdapter<>();

            locProcFut.listen(f -> fut.onDone(f.error()));

            return fut;
        }
    }

    /** A DTO base to transfer nodes' results for the both phases. */
    private abstract static class AbstractSnapshotCheckResponse implements Serializable {
        /** The result. */
        protected Object result;

        /** Exceptions per consistent id. */
        @Nullable private Map<String, Throwable> exceptions;

        /** */
        private AbstractSnapshotCheckResponse(Object result, @Nullable Map<String, Throwable> exceptions) {
            assert result instanceof Serializable : "Snapshot check result is not serializable.";
            assert exceptions == null || exceptions instanceof Serializable : "Snapshot check exceptions aren't serializable.";

            this.result = result;
            this.exceptions = exceptions == null ? null : Collections.unmodifiableMap(exceptions);
        }

        /** @return Exceptions per consistent id. */
        protected @Nullable Map<String, Throwable> exceptions() {
            return exceptions;
        }
    }

    /** A DTO to transfer metas result for phase 1. */
    private static final class SnapshotCheckMetasResponse extends AbstractSnapshotCheckResponse {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** */
        private SnapshotCheckMetasResponse(List<SnapshotMetadata> result) {
            super(result, null);
        }

        /** @return All the snapshot metadatatas found on this node. */
        private List<SnapshotMetadata> metas() {
            return (List<SnapshotMetadata>)result;
        }
    }

    /** A DTO used to transfer partition hashes result for phase 2. */
    private static final class SnapshotCheckPartitionsHashesResponse extends AbstractSnapshotCheckResponse {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param result Partitions results per consistent id.
         * @param exceptions Exceptions per consistent id.
         */
        private SnapshotCheckPartitionsHashesResponse(
            Map<String, Map<PartitionKeyV2, PartitionHashRecordV2>> result,
            Map<String, Throwable> exceptions
        ) {
            super(result, exceptions);
        }

        /** @return Partitions hashes per consistent id. */
        private Map<String, Map<PartitionKeyV2, PartitionHashRecordV2>> partitionsHashes() {
            return (Map<String, Map<PartitionKeyV2, PartitionHashRecordV2>>)result;
        }
    }

    /**
     * A DTO used to transfer all the handlers results for phase 2.
     *
     * @see IgniteSnapshotManager#handlers().
     */
    private static final class SnapshotCheckCustomHandlersResponse extends AbstractSnapshotCheckResponse {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param result Handlers results per consistent id.
         * @param exceptions Exceptions per consistent id.
         */
        private SnapshotCheckCustomHandlersResponse(
            Map<String, Map<String, SnapshotHandlerResult<Object>>> result,
            Map<String, Throwable> exceptions
        ) {
            super(result, exceptions);
        }

        /** @return Handlers results per consistent id. */
        private Map<String, Map<String, SnapshotHandlerResult<Object>>> customHandlersResults() {
            return (Map<String, Map<String, SnapshotHandlerResult<Object>>>)result;
        }
    }

    /** A DTO used to transfer incremental snapshot check result for phase 2. */
    private static final class SnapshotCheckIncrementalResponse extends AbstractSnapshotCheckResponse {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param result Incremental snapshot check result per consistent id.
         * @param exceptions Exceptions per consistent id.
         */
        private SnapshotCheckIncrementalResponse(
            Map<String, IncrementalSnapshotCheckResult> result,
            Map<String, Throwable> exceptions
        ) {
            super(result, exceptions);
        }

        /** @return Incremental snapshot check results per consistent id. */
        private Map<String, IncrementalSnapshotCheckResult> incremenlatSnapshotResults() {
            return (Map<String, IncrementalSnapshotCheckResult>)result;
        }
    }
}
