/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.LIST_SNAPSHOTS;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_SNAPSHOT;

/**
 * Distributed process to list all snapshots and their sizes on all online server nodes.
 */
public class SnapshotListProcess {
    /** Status: snapshot is in process of creation. */
    static final String STATUS_CREATING = "CREATING";

    /** Status: snapshot is in process of deletion. */
    static final String STATUS_DELETING = "DELETING";

    /** Status: snapshot is in process of restore. */
    static final String STATUS_RESTORING = "RESTORING";

    /** Kernal context. */
    private final GridKernalContext kctx;

    /** Logger. */
    private final IgniteLogger log;

    /** */
    private volatile boolean interrupted;

    /** Cluster-wide operation futures per request id on certain node. */
    private final Map<UUID, GridFutureAdapter<SnapshotListProcessResult>> clusterOpFuts = new ConcurrentHashMap<>();

    /** The distributed process. */
    private final DistributedProcess<SnapshotListRequest, SnapshotListResponse> distrProc;

    /**
     * @param ctx Kernal context.
     */
    public SnapshotListProcess(GridKernalContext ctx) {
        this.kctx = ctx;

        log = ctx.log(getClass());

        distrProc = new DistributedProcess<>(ctx, LIST_SNAPSHOTS, this::listPhase, this::reducePhase);
    }

    /**
     * Starts the cluster snapshot list process.
     *
     * @param snpPath Snapshot directory path (optional).
     * @return Future that will be completed when the snapshot list is gathered from all online server nodes.
     */
    public IgniteFuture<SnapshotListProcessResult> start(@Nullable String snpPath) {
        var clusterOpFut = new GridFutureAdapter<SnapshotListProcessResult>();

        UUID reqId = UUID.randomUUID();

        clusterOpFut.listen(fut -> clusterOpFuts.remove(reqId));

        try {
            synchronized (clusterOpFuts) {
                if (interrupted || kctx.isStopping())
                    throw new NodeStoppingException("Failed to start snapshot list process: node is stopping.");

                clusterOpFuts.put(reqId, clusterOpFut);
            }

            SnapshotListRequest req = new SnapshotListRequest(reqId, snpPath);

            distrProc.start(reqId, req);
        }
        catch (Throwable t) {
            log.error("Failed to start distributed list snapshot process [snpPath=" + snpPath + ']', t);

            clusterOpFut.onDone(t);
        }

        return new IgniteFutureImpl<>(clusterOpFut);
    }

    /** */
    private IgniteInternalFuture<SnapshotListResponse> listPhase(UUID ignored, SnapshotListRequest req) {
        if (interrupted || kctx.isStopping()) {
            return new GridFinishedFuture<>(new NodeStoppingException(
                "Snapshot list was rejected. Node is stopping [req=" + req + ']'));
        }

        if (kctx.cluster().get().localNode().isClient())
            return new GridFinishedFuture<>(new SnapshotListResponse());

        kctx.security().authorize(ADMIN_SNAPSHOT);

        IgniteSnapshotManager snpMgr = kctx.cache().context().snapshotMgr();

        try {
            File path = resolvePath(req.snpPath);

            File[] dirs;

            if (path.exists() && path.isDirectory())
                dirs = path.listFiles(File::isDirectory);
            else
                dirs = null;

            Map<String, Long> sizes = new HashMap<>();
            Map<String, String> statuses = new HashMap<>();

            if (dirs != null) {
                for (File dir : dirs) {
                    String snpName = dir.getName();

                    if (isSnapshotCreating(snpMgr, snpName) || isSnapshotRestoring(snpMgr, snpName)) {
                        statuses.put(snpName, STATUS_CREATING);
                    }
                    else if (snpMgr.isSnapshotDeleting(snpName, req.snpPath)) {
                        statuses.put(snpName, STATUS_DELETING);
                    }
                    else {
                        try {
                            sizes.put(snpName, directorySize(dir));
                        }
                        catch (IOException e) {
                            // Snapshot might be modified between listing and size calculation.
                            statuses.put(snpName, STATUS_DELETING);
                        }
                    }
                }
            }

            return new GridFinishedFuture<>(new SnapshotListResponse(sizes, statuses));
        }
        catch (Throwable t) {
            log.warning("An error occurred during snapshot listing [req=" + req + ']', t);

            return new GridFinishedFuture<>(t);
        }
    }

    /** */
    private boolean isSnapshotCreating(IgniteSnapshotManager snpMgr, String snpName) {
        var curCreateRq = snpMgr.currentCreateRequest();

        return curCreateRq != null && curCreateRq.snpName.equalsIgnoreCase(snpName);
    }

    /** */
    private boolean isSnapshotRestoring(IgniteSnapshotManager snpMgr, String snpName) {
        return snpMgr.isRestoring(snpName);
    }

    /** */
    private File resolvePath(@Nullable String path) {
        var res = kctx.pdsFolderResolver().fileTree().snapshotsRoot();

        if (path != null) {
            File reqPath = new File(path);

            res = reqPath.isAbsolute() ? reqPath : new File(res, path);
        }

        return res;
    }

    /**
     * Calculates total size of all files in the given directory recursively.
     *
     * @param dir Directory to measure.
     * @return Total size in bytes.
     * @throws IOException If an I/O error occurs during traversal.
     */
    private long directorySize(File dir) throws IOException {
        Path path = dir.toPath();

        long[] size = {0L};

        Files.walkFileTree(path, new SimpleFileVisitor<>() {
            /** {@inheritDoc} */
            @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                size[0] += attrs.size();

                return FileVisitResult.CONTINUE;
            }

            /** {@inheritDoc} */
            @Override public FileVisitResult visitFileFailed(Path file, IOException exc) {
                // File might have been deleted between listing and traversal.
                return FileVisitResult.CONTINUE;
            }
        });

        return size[0];
    }

    /** */
    private void reducePhase(UUID reqId, Map<UUID, SnapshotListResponse> results, Map<UUID, Throwable> errors) {
        var clusterOpFut = clusterOpFuts.get(reqId);

        if (clusterOpFut == null)
            return;

        assert clusterOpFut != null;

        try {
            var errP = F.isEmpty(errors) ? null : F.first(errors.entrySet());

            if (errP != null) {
                log.warning("Snapshot listing finished with an error [reqId=" + reqId + ", nodeId="
                    + errP.getKey() + ", err='" + errP.getValue().getMessage() + "']", errP.getValue());

                clusterOpFut.onDone(errP.getValue());

                return;
            }

            Map<UUID, Map<String, Long>> nodeSizes = new HashMap<>();
            Map<UUID, Map<String, String>> nodeStatuses = new HashMap<>();
            Map<UUID, String> nodeConsistentIds = new HashMap<>();

            results.forEach((nodeId, nodeRes) -> {
                nodeConsistentIds.put(nodeId, consistentId(nodeId));

                if (nodeRes.snapshotSizes != null && !nodeRes.snapshotSizes.isEmpty())
                    nodeSizes.put(nodeId, nodeRes.snapshotSizes);

                if (nodeRes.snapshotStatuses != null && !nodeRes.snapshotStatuses.isEmpty())
                    nodeStatuses.put(nodeId, nodeRes.snapshotStatuses);
            });

            clusterOpFut.onDone(new SnapshotListProcessResult(nodeSizes, nodeStatuses, nodeConsistentIds));
        }
        catch (Throwable t) {
            clusterOpFut.onDone(t);
        }
    }

    /** */
    private String consistentId(UUID nodeId) {
        var node = kctx.discovery().node(nodeId);

        if (node == null)
            node = kctx.discovery().historicalNode(nodeId);

        return node == null ? "" : node.consistentId().toString();
    }

    /**
     * @param err The interrupt reason.
     */
    void interrupt(Throwable err) {
        synchronized (clusterOpFuts) {
            interrupted = true;
        }

        clusterOpFuts.forEach((reqId, clusterOpFut) -> clusterOpFut.onDone(err));

        clusterOpFuts.clear();
    }
}
