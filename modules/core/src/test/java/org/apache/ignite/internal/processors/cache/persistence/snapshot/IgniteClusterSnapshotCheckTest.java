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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridJobExecuteRequest;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.management.cache.CacheFilterEnum;
import org.apache.ignite.internal.management.cache.CacheIdleVerifyCommandArg;
import org.apache.ignite.internal.management.cache.IdleVerifyResultV2;
import org.apache.ignite.internal.management.cache.PartitionKeyV2;
import org.apache.ignite.internal.management.cache.VerifyBackupPartitionsTaskV2;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.distributed.FullMessage;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SNAPSHOT_DIRECTORY;
import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.TTL_ETERNAL;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFileName;
import static org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId.getTypeByPartId;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_METAFILE_EXT;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.databaseRelativePath;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_START;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.SNAPSHOT_CHECK_METAS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.SNAPSHOT_VALIDATE_PARTS;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Cluster-wide snapshot check procedure tests.
 */
public class IgniteClusterSnapshotCheckTest extends AbstractSnapshotSelfTest {
    /** Map of intermediate compute task results collected prior performing reduce operation on them. */
    private final Map<Class<?>, Map<PartitionKeyV2, List<PartitionHashRecordV2>>> jobResults = new ConcurrentHashMap<>();

    /** Partition id used for tests. */
    private static final int PART_ID = 0;

    /** Optional cache name to be created on demand. */
    private static final String OPTIONAL_CACHE_NAME = "CacheName";

    /** Cleanup data of task execution results if need. */
    @Before
    public void beforeCheck() {
        jobResults.clear();
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheck() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg, CACHE_KEYS_RANGE);

        startClientGrid();

        createAndCheckSnapshot(ignite, SNAPSHOT_NAME);

        IdleVerifyResultV2 res = snp(ignite).checkSnapshot(SNAPSHOT_NAME, null).get().idleVerifyResult();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        assertTrue(F.isEmpty(res.exceptions()));
        assertPartitionsSame(res);
        assertContains(log, b.toString(), "The check procedure has finished, no conflicts have been found");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckMissedPart() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg, CACHE_KEYS_RANGE);

        createAndCheckSnapshot(ignite, SNAPSHOT_NAME);

        Path part0 = U.searchFileRecursively(snp(ignite).snapshotLocalDir(SNAPSHOT_NAME).toPath(),
            getPartitionFileName(0));

        assertNotNull(part0);
        assertTrue(part0.toString(), part0.toFile().exists());
        assertTrue(part0.toFile().delete());

        IdleVerifyResultV2 res = snp(ignite).checkSnapshot(SNAPSHOT_NAME, null).get().idleVerifyResult();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        assertFalse(F.isEmpty(res.exceptions()));
        assertContains(log, b.toString(), "Snapshot data doesn't contain required cache group partition");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckMissedGroup() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg, CACHE_KEYS_RANGE);

        createAndCheckSnapshot(ignite, SNAPSHOT_NAME);

        Path dir = Files.walk(snp(ignite).snapshotLocalDir(SNAPSHOT_NAME).toPath())
            .filter(d -> d.toFile().getName().equals(cacheDirName(dfltCacheCfg)))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Cache directory not found"));

        assertTrue(dir.toString(), dir.toFile().exists());
        assertTrue(U.delete(dir));

        IdleVerifyResultV2 res = snp(ignite).checkSnapshot(SNAPSHOT_NAME, null).get().idleVerifyResult();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        assertFalse(F.isEmpty(res.exceptions()));
        assertContains(log, b.toString(), "Snapshot data doesn't contain required cache groups");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckMissedMeta() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg, CACHE_KEYS_RANGE);

        createAndCheckSnapshot(ignite, SNAPSHOT_NAME);

        File[] smfs = snp(ignite).snapshotLocalDir(SNAPSHOT_NAME).listFiles((dir, name) ->
            name.toLowerCase().endsWith(SNAPSHOT_METAFILE_EXT));

        assertNotNull(smfs);
        assertTrue(smfs[0].toString(), smfs[0].exists());
        assertTrue(U.delete(smfs[0]));

        assertThrowsAnyCause(
            log,
            () -> snp(ignite).checkSnapshot(SNAPSHOT_NAME, null).get().idleVerifyResult(),
            IgniteException.class,
            "No snapshot metadatas found for the baseline nodes with consistent ids: "
        );
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckWithNodeFilter() throws Exception {
        IgniteEx ig0 = startGridsWithoutCache(3);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++) {
            ig0.getOrCreateCache(txCacheConfig(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME))
                .setNodeFilter(node -> node.consistentId().toString().endsWith("0"))).put(i, i);
        }

        createAndCheckSnapshot(ig0, SNAPSHOT_NAME);

        IdleVerifyResultV2 res = snp(ig0).checkSnapshot(SNAPSHOT_NAME, null).get().idleVerifyResult();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        assertTrue(F.isEmpty(res.exceptions()));
        assertPartitionsSame(res);
        assertContains(log, b.toString(), "The check procedure has finished, no conflicts have been found");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckPartitionCounters() throws Exception {
        Assume.assumeFalse("One copy of partiton created in only primary mode", onlyPrimary);

        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg.
            setAffinity(new RendezvousAffinityFunction(false, 1)),
            CACHE_KEYS_RANGE);

        createAndCheckSnapshot(ignite, SNAPSHOT_NAME);

        Path part0 = U.searchFileRecursively(snp(ignite).snapshotLocalDir(SNAPSHOT_NAME).toPath(),
            getPartitionFileName(PART_ID));

        assertNotNull(part0);
        assertTrue(part0.toString(), part0.toFile().exists());

        int grpId = CU.cacheId(dfltCacheCfg.getName());

        try (FilePageStore pageStore = (FilePageStore)((FilePageStoreManager)ignite.context().cache().context().pageStore())
            .getPageStoreFactory(grpId, ignite.context().cache().isEncrypted(grpId))
            .createPageStore(getTypeByPartId(PART_ID),
                () -> part0,
                val -> {
                })
        ) {
            ByteBuffer buff = ByteBuffer.allocateDirect(ignite.configuration().getDataStorageConfiguration().getPageSize())
                .order(ByteOrder.nativeOrder());

            buff.clear();
            pageStore.read(0, buff, false);

            long pageAddr = GridUnsafe.bufferAddress(buff);

            boolean shouldCompress = false;
            if (PageIO.getCompressionType(pageAddr) != CompressionProcessor.UNCOMPRESSED_PAGE) {
                shouldCompress = true;

                ignite.context().compress().decompressPage(buff, pageStore.getPageSize());
            }

            PagePartitionMetaIO io = PageIO.getPageIO(buff);

            io.setUpdateCounter(pageAddr, CACHE_KEYS_RANGE * 2);

            if (shouldCompress) {
                CacheGroupContext grpCtx = ignite.context().cache().cacheGroup(grpId);

                assertNotNull("Group context for grpId:" + grpId, grpCtx);

                ByteBuffer compressedPageBuf = grpCtx.compressionHandler().compressPage(buff, pageStore);

                if (compressedPageBuf != buff) {
                    buff = compressedPageBuf;

                    PageIO.setCrc(buff, 0);
                }
            }
            else
                buff.flip();

            pageStore.beginRecover();
            pageStore.write(PageIO.getPageId(buff), buff, 0, true);
            pageStore.finishRecover();
        }

        IdleVerifyResultV2 res = snp(ignite).checkSnapshot(SNAPSHOT_NAME, null).get().idleVerifyResult();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        assertTrue(F.isEmpty(res.exceptions()));
        assertContains(log, b.toString(),
            "The check procedure has failed, conflict partitions has been found: [counterConflicts=1, hashConflicts=0]");
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testClusterSnapshotCheckOtherCluster() throws Exception {
        IgniteEx ig0 = startGridsWithCache(3, dfltCacheCfg.
                setAffinity(new RendezvousAffinityFunction(false, 1)),
            CACHE_KEYS_RANGE);

        createAndCheckSnapshot(ig0, SNAPSHOT_NAME);

        stopAllGrids();

        // Cleanup persistence directory except created snapshots.
        Arrays.stream(new File(U.defaultWorkDirectory()).listFiles())
            .filter(f -> !f.getName().equals(DFLT_SNAPSHOT_DIRECTORY))
            .forEach(U::delete);

        Set<UUID> assigns = Collections.newSetFromMap(new ConcurrentHashMap<>());

        for (int i = 4; i < 7; i++) {
            startGrid(optimize(getConfiguration(getTestIgniteInstanceName(i)).setCacheConfiguration()));

            UUID locNodeId = grid(i).localNode().id();

            grid(i).context().io().addMessageListener(GridTopic.TOPIC_JOB, new GridMessageListener() {
                @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                    if (msg instanceof GridJobExecuteRequest) {
                        GridJobExecuteRequest msg0 = (GridJobExecuteRequest)msg;

                        if (msg0.getTaskName().contains(SnapshotPartitionsVerifyTask.class.getName()))
                            assigns.add(locNodeId);
                    }
                }
            });
        }

        IgniteEx ignite = grid(4);
        ignite.cluster().baselineAutoAdjustEnabled(false);
        ignite.cluster().state(ACTIVE);

        IdleVerifyResultV2 res = snp(ignite).checkSnapshot(SNAPSHOT_NAME, null, null, false, 0, false).get().idleVerifyResult();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        // GridJobExecuteRequest is not send to the local node.
        assertTrue("Number of jobs must be equal to the cluster size (except local node): " + assigns + ", count: "
                + assigns.size(), waitForCondition(() -> assigns.size() == 2, 5_000L));

        assertTrue(F.isEmpty(res.exceptions()));
        assertPartitionsSame(res);
        assertContains(log, b.toString(), "The check procedure has finished, no conflicts have been found");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckCRCFail() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg.
                setAffinity(new RendezvousAffinityFunction(false, 1)), CACHE_KEYS_RANGE);

        createAndCheckSnapshot(ignite, SNAPSHOT_NAME);

        corruptPartitionFile(ignite, SNAPSHOT_NAME, dfltCacheCfg, PART_ID);

        IdleVerifyResultV2 res = snp(ignite).checkSnapshot(SNAPSHOT_NAME, null, null, false, -1, false).get().idleVerifyResult();

        assertEquals("Check must be disabled", 0, res.exceptions().size());

        res = snp(ignite).checkSnapshot(SNAPSHOT_NAME, null, null, false, -1, true).get().idleVerifyResult();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        assertEquals(1, res.exceptions().size());
        assertContains(log, b.toString(), "The check procedure failed on 1 node.");

        Exception ex = res.exceptions().values().iterator().next();
        assertTrue(X.hasCause(ex, IgniteDataIntegrityViolationException.class));
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckFailsOnPartitionDataDiffers() throws Exception {
        Assume.assumeFalse("One copy of partiton created in only primary mode", onlyPrimary);

        CacheConfiguration<Integer, Value> ccfg = txCacheConfig(new CacheConfiguration<Integer, Value>(DEFAULT_CACHE_NAME))
            .setAffinity(new RendezvousAffinityFunction(false, 1));

        IgniteEx ignite = startGridsWithoutCache(2);

        ignite.getOrCreateCache(ccfg).put(1, new Value(new byte[2000]));

        forceCheckpoint(ignite);

        GridCacheSharedContext<?, ?> cctx = ignite.context().cache().context();
        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)cctx.database();

        BinaryContext binCtx = ((CacheObjectBinaryProcessorImpl)ignite.context().cacheObjects()).binaryContext();

        GridCacheAdapter<?, ?> cache = ignite.context().cache().internalCache(dfltCacheCfg.getName());
        long partCtr = cache.context().topology().localPartition(PART_ID, NONE, false)
            .dataStore()
            .updateCounter();
        AtomicBoolean done = new AtomicBoolean();

        db.addCheckpointListener(new CheckpointListener() {
            @Override public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {
                // Change the cache value only at on of the cluster node to get hash conflict when the check command ends.
                if (!done.compareAndSet(false, true))
                    return;

                GridIterator<CacheDataRow> it = cache.context().offheap().partitionIterator(PART_ID);

                assertTrue(it.hasNext());

                CacheDataRow row0 = it.nextX();

                AffinityTopologyVersion topVer = cctx.exchange().readyAffinityVersion();
                GridCacheEntryEx cached = cache.entryEx(row0.key(), topVer);

                byte[] bytes = new byte[2000];
                new Random().nextBytes(bytes);

                try {
                    BinaryObjectImpl newVal = new BinaryObjectImpl(binCtx, binCtx.marshaller().marshal(new Value(bytes)), 0);

                    boolean success = cached.initialValue(
                        newVal,
                        new GridCacheVersion(row0.version().topologyVersion(),
                            row0.version().nodeOrder(),
                            row0.version().order() + 1),
                        TTL_ETERNAL,
                        row0.expireTime(),
                        true,
                        topVer,
                        DR_NONE,
                        false,
                        false,
                        null);

                    assertTrue(success);

                    long newPartCtr = cache.context().topology().localPartition(PART_ID, NONE, false)
                        .dataStore()
                        .updateCounter();

                    assertEquals(newPartCtr, partCtr);
                }
                catch (Exception e) {
                    throw new IgniteCheckedException(e);
                }
            }

            @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {

            }

            @Override public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {

            }
        });

        db.waitForCheckpoint("test-checkpoint");

        createAndCheckSnapshot(ignite, SNAPSHOT_NAME);

        Path part0 = U.searchFileRecursively(snp(ignite).snapshotLocalDir(SNAPSHOT_NAME).toPath(),
            getPartitionFileName(PART_ID));

        assertNotNull(part0);
        assertTrue(part0.toString(), part0.toFile().exists());

        IdleVerifyResultV2 res = snp(ignite).checkSnapshot(SNAPSHOT_NAME, null).get().idleVerifyResult();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        assertTrue(F.isEmpty(res.exceptions()));
        assertContains(log, b.toString(),
            "The check procedure has failed, conflict partitions has been found: [counterConflicts=0, hashConflicts=1]");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckHashesSameAsIdleVerifyHashes() throws Exception {
        Random rnd = new Random();
        CacheConfiguration<Integer, Value> ccfg = txCacheConfig(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        IgniteEx ignite = startGridsWithCache(1, CACHE_KEYS_RANGE, k -> new Value(new byte[rnd.nextInt(32768)]), ccfg);

        createAndCheckSnapshot(ignite, SNAPSHOT_NAME);

        CacheIdleVerifyCommandArg arg = new CacheIdleVerifyCommandArg();

        arg.caches(new String[] {ccfg.getName()});
        arg.skipZeros(false);
        arg.cacheFilter(CacheFilterEnum.USER);
        arg.checkCrc(true);

        IdleVerifyResultV2 idleVerifyRes = ignite.compute().execute(new TestVisorBackupPartitionsTask(), arg);

        IdleVerifyResultV2 snpVerifyRes = ignite.compute().execute(
            new TestSnapshotPartitionsVerifyTask(),
            new SnapshotPartitionsVerifyTaskArg(
                new HashSet<>(),
                Collections.singletonMap(ignite.cluster().localNode(),
                Collections.singletonList(snp(ignite).readSnapshotMetadata(
                    snp(ignite).snapshotLocalDir(SNAPSHOT_NAME),
                    (String)ignite.configuration().getConsistentId()
                ))),
                null,
                0,
                true
            )
        ).idleVerifyResult();

        Map<PartitionKeyV2, List<PartitionHashRecordV2>> idleVerifyHashes = jobResults.get(TestVisorBackupPartitionsTask.class);
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> snpCheckHashes = jobResults.get(TestVisorBackupPartitionsTask.class);

        assertFalse(F.isEmpty(idleVerifyHashes));
        assertFalse(F.isEmpty(snpCheckHashes));

        assertEquals(idleVerifyHashes, snpCheckHashes);
        assertEquals(idleVerifyRes, snpVerifyRes);
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckWithTwoCachesCheckNullInput() throws Exception {
        SnapshotPartitionsVerifyTaskResult res = checkSnapshotWithTwoCachesWhenOneIsCorrupted(null);

        StringBuilder b = new StringBuilder();
        res.idleVerifyResult().print(b::append, true);

        assertFalse(F.isEmpty(res.exceptions()));
        assertNotNull(res.metas());
        assertContains(log, b.toString(), "The check procedure failed on 1 node.");
        assertContains(log, b.toString(), "Failed to read page (CRC validation failed)");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckWithTwoCachesCheckNotCorrupted() throws Exception {
        SnapshotPartitionsVerifyTaskResult res = checkSnapshotWithTwoCachesWhenOneIsCorrupted(Collections.singletonList(
            OPTIONAL_CACHE_NAME));

        StringBuilder b = new StringBuilder();
        res.idleVerifyResult().print(b::append, true);

        assertTrue(F.isEmpty(res.exceptions()));
        assertNotNull(res.metas());
        assertContains(log, b.toString(), "The check procedure has finished, no conflicts have been found");
        assertNotContains(log, b.toString(), "Failed to read page (CRC validation failed)");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckWithTwoCachesCheckTwoCaches() throws Exception {
        SnapshotPartitionsVerifyTaskResult res = checkSnapshotWithTwoCachesWhenOneIsCorrupted(Arrays.asList(
            OPTIONAL_CACHE_NAME, DEFAULT_CACHE_NAME));

        StringBuilder b = new StringBuilder();
        res.idleVerifyResult().print(b::append, true);

        assertFalse(F.isEmpty(res.exceptions()));
        assertNotNull(res.metas());
        assertContains(log, b.toString(), "The check procedure failed on 1 node.");
        assertContains(log, b.toString(), "Failed to read page (CRC validation failed)");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckMultipleTimes() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg, CACHE_KEYS_RANGE);

        startClientGrid();

        createAndCheckSnapshot(ignite, SNAPSHOT_NAME);

        int iterations = 10;

        // Warmup.
        for (int i = 0; i < iterations; i++)
            snp(ignite).checkSnapshot(SNAPSHOT_NAME, null).get();

        int activeThreadsCntBefore = Thread.activeCount();

        for (int i = 0; i < iterations; i++)
            snp(ignite).checkSnapshot(SNAPSHOT_NAME, null).get();

        int createdThreads = Thread.activeCount() - activeThreadsCntBefore;

        assertTrue("Threads created: " + createdThreads, createdThreads < iterations);
    }

    /** Tests that concurrent snapshot full checks are declined for the same snapshot. */
    @Test
    public void testConcurrentTheSameSnpFullChecksDeclined() throws Exception {
        // 0 - coordinator; 0,1,2 - baselines; 3 - non-baseline; 4,5 - clients.
        prepareGridsAndSnapshot(4, 3, 2, false);

        for (int i = 0; i < G.allGrids().size(); ++i) {
            for (int j = 1; j < G.allGrids().size() - 1; ++j) {
                int i0 = i;
                int j0 = j;

                doTestConcurrentSnpCheckOperations(
                    () -> new IgniteFutureImpl<>(snp(grid(i0)).checkSnapshot(SNAPSHOT_NAME, null)),
                    () -> new IgniteFutureImpl<>(snp(grid(j0)).checkSnapshot(SNAPSHOT_NAME, null)),
                    SNAPSHOT_CHECK_METAS,
                    SNAPSHOT_VALIDATE_PARTS,
                    true,
                    true,
                    null,
                    null
                );
            }
        }
    }

    /** Tests that concurrent snapshot full checks are allowed for different snapshots. */
    @Test
    public void testConcurrentDifferentSnpFullChecksAllowed() throws Exception {
        // 0 - coordinator; 0,1 - baselines; 2 - non-baseline; 3,4 - clients.
        prepareGridsAndSnapshot(3, 2, 2, false);

        snp(grid(3)).createSnapshot(SNAPSHOT_NAME + '2').get();

        for (int i = 0; i < G.allGrids().size(); ++i) {
            for (int j = 1; j < G.allGrids().size() - 1; ++j) {
                int i0 = i;
                int j0 = j;

                doTestConcurrentSnpCheckOperations(
                    () -> new IgniteFutureImpl<>(snp(grid(i0)).checkSnapshot(SNAPSHOT_NAME, null)),
                    () -> new IgniteFutureImpl<>(snp(grid(j0)).checkSnapshot(SNAPSHOT_NAME + '2', null)),
                    SNAPSHOT_CHECK_METAS,
                    SNAPSHOT_VALIDATE_PARTS,
                    false,
                    true,
                    null,
                    null
                );
            }
        }
    }

    /** Tests that concurrent snapshot full check and restoration (without checking) are allowed for different snapshots. */
    @Test
    public void testConcurrentDifferentSnpFullCheckAndRestorationAllowed() throws Exception {
        prepareGridsAndSnapshot(3, 2, 2, false);

        snp(grid(3)).createSnapshot(SNAPSHOT_NAME + '2').get();

        grid(0).destroyCache(DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();

        for (int i = 0; i < G.allGrids().size(); ++i) {
            // Snapshot restoration is disallowed from client nodes.
            for (int j = 1; j < 3; ++j) {
                int i0 = i;
                int j0 = j;

                doTestConcurrentSnpCheckOperations(
                    () -> new IgniteFutureImpl<>(snp(grid(i0)).checkSnapshot(SNAPSHOT_NAME, null)),
                    () -> snp(grid(j0)).restoreSnapshot(SNAPSHOT_NAME + '2', null),
                    SNAPSHOT_VALIDATE_PARTS,
                    RESTORE_CACHE_GROUP_SNAPSHOT_START,
                    false,
                    false,
                    null,
                    () -> grid(0).destroyCache(DEFAULT_CACHE_NAME)
                );
            }
        }
    }

    /** Tests concurrent snapshot full check and full restoration (with checking) are allowed for different snapshots. */
    @Test
    public void testConcurrentDifferentSnpCheckAndFullRestorationAllowed() throws Exception {
        prepareGridsAndSnapshot(3, 2, 2, false);

        snp(grid(0)).createSnapshot(SNAPSHOT_NAME + '2').get();

        grid(0).destroyCache(DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();

        for (int i = 0; i < G.allGrids().size(); ++i) {
            // Snapshot restoration is disallowed from client nodes.
            for (int j = 1; j < 3; ++j) {
                int i0 = i;
                int j0 = j;

                doTestConcurrentSnpCheckOperations(
                    () -> new IgniteFutureImpl<>(snp(grid(i0)).checkSnapshot(SNAPSHOT_NAME, null)),
                    () -> snp(grid(j0)).restoreSnapshot(SNAPSHOT_NAME + '2', null, null, 0, true),
                    SNAPSHOT_CHECK_METAS,
                    SNAPSHOT_VALIDATE_PARTS,
                    false,
                    true,
                    null,
                    () -> grid(0).destroyCache(DEFAULT_CACHE_NAME)
                );
            }
        }
    }

    /** Tests that concurrent snapshot full restoration (with checking) is declined when the same snapshot is being fully checked. */
    @Test
    public void testConcurrentTheSameSnpFullRestorationWhenFullyCheckingDeclined() throws Exception {
        prepareGridsAndSnapshot(3, 2, 2, false);

        for (int i = 0; i < G.allGrids().size(); ++i) {
            // Snapshot restoration is disallowed from client nodes.
            for (int j = 1; j < 3; ++j) {
                int i0 = i;
                int j0 = j;

                doTestConcurrentSnpCheckOperations(
                    () -> new IgniteFutureImpl<>(snp(grid(i0)).checkSnapshot(SNAPSHOT_NAME, null)),
                    () -> snp(grid(j0)).restoreSnapshot(SNAPSHOT_NAME, null, null, 0, true),
                    SNAPSHOT_CHECK_METAS,
                    SNAPSHOT_VALIDATE_PARTS,
                    true,
                    true,
                    null,
                    () -> grid(0).destroyCache(DEFAULT_CACHE_NAME)
                );
            }
        }
    }

    /** Tests that concurrent snapshot full check is declined when the same snapshot is being fully restored (checked). */
    @Test
    public void testConcurrentTheSameSnpFullCheckWhenFullyRestoringDeclined() throws Exception {
        prepareGridsAndSnapshot(3, 2, 2, true);

        // Snapshot restoration is disallowed from client nodes.
        for (int i = 0; i < 3; ++i) {
            for (int j = 1; j < G.allGrids().size(); ++j) {
                int i0 = i;
                int j0 = j;

                doTestConcurrentSnpCheckOperations(
                    () -> snp(grid(i0)).restoreSnapshot(SNAPSHOT_NAME, null, null, 0, true),
                    () -> new IgniteFutureImpl<>(snp(grid(j0)).checkSnapshot(SNAPSHOT_NAME, null)),
                    SNAPSHOT_CHECK_METAS,
                    SNAPSHOT_VALIDATE_PARTS,
                    true,
                    true,
                    null,
                    () -> grid(0).destroyCache(DEFAULT_CACHE_NAME)
                );
            }
        }
    }

    /** Tests that concurrent full check and restoration (without checking) of the same snapshot are allowed. */
    @Test
    public void testConcurrentTheSameSnpFullCheckAndRestoreAllowed() throws Exception {
        prepareGridsAndSnapshot(3, 2, 2, true);

        for (int i = 0; i < G.allGrids().size(); ++i) {
            // Snapshot restoration is disallowed from client nodes.
            for (int j = 1; j < 3; ++j) {
                int i0 = i;
                int j0 = j;

                doTestConcurrentSnpCheckOperations(
                    () -> new IgniteFutureImpl<>(snp(grid(i0)).checkSnapshot(SNAPSHOT_NAME, null)),
                    () -> snp(grid(j0)).restoreSnapshot(SNAPSHOT_NAME, null),
                    SNAPSHOT_CHECK_METAS,
                    RESTORE_CACHE_GROUP_SNAPSHOT_START,
                    false,
                    false,
                    null,
                    () -> grid(0).destroyCache(DEFAULT_CACHE_NAME)
                );
            }
        }
    }

    /** Tests that snapshot full check doesn't affect a snapshot creation. */
    @Test
    public void testConcurrentSnpCheckAndCreate() throws Exception {
        prepareGridsAndSnapshot(3, 2, 2, false);

        for (int i = 0; i < G.allGrids().size(); ++i) {
            for (int j = 1; j < G.allGrids().size() - 1; ++j) {
                int i0 = i;
                int j0 = j;

                doTestConcurrentSnpCheckOperations(
                    () -> new IgniteFutureImpl<>(snp(grid(i0)).checkSnapshot(SNAPSHOT_NAME, null)),
                    () -> snp(grid(j0)).createSnapshot(SNAPSHOT_NAME + "_2", null, false, false),
                    SNAPSHOT_CHECK_METAS,
                    null,
                    false,
                    false,
                    () -> U.delete(snp(grid(0)).snapshotLocalDir(SNAPSHOT_NAME + "_2")),
                    () -> U.delete(snp(grid(0)).snapshotLocalDir(SNAPSHOT_NAME + "_2"))
                );
            }
        }
    }

    /** Tests snapshot checking processes a baseline node leave. */
    @Test
    public void testBaselineLeavesDuringSnapshotChecking() throws Exception {
        prepareGridsAndSnapshot(6, 5, 1, false);

        Set<Integer> stopped = new HashSet<>();

        // Snapshot checking started from the coordinator.
        doTestNodeStopsDuringSnapshotChecking(0, 4, stopped);

        // Snapshot checking started from non-baseline.
        doTestNodeStopsDuringSnapshotChecking(5, 3, stopped);

        // Snapshot checking started from a client.
        doTestNodeStopsDuringSnapshotChecking(5, 2, stopped);

        // The same baseline leaves.
        doTestNodeStopsDuringSnapshotChecking(1, 1, stopped);
    }

    /** Tests snapshot checking processes a client node leave. */
    @Test
    public void testClientLeavesDuringSnapshotChecking() throws Exception {
        prepareGridsAndSnapshot(3, 2, 6, false);

        Set<Integer> stopped = new HashSet<>();

        // Snapshot checking started from a baseline.
        doTestNodeStopsDuringSnapshotChecking(1, 8, stopped);

        // Snapshot checking started from a non-baseline.
        doTestNodeStopsDuringSnapshotChecking(2, 7, stopped);

        // Snapshot checking started from the coordinator.
        doTestNodeStopsDuringSnapshotChecking(0, 6, stopped);

        // Snapshot checking started from other client.
        doTestNodeStopsDuringSnapshotChecking(4, 5, stopped);

        // Snapshot checking started from the same client.
        doTestNodeStopsDuringSnapshotChecking(4, 4, stopped);
    }

    /** Tests snapshot checking processes a non-baseline node leave. */
    @Test
    public void testNonBaselineServerLeavesDuringSnapshotChecking() throws Exception {
        prepareGridsAndSnapshot(7, 2, 1, false);

        Set<Integer> stopped = new HashSet<>();

        // Snapshot checking started from a sever node.
        doTestNodeStopsDuringSnapshotChecking(1, 6, stopped);

        // Snapshot checking started from a client node.
        doTestNodeStopsDuringSnapshotChecking(7, 5, stopped);

        // Snapshot checking started from another non-baseline.
        doTestNodeStopsDuringSnapshotChecking(3, 4, stopped);

        // Snapshot checking started from coordinator.
        doTestNodeStopsDuringSnapshotChecking(0, 3, stopped);

        // Snapshot checking started from the same non-baseline.
        doTestNodeStopsDuringSnapshotChecking(2, 2, stopped);
    }

    /** Tests snapshot checking process continues when a new baseline node leaves. */
    @Test
    public void testNewBaselineServerLeavesDuringSnapshotChecking() throws Exception {
        prepareGridsAndSnapshot(3, 2, 1, false);

        int grids = G.allGrids().size();

        discoSpi(grid(0)).block(msg -> msg instanceof FullMessage && ((FullMessage<?>)msg).type() == SNAPSHOT_CHECK_METAS.ordinal());

        IgniteInternalFuture<?> fut = snp(grid(3)).checkSnapshot(SNAPSHOT_NAME, null, null, false, 0, true);

        discoSpi(grid(0)).waitBlocked(getTestTimeout());

        grid(0).cluster().setBaselineTopology(Stream.of(grid(0).localNode(), grid(1).localNode(), grid(2).localNode())
            .collect(Collectors.toList()));

        stopGrid(2);

        waitForCondition(() -> {
            for (int i = 0; i < grids; ++i) {
                if (i != 2 && grid(i).cluster().nodes().size() != grids - 1)
                    return false;
            }

            return true;
        }, getTestTimeout());

        discoSpi(grid(0)).unblock();

        fut.get(getTestTimeout());
    }

    /** Tests snapshot checking process stops when the coorditator leaves. */
    @Test
    public void testCoordinatorLeavesDuringSnapshotChecking() throws Exception {
        prepareGridsAndSnapshot(5, 4, 1, false);

        Set<Integer> stopped = new HashSet<>();

        // Coordinator leaves when snapshot started from a server node.
        doTestNodeStopsDuringSnapshotChecking(4, 0, stopped);

        // Coordinator leaves when snapshot started from a client node.
        assertTrue(U.isLocalNodeCoordinator(grid(1).context().discovery()));

        doTestNodeStopsDuringSnapshotChecking(5, 1, stopped);

        // Coordinator leaves when snapshot started from it.
        assertTrue(U.isLocalNodeCoordinator(grid(2).context().discovery()));

        doTestNodeStopsDuringSnapshotChecking(2, 2, stopped);
    }

    /** Starts frids with the blocking discovery. */
    private void prepareGridsAndSnapshot(int servers, int baseLineCnt, int clients, boolean removeTheCache) throws Exception {
        assert baseLineCnt > 0 && baseLineCnt <= servers;

        IgniteEx ignite = null;

        for (int i = 0; i < servers + clients; ++i) {
            IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(i));

            cfg.setDiscoverySpi(new BlockingCustomMessageDiscoverySpi());

            if (i >= servers)
                cfg.setClientMode(true);

            ignite = startGrid(cfg);

            if (i == baseLineCnt - 1) {
                ignite.cluster().state(ACTIVE);

                ignite.cluster().setBaselineTopology(ignite.cluster().topologyVersion());
            }
        }

        try (IgniteDataStreamer<Integer, Integer> ds = grid(0).dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 100; ++i)
                ds.addData(i, i);
        }

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        if (removeTheCache)
            ignite.destroyCache(DEFAULT_CACHE_NAME);
    }

    /**
     * Tests concurrent snapshot operations related to the snapshot checking.
     *
     * @param originatorOp First snapshot operation on an originator node.
     * @param trierOp Second concurrent snapshot operation on a trier node.
     * @param firstFullMsgToDelay First distributed process full message of {@code originatorOp} to delay on the coordinator
     *                            to launch {@code trierOp}.
     * @param secondFullMsgToDelay Second distributed process full message of {@code originatorOp} to delay on the coordinator
     *                             to launch {@code trierOp} again.
     * @param expectFailure If {@code true}, the 'snapshot-check-is-in-progress' error is excepted during excution of
     *                      {@code trierOp}. Otherwise, {@code trierOp} must successfully finish.
     * @param sameOperation If {@code true}, {@code firstFullMsgToDelay} and {@code secondFullMsgToDelay} are expected for
     *                      both {@code originatorOp} and {@code trierOp}. Otherwise, only for {@code originatorOp}.
     * @param step2preparation If not {@code null}, is executed before delaying and waiting for {@code secondFullMsgToDelay}.
     * @param cleaner If not {@code null}, is executed at the end.
     */
    private void doTestConcurrentSnpCheckOperations(
        Supplier<IgniteFuture<?>> originatorOp,
        Supplier<IgniteFuture<?>> trierOp,
        DistributedProcess.DistributedProcessType firstFullMsgToDelay,
        @Nullable DistributedProcess.DistributedProcessType secondFullMsgToDelay,
        boolean expectFailure,
        boolean sameOperation,
        @Nullable Runnable step2preparation,
        @Nullable Runnable cleaner
    ) throws Exception {
        try {
            discoSpi(grid(0)).block(msg -> msg instanceof FullMessage && ((FullMessage<?>)msg).type() == firstFullMsgToDelay.ordinal());

            IgniteFuture<?> fut = originatorOp.get();

            discoSpi(grid(0)).waitBlocked(getTestTimeout());

            IgniteFuture<?> fut2 = trierOp.get();

            if (expectFailure) {
                assertThrowsAnyCause(
                    log,
                    fut2::get,
                    IllegalStateException.class,
                    "Validation of snapshot '" + SNAPSHOT_NAME + "' has already started"
                );

                if (secondFullMsgToDelay == null) {
                    discoSpi(grid(0)).unblock();

                    fut.get(getTestTimeout());

                    return;
                }

                discoSpi(grid(0)).blockNextAndRelease(msg -> msg instanceof FullMessage
                    && ((FullMessage<?>)msg).type() == secondFullMsgToDelay.ordinal());

                discoSpi(grid(0)).waitBlocked(getTestTimeout());

                if (step2preparation != null)
                    step2preparation.run();

                assertThrowsAnyCause(
                    log,
                    fut2::get,
                    IllegalStateException.class,
                    "Validation of snapshot '" + SNAPSHOT_NAME + "' has already started"
                );

                discoSpi(grid(0)).unblock();

                fut.get(getTestTimeout());
            }
            else {
                if (sameOperation) {
                    discoSpi(grid(0)).waitBlockedSize(2, getTestTimeout());

                    if (secondFullMsgToDelay != null) {
                        discoSpi(grid(0)).blockNextAndRelease(msg -> msg instanceof FullMessage
                            && ((FullMessage<?>)msg).type() == secondFullMsgToDelay.ordinal());

                        discoSpi(grid(0)).waitBlockedSize(2, getTestTimeout());
                    }
                }
                else {
                    if (secondFullMsgToDelay != null) {
                        discoSpi(grid(0)).blockNextAndRelease(msg -> msg instanceof FullMessage
                            && ((FullMessage<?>)msg).type() == secondFullMsgToDelay.ordinal());

                        discoSpi(grid(0)).waitBlocked(getTestTimeout());
                    }
                    else
                        fut2.get();
                }

                discoSpi(grid(0)).unblock();

                fut2.get();

                fut.get();
            }
        }
        finally {
            discoSpi(grid(0)).unblock();

            if (cleaner != null)
                cleaner.run();

            awaitPartitionMapExchange();
        }
    }

    /**  */
    private void doTestNodeStopsDuringSnapshotChecking(int originatorIdx, int nodeToStopIdx, Set<Integer> stopped) throws Exception {
        int grids = G.allGrids().size();

        ClusterNode leaving = grid(nodeToStopIdx).cluster().localNode();

        boolean requredLeft = !leaving.isClient() && grid(nodeToStopIdx).cluster().currentBaselineTopology().stream()
            .anyMatch(bl -> bl.consistentId().equals(leaving.consistentId()));

        int coordIdx = -1;

        for (int i = 0; i < grids; ++i) {
            if (stopped.contains(i) || !U.isLocalNodeCoordinator(grid(i).context().discovery()))
                continue;

            coordIdx = i;

            break;
        }

        AtomicInteger chkAgainIdx = new AtomicInteger(-1);

        try {
            discoSpi(grid(coordIdx)).block(msg -> msg instanceof FullMessage
                && ((FullMessage<?>)msg).type() == SNAPSHOT_CHECK_METAS.ordinal());

            IgniteInternalFuture<?> fut = snp(grid(originatorIdx)).checkSnapshot(SNAPSHOT_NAME, null, null, false, 0, true);

            discoSpi(grid(coordIdx)).waitBlocked(getTestTimeout());

            stopGrid(nodeToStopIdx);

            stopped.add(nodeToStopIdx);

            waitForCondition(() -> {
                for (int i = 0; i < grids; ++i) {
                    if (!stopped.contains(i) && grid(i).cluster().nodes().size() != grids - 1)
                        return false;
                }

                return true;
            }, getTestTimeout());

            if (nodeToStopIdx != coordIdx)
                discoSpi(grid(coordIdx)).unblock();

            if (originatorIdx == nodeToStopIdx) {
                assertThrowsAnyCause(
                    null,
                    () -> fut.get(getTestTimeout()),
                    NodeStoppingException.class,
                    "Node is stopping"
                );

                return;
            }

            if (requredLeft) {
                assertThrowsAnyCause(
                    null,
                    () -> fut.get(getTestTimeout()),
                    ClusterTopologyCheckedException.class,
                    "Snapshot checking stopped. A node left the cluster"
                );
            }
            else
                fut.get(getTestTimeout());
        }
        finally {
            if (nodeToStopIdx != coordIdx)
                discoSpi(grid(coordIdx)).unblock();

            awaitPartitionMapExchange();

            waitForCondition(() -> {
                for (int i = 0; i < grids; ++i) {
                    if (stopped.contains(i))
                        continue;

                    chkAgainIdx.compareAndSet(-1, i);

                    if (!snp(grid(i)).checkSnpProc.requests().isEmpty())
                        return false;
                }

                return true;
            }, getTestTimeout());
        }

        snp(grid(chkAgainIdx.get())).checkSnapshot(SNAPSHOT_NAME, null, null, false, 0, true).get();
    }

    /** */
    @Test
    public void testClusterSnapshotCheckWithExpiring() throws Exception {
        IgniteEx ignite = startGrids(3);

        ignite.cluster().state(ACTIVE);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(new CacheConfiguration<>("expCache")
            .setAffinity(new RendezvousAffinityFunction(false, 32)).setBackups(1));

        Random rnd = new Random();

        for (int i = 0; i < 10_000; i++) {
            cache.withExpiryPolicy(new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS,
                rnd.nextInt(10_000)))).put(i, i);
        }

        long timeout = getTestTimeout();

        snp(ignite).createSnapshot(SNAPSHOT_NAME).get(timeout);

        SnapshotPartitionsVerifyTaskResult res = snp(ignite).checkSnapshot(SNAPSHOT_NAME, null).get(timeout);

        assertFalse(res.idleVerifyResult().hasConflicts());
    }

    /**
     * @param cls Class of running task.
     * @param results Results of compute.
     */
    private void saveHashes(Class<?> cls, List<ComputeJobResult> results) {
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> hashes = new HashMap<>();

        for (ComputeJobResult job : results) {
            if (job.getException() != null)
                continue;

            job.<Map<PartitionKeyV2, PartitionHashRecordV2>>getData().forEach((k, v) ->
                hashes.computeIfAbsent(k, k0 -> new ArrayList<>()).add(v));
        }

        Object mustBeNull = jobResults.putIfAbsent(cls, hashes);

        assertNull(mustBeNull);
    }

    /**
     * @param cachesToCheck Cache names to check.
     * @return Check result.
     * @throws Exception If fails.
     */
    private SnapshotPartitionsVerifyTaskResult checkSnapshotWithTwoCachesWhenOneIsCorrupted(
        Collection<String> cachesToCheck
    ) throws Exception {
        Random rnd = new Random();
        CacheConfiguration<Integer, Value> ccfg1 = txCacheConfig(new CacheConfiguration<>(DEFAULT_CACHE_NAME));
        CacheConfiguration<Integer, Value> ccfg2 = txCacheConfig(new CacheConfiguration<>(OPTIONAL_CACHE_NAME));

        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, k -> new Value(new byte[rnd.nextInt(32768)]),
            ccfg1, ccfg2);

        createAndCheckSnapshot(ignite, SNAPSHOT_NAME);

        corruptPartitionFile(ignite, SNAPSHOT_NAME, ccfg1, PART_ID);

        return snp(ignite).checkSnapshot(SNAPSHOT_NAME, null, cachesToCheck, false, 0, true).get(TIMEOUT);
    }

    /**
     * @param ignite Ignite instance.
     * @param snpName Snapshot name.
     * @param ccfg Cache configuration.
     * @param partId Partition id to corrupt.
     * @throws IgniteCheckedException If fails.
     * @throws IOException If partition file failed to be changed.
     */
    private static void corruptPartitionFile(
        IgniteEx ignite,
        String snpName,
        CacheConfiguration<?, ?> ccfg,
        int partId
    ) throws IgniteCheckedException, IOException {
        Path cachePath = Paths.get(snp(ignite).snapshotLocalDir(snpName).getAbsolutePath(),
            databaseRelativePath(ignite.context().pdsFolderResolver().resolveFolders().folderName()),
            cacheDirName(ccfg));

        Path part0 = U.searchFileRecursively(cachePath, getPartitionFileName(partId));

        int grpId = CU.cacheId(ccfg.getName());

        try (FilePageStore pageStore = (FilePageStore)((FilePageStoreManager)ignite.context().cache().context().pageStore())
            .getPageStoreFactory(grpId, ignite.context().cache().isEncrypted(grpId))
            .createPageStore(getTypeByPartId(partId),
                () -> part0,
                val -> {
                })
        ) {
            ByteBuffer buff = ByteBuffer.allocateDirect(ignite.configuration().getDataStorageConfiguration().getPageSize())
                .order(ByteOrder.nativeOrder());
            pageStore.read(0, buff, false);

            pageStore.beginRecover();

            PageIO.setCrc(buff, 1);

            buff.flip();
            pageStore.write(PageIO.getPageId(buff), buff, 0, false);
            pageStore.finishRecover();
        }
    }

    /** */
    private class TestVisorBackupPartitionsTask extends VerifyBackupPartitionsTaskV2 {
        /** {@inheritDoc} */
        @Override public @Nullable IdleVerifyResultV2 reduce(List<ComputeJobResult> results) throws IgniteException {
            IdleVerifyResultV2 res = super.reduce(results);

            saveHashes(TestVisorBackupPartitionsTask.class, results);

            return res;
        }
    }

    /** Test compute task to collect partition data hashes when the snapshot check procedure ends. */
    private class TestSnapshotPartitionsVerifyTask extends SnapshotPartitionsVerifyTask {
        /** {@inheritDoc} */
        @Override public @Nullable SnapshotPartitionsVerifyTaskResult reduce(List<ComputeJobResult> results) throws IgniteException {
            SnapshotPartitionsVerifyTaskResult res = super.reduce(results);

            saveHashes(TestSnapshotPartitionsVerifyTask.class, results);

            return res;
        }
    }
}
