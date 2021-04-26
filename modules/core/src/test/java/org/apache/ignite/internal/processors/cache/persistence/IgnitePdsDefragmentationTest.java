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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteState;
import org.apache.ignite.Ignition;
import org.apache.ignite.IgnitionListener;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.maintenance.MaintenanceFileStore;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.store.PageStoreCollection;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxyImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.util.model.TheEpicData;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.defragmentationCompletionMarkerFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.defragmentedIndexFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.defragmentedPartFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.defragmentedPartMappingFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.maintenance.DefragmentationParameters.toStore;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/** */
public class IgnitePdsDefragmentationTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_2_NAME = "cache2";

    /** */
    public static final int PARTS = 5;

    /** */
    public static final int ADDED_KEYS_COUNT = 1500;

    /** */
    protected static final String GRP_NAME = "group";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    /** */
    protected static class PolicyFactory implements Factory<ExpiryPolicy> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public ExpiryPolicy create() {
            return new ExpiryPolicy() {
                @Override public Duration getExpiryForCreation() {
                    return new Duration(TimeUnit.MILLISECONDS, 13000);
                }

                /** {@inheritDoc} */
                @Override public Duration getExpiryForAccess() {
                    return new Duration(TimeUnit.MILLISECONDS, 13000);
                }

                /** {@inheritDoc} */
                @Override public Duration getExpiryForUpdate() {
                    return new Duration(TimeUnit.MILLISECONDS, 13000);
                }
            };
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.setWalSegmentSize(4 * 1024 * 1024);

        dsCfg.setDefaultDataRegionConfiguration(
            new DataRegionConfiguration()
                .setInitialSize(100L * 1024 * 1024)
                .setMaxSize(1024L * 1024 * 1024)
                .setPersistenceEnabled(true)
        );

        cfg.setDataStorageConfiguration(dsCfg);

        CacheConfiguration<?, ?> cache1Cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setGroupName(GRP_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, PARTS));

        CacheConfiguration<?, ?> cache2Cfg = new CacheConfiguration<>(CACHE_2_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setGroupName(GRP_NAME)
            .setExpiryPolicyFactory(new PolicyFactory())
            .setAffinity(new RendezvousAffinityFunction(false, PARTS));

        cfg.setCacheConfiguration(cache1Cfg, cache2Cfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE >> 1;
    }

    /**
     * Basic test scenario. Does following steps:
     *  - Start node;
     *  - Fill cache;
     *  - Remove part of data;
     *  - Stop node;
     *  - Start node in defragmentation mode;
     *  - Stop node;
     *  - Start node;
     *  - Check that partitions became smaller;
     *  - Check that cache is accessible and works just fine.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSuccessfulDefragmentation2() throws Exception {
        final int supposedDataExtraSize = 68;
        final float supposedRegionMetaPercent = 0.03f;

        final int cycles = 100;
//        final int partCnt = 1024;

        final long maxRegionSize = 1024 * IgniteUtils.MB;
//        final long maxRegionSize = 512 * 1024L * 1024L;
        final int pageSize = 4 * 1024;

        final int minDataSize = 2 * 1024;
        final int maxDataSize = 88 * 1024;

//        final int maxDataCnt = (int)(((double)maxRegionSize * (1.0f - supposedRegionMetaPercent))
//            / (supposedDataExtraSize + maxDataSize));
//        final int maxDataCnt = (int)(((double)maxRegionSize * (1.0f - supposedRegionMetaPercent)) / supposedDataExtraSize);

//        final int maxDataCnt = (int)(((double)maxRegionSize * (1.0f - supposedRegionMetaPercent))
//            / (supposedDataExtraSize + new TheEpicData(false).dataSize()));
//        final int maxDataCnt = 18920;
        final int maxDataCnt = 25_000;

        final int transactionSize = 0;

        IgniteConfiguration cfg = getConfiguration();
//        cfg.getDataStorageConfiguration().setWalMode(WALMode.NONE);
        cfg.getDataStorageConfiguration().setWalSegmentSize(64 * (int)IgniteUtils.MB);
        cfg.getDataStorageConfiguration().setWalSegments(12);
//        cfg.getDataStorageConfiguration().setWalBufferSize(8 * (int)IgniteUtils.MB);
//        cfg.getDataStorageConfiguration().setWalCompactionEnabled(false);
        cfg.getDataStorageConfiguration().setMaxWalArchiveSize(256 * IgniteUtils.MB);
//        cfg.getDataStorageConfiguration().setWalArchivePath(cfg.getDataStorageConfiguration().getWalPath());
        cfg.getDataStorageConfiguration().setCheckpointFrequency(3000);

        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setMaxSize(maxRegionSize);
        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setInitialSize(maxRegionSize);
        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setPersistenceEnabled(false);
        cfg.getDataStorageConfiguration().setPageSize(pageSize);

        CacheConfiguration<Integer, TheEpicData> cacheCfg = new CacheConfiguration<>("defragCache");
        cacheCfg.setAtomicityMode(transactionSize > 0 ? TRANSACTIONAL : ATOMIC);
//        AffinityFunction affFunction = new RendezvousAffinityFunction(false, partCnt);
//        cacheCfg.setAffinity(affFunction);
        cacheCfg.setIndexedTypes(Integer.class, TheEpicData.class);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);

        cfg.setCacheConfiguration(cacheCfg);

        cfg.setWorkDirectory("/tmp/ignite");

        IgniteEx ig = startGrid(cfg);

        ig.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, TheEpicData> cache = ig.getOrCreateCache(cacheCfg);

        Transaction tx = null;
        int txCnt = 0;
        final Random rnd = new Random();

        long totalLoad;
        long recordsNum;

        SortedSet<Long> loadSet = new TreeSet<>();

        File writterF = new File(((IgniteKernal)ig).getIgniteHome() +"/results/defragTest.log");
        writterF.delete();
        writterF.createNewFile();

        try (PrintWriter writter = new PrintWriter(writterF)){
            for (int c = 0; c < cycles; ++c) {
                writter.println("TEST | Cycle " + (c + 1) + " / " + cycles + " ...");
                writter.flush();

                totalLoad = 0;
                recordsNum = 0;

                List<Integer> keys = IntStream.range(0, maxDataCnt).boxed().collect(Collectors.toList());
                Collections.shuffle(keys);

                for (int i = 0; i < keys.size(); i++) {
                    if (transactionSize > 0 && txCnt == 0)
                        tx = ig.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);

//                    int dataSize = (c * 100) / cycles;
//                    dataSize = minDataSize +
//                        (maxDataSize-minDataSize) * (dataSize + rnd.nextInt(100 - dataSize)) / 100;
//                    assert dataSize >= minDataSize && dataSize <= maxDataSize;
//
//                    byte[] data = new byte[dataSize];

                    try {
                        TheEpicData data = new TheEpicData(keys.get(i), false);
                        cache.put(keys.get(i), data);

                        QueryCursor<List<?>> cursor = cache.query(
                            new SqlFieldsQuery("select * from TheEpicData where indexedStr = '" + data.getIndexedStr() + "'"));

                        for (List<?> lst : cursor){
                            System.err.println("TEST lst size: " + lst.size());
                        }


//                        cache.putIfAbsent(keys.get(i), data);
                        int dataSize = data.dataSize();

                        if (transactionSize > 0 && ++txCnt >= transactionSize) {
                            txCnt = 0;
                            tx.commit();
                        }

                        totalLoad += dataSize;
                        ++recordsNum;

                        loadSet.add(totalLoad);
                    }
                    catch (Exception e) {
                        writter.println("TEST | Unable to put data: " + e.getMessage());
                        break;
                    }
                }

                if (txCnt > 0) {
                    txCnt = 0;
                    try {
                        tx.commit();
                    }
                    catch (Exception ignored) {
                        // No-op.
                    }
                }

                if (cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().isPersistenceEnabled()) {
                    forceCheckpoint(ig);

                    File cacheDir = new File(cfg.getWorkDirectory() + "/db/" +
                        ig.name().replaceAll("\\.", "_") + "/cache-" + cacheCfg.getName());

                    writter.println("TEST | Cycle " + (c + 1) + ". Load: " + IgniteUtils.sizeInMegabytes(totalLoad) +
                            "mb. Records: " + recordsNum + String.format(". Util.: %.1f", 100.0 * totalLoad / maxRegionSize) +
//                    "%. Record size: " + data.length + ". Total load: " + IgniteUtils.sizeInMegabytes(totalLoad) +
                            "%. Total load: " + IgniteUtils.sizeInMegabytes(totalLoad) +
                            "mb. Persist. size: " + IgniteUtils.sizeInMegabytes(IgniteUtils.dirSize(cacheDir.toPath())) + "mb"
                    );
                }
                else {
                    writter.println("TEST | Cycle " + (c + 1) + ". Load: " + IgniteUtils.sizeInMegabytes(totalLoad) +
                            "mb. Records: " + recordsNum + String.format(". Util.: %.1f", 100.0 * totalLoad / maxRegionSize) +
//                    "%. Record size: " + data.length + ". Total load: " + IgniteUtils.sizeInMegabytes(totalLoad) +
                            "%. Total load: " + IgniteUtils.sizeInMegabytes(totalLoad) + "mb"
                    );
                }

                writter.flush();
                System.gc();

                Collections.shuffle(keys);
                for (int k : keys) {
                    if (rnd.nextInt(100) < 100)
                        cache.remove(k);
                }

                System.gc();

                writter.println("TEST | Finished cycle " + (c + 1));
                writter.flush();
            }
        }

        //forceCheckpoint(ig);

        //TreeIterator treeIter = new TreeIterator(ig.configuration().getDataStorageConfiguration().getPageSize());

        System.gc();

        IgniteCacheProxyImpl<Integer, byte[]> cacheImpl = IgniteUtils.field(cache, "delegate");

        for (int partNum = 0; partNum < cacheImpl.context().topology().partitions(); partNum++) {
            GridDhtLocalPartition part = cacheImpl.context().topology().localPartition(partNum);
//            IgniteCacheOffheapManager.CacheDataStore dataStore = cacheImpl.context().offheap().dataStore(partition);

            iterate(part);

//            treeIter.iterate(dataStore.tree(), (PageMemoryEx)partition.group().dataRegion().pageMemory(),
//                (tree0, io, pageAddr, idx) -> {
//                    System.err.println("TEST | treeIter.iterate : pageAddr==" + pageAddr + ", itemIdx==" + idx +
//                        ", io==" + io);
//
//                    CacheIdAwareDataLeafIO dataIO = (CacheIdAwareDataLeafIO)io;
//
//                    return true;
//                });
        }

        for (IgniteCacheOffheapManager.CacheDataStore dataStore : cacheImpl.context().offheap().cacheDataStores()) {

        }
        //cache.cacheG

//        treeIter.iterate();
//
//        cache.

    }

    /** */
    private void iterate(GridDhtLocalPartition partition) throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore partDataStore = partition.group().offheap().dataStore(partition);
        CacheGroupContext cacheGrp = partition.group();
        PageMemory partPageMem = cacheGrp.dataRegion().pageMemory();
        long metaPageId = partDataStore.tree().getMetaPageId();
        long metaPage = partPageMem.acquirePage(cacheGrp.groupId(), metaPageId);

        try {
            long metaPageAddr = partPageMem.readLock(cacheGrp.groupId(), metaPageId, metaPage);

            try {
                BPlusMetaIO metaIO = PageIO.getPageIO(metaPageAddr);

                long rootLvl = metaIO.getRootLevel(metaPageAddr);
                long lvlCnt = metaIO.getLevelsCount(metaPageAddr);

                for (int lvl = 0; lvl < lvlCnt; lvl++) {
                    long firstPageId = metaIO.getFirstPageId(metaPageAddr, lvl);

                   // PageIO pageIO = PageIO.getPageIO()
                }

                //return metaIO.getFirstPageId(metaPageAddr, 0);
            }
            finally {
                partPageMem.readUnlock(cacheGrp.groupId(), metaPageId, metaPageId);
            }
        }
        finally {
            partPageMem.releasePage(cacheGrp.groupId(), metaPageId, metaPageId);
        }
    }

    /**
     * Basic test scenario. Does following steps:
     *  - Start node;
     *  - Fill cache;
     *  - Remove part of data;
     *  - Stop node;
     *  - Start node in defragmentation mode;
     *  - Stop node;
     *  - Start node;
     *  - Check that partitions became smaller;
     *  - Check that cache is accessible and works just fine.
     *
     * @throws Exception If failed.
     */
//    @Test
//    public void testSuccessfulDefragmentation() throws Exception {
//        IgniteEx ig = startGrid(0);
//
//        ig.cluster().state(ClusterState.ACTIVE);
//
//        fillCache(ig.cache(DEFAULT_CACHE_NAME));
//
//        forceCheckpoint(ig);
//
//        createMaintenanceRecord();
//
//        stopGrid(0);
//
//        File workDir = resolveCacheWorkDir(ig);
//
//        long[] oldPartLen = partitionSizes(workDir);
//
//        long oldIdxFileLen = new File(workDir, FilePageStoreManager.INDEX_FILE_NAME).length();
//
//        startGrid(0);
//
//        waitForDefragmentation(0);
//
//        assertEquals(ClusterState.INACTIVE, grid(0).context().state().clusterState().state());
//
//        GridTestUtils.assertThrowsAnyCause(
//            log,
//            () -> {
//                grid(0).cluster().state(ClusterState.ACTIVE);
//
//                return null;
//            },
//            IgniteCheckedException.class,
//            "Failed to activate cluster (node is in maintenance mode)"
//        );
//
//        long[] newPartLen = partitionSizes(workDir);
//
//        for (int p = 0; p < PARTS; p++)
//            assertTrue(newPartLen[p] < oldPartLen[p]);
//
//        long newIdxFileLen = new File(workDir, FilePageStoreManager.INDEX_FILE_NAME).length();
//
//        assertTrue(newIdxFileLen <= oldIdxFileLen);
//
//        File completionMarkerFile = defragmentationCompletionMarkerFile(workDir);
//        assertTrue(completionMarkerFile.exists());
//
//        stopGrid(0);
//
//        IgniteEx ig0 = startGrid(0);
//
//        ig0.cluster().state(ClusterState.ACTIVE);
//
//        assertFalse(completionMarkerFile.exists());
//
//        validateCache(grid(0).cache(DEFAULT_CACHE_NAME));
//
//        validateLeftovers(workDir);
//    }

    protected long[] partitionSizes(CacheGroupContext grp) {
        final int grpId = grp.groupId();

        return IntStream.concat(
            IntStream.of(INDEX_PARTITION),
            IntStream.range(0, grp.shared().affinity().affinity(grpId).partitions())
        ).mapToLong(p -> {
            try {
                final FilePageStore store = (FilePageStore) ((PageStoreCollection) grp.shared().pageStore()).getStore(grpId, p);

                return new File(store.getFileAbsolutePath()).length();
            } catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }).toArray();
    }

    /**
     * @return Working directory for cache group {@link IgnitePdsDefragmentationTest#GRP_NAME}.
     * @throws IgniteCheckedException If failed for some reason, like if it's a file instead of directory.
     */
    private File resolveCacheWorkDir(IgniteEx ig) throws IgniteCheckedException {
        File dbWorkDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        File nodeWorkDir = new File(dbWorkDir, U.maskForFileName(ig.name()));

        return new File(nodeWorkDir, FilePageStoreManager.CACHE_GRP_DIR_PREFIX + GRP_NAME);
    }

    /**
     * Force checkpoint and wait for it so all partitions will be in their final state after restart if no more data is
     * uploaded.
     *
     * @param ig Ignite node.
     * @throws IgniteCheckedException If checkpoint failed for some reason.
     */
    private void forceCheckpoint(IgniteEx ig) throws IgniteCheckedException {
        ig.context().cache().context().database()
            .forceCheckpoint("testDefrag")
            .futureFor(CheckpointState.FINISHED)
            .get();
    }

    /** */
    protected void waitForDefragmentation(int idx) throws IgniteCheckedException {
        IgniteEx ig = grid(idx);

        ((GridCacheDatabaseSharedManager)ig.context().cache().context().database())
            .defragmentationManager()
            .completionFuture()
            .get();
    }

    /** */
    protected void createMaintenanceRecord(String... cacheNames) throws IgniteCheckedException {
        IgniteEx grid = grid(0);

        MaintenanceRegistry mntcReg = grid.context().maintenanceRegistry();

        final List<String> caches = new ArrayList<>();

        caches.add(DEFAULT_CACHE_NAME);

        if (cacheNames != null && cacheNames.length != 0)
            caches.addAll(Arrays.asList(cacheNames));

        mntcReg.registerMaintenanceTask(toStore(caches));
    }

    /**
     * Returns array that contains sizes of partition files in gived working directories. Assumes that partitions
     * {@code 0} to {@code PARTS - 1} exist in that dir.
     *
     * @param workDir Working directory.
     * @return The array.
     */
    protected long[] partitionSizes(File workDir) {
        return IntStream.range(0, PARTS)
            .mapToObj(p -> new File(workDir, String.format(FilePageStoreManager.PART_FILE_TEMPLATE, p)))
            .mapToLong(File::length)
            .toArray();
    }

    /**
     * Checks that plain node start after failed defragmentation will finish batch renaming.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailoverRestartWithoutDefragmentation() throws Exception {
        testFailover(workDir -> {
            try {
                File mntcRecFile = new File(workDir.getParent(), MaintenanceFileStore.MAINTENANCE_FILE_NAME);

                assertTrue(mntcRecFile.exists());

                Files.delete(mntcRecFile.toPath());

                startGrid(0);

                validateLeftovers(workDir);
            }
            catch (Exception e) {
                throw new IgniteCheckedException(e);
            }
            finally {
                createMaintenanceRecord();

                stopGrid(0);
            }
        });
    }

    /**
     * Checks that second start in defragmentation mode will finish defragmentation if no completion marker was found.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailoverOnLastStage() throws Exception {
        testFailover(workDir -> {});
    }

    /**
     * Checks that second start in defragmentation mode will finish defragmentation if index was not defragmented.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailoverIncompletedIndex() throws Exception {
        testFailover(workDir -> move(
            DefragmentationFileUtils.defragmentedIndexFile(workDir),
            DefragmentationFileUtils.defragmentedIndexTmpFile(workDir)
        ));
    }

    /**
     * Checks that second start in defragmentation mode will finish defragmentation if partition was not defragmented.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailoverIncompletedPartition1() throws Exception {
        testFailover(workDir -> {
            DefragmentationFileUtils.defragmentedIndexFile(workDir).delete();

            move(
                DefragmentationFileUtils.defragmentedPartFile(workDir, PARTS - 1),
                DefragmentationFileUtils.defragmentedPartTmpFile(workDir, PARTS - 1)
            );
        });
    }

    /**
     * Checks that second start in defragmentation mode will finish defragmentation if no mapping was found for partition.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailoverIncompletedPartition2() throws Exception {
        testFailover(workDir -> {
            DefragmentationFileUtils.defragmentedIndexFile(workDir).delete();

            DefragmentationFileUtils.defragmentedPartMappingFile(workDir, PARTS - 1).delete();
        });
    }

    /** */
    private void move(File from, File to) throws IgniteCheckedException {
        try {
            Files.move(from.toPath(), to.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** */
    private void testFailover(IgniteThrowableConsumer<File> c) throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        fillCache(ig.cache(DEFAULT_CACHE_NAME));

        forceCheckpoint(ig);

        createMaintenanceRecord();

        stopGrid(0);

        File workDir = resolveCacheWorkDir(ig);

        //Defragmentation should fail when node starts.
        startAndAwaitNodeFail(workDir);

        c.accept(workDir);

        startGrid(0); // Fails here VERY rarely. WTF?

        waitForDefragmentation(0);

        stopGrid(0);

        // Everything must be completed.
        startGrid(0).cluster().state(ClusterState.ACTIVE);

        validateCache(grid(0).cache(DEFAULT_CACHE_NAME));

        validateLeftovers(workDir);
    }

    /**
     * @throws IgniteInterruptedCheckedException If fail.
     */
    private void startAndAwaitNodeFail(File workDir) throws IgniteInterruptedCheckedException {
        String errMsg = "Failed to create defragmentation completion marker.";

        AtomicBoolean errOccurred = new AtomicBoolean();

        UnaryOperator<IgniteConfiguration> cfgOp = cfg -> {
            DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration();

            FileIOFactory delegate = dsCfg.getFileIOFactory();

            dsCfg.setFileIOFactory((file, modes) -> {
                if (file.equals(defragmentationCompletionMarkerFile(workDir))) {
                    errOccurred.set(true);

                    throw new IOException(errMsg);
                }

                return delegate.create(file, modes);
            });

            return cfg;
        };

        AtomicBoolean nodeStopped = new AtomicBoolean();
        IgnitionListener nodeStopListener = (name, state) -> {
            if (name.equals(getTestIgniteInstanceName(0)) && state == IgniteState.STOPPED_ON_FAILURE)
                nodeStopped.set(true);
        };

        Ignition.addListener(nodeStopListener);
        try {
            try {
                startGrid(0, cfgOp);
            }
            catch (Exception ignore) {
                // No-op.
            }

            // Failed node can leave interrupted status of the thread that needs to be cleared,
            // otherwise following "wait" wouldn't work.
            // This call can't be moved inside of "catch" block because interruption can actually be silent.
            Thread.interrupted();

            assertTrue(GridTestUtils.waitForCondition(errOccurred::get, 3_000L));
            assertTrue(GridTestUtils.waitForCondition(nodeStopped::get, 3_000L));
        }
        finally {
            Ignition.removeListener(nodeStopListener);
        }
    }

    /** */
    public void validateLeftovers(File workDir) {
        assertFalse(defragmentedIndexFile(workDir).exists());

        for (int p = 0; p < PARTS; p++) {
            assertFalse(defragmentedPartMappingFile(workDir, p).exists());

            assertFalse(defragmentedPartFile(workDir, p).exists());
        }
    }

    /** */
    @Test
    public void testDefragmentedPartitionCreated() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        fillCache(ig.cache(DEFAULT_CACHE_NAME));

        fillCache(ig.getOrCreateCache(CACHE_2_NAME));

        createMaintenanceRecord();

        stopGrid(0);

        startGrid(0);

        waitForDefragmentation(0);

        File workDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        AtomicReference<File> cachePartFile = new AtomicReference<>();
        AtomicReference<File> defragCachePartFile = new AtomicReference<>();

        Files.walkFileTree(workDir.toPath(), new FileVisitor<Path>() {
            @Override public FileVisitResult preVisitDirectory(Path path, BasicFileAttributes basicFileAttributes) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override public FileVisitResult visitFile(Path path, BasicFileAttributes basicFileAttributes) throws IOException {
                if (path.toString().contains("cacheGroup-group")) {
                    File file = path.toFile();

                    if (file.getName().contains("part-dfrg-"))
                        cachePartFile.set(file);
                    else if (file.getName().contains("part-"))
                        defragCachePartFile.set(file);
                }

                return FileVisitResult.CONTINUE;
            }

            @Override public FileVisitResult visitFileFailed(Path path, IOException e) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override public FileVisitResult postVisitDirectory(Path path, IOException e) throws IOException {
                return FileVisitResult.CONTINUE;
            }
        });

        assertNull(cachePartFile.get()); //TODO Fails.
        assertNotNull(defragCachePartFile.get());
    }

    /**
     * Fill cache using integer keys.
     *
     * @param cache
     */
    protected void fillCache(IgniteCache<Integer, Object> cache) {
        fillCache(Function.identity(), cache);
    }

    /** */
    protected <T> void fillCache(Function<Integer, T> keyMapper, IgniteCache<T, Object> cache) {
        try (IgniteDataStreamer<T, Object> ds = grid(0).dataStreamer(cache.getName())) {
            for (int i = 0; i < ADDED_KEYS_COUNT; i++) {
                byte[] val = new byte[8192];
                new Random().nextBytes(val);

                ds.addData(keyMapper.apply(i), val);
            }
        }

        try (IgniteDataStreamer<T, Object> ds = grid(0).dataStreamer(cache.getName())) {
            ds.allowOverwrite(true);

            for (int i = 0; i <= ADDED_KEYS_COUNT / 2; i++)
                ds.removeData(keyMapper.apply(i * 2));
        }
    }

    /** */
    public void validateCache(IgniteCache<Object, Object> cache) {
        for (int k = 0; k < ADDED_KEYS_COUNT; k++) {
            Object val = cache.get(k);

            if (k % 2 == 0)
                assertNull(val);
            else
                assertNotNull(val);
        }
    }

    /**
     * Start node, wait for defragmentation and validate that sizes of caches are less than those before the defragmentation.
     * @param gridId Idx of ignite grid.
     * @param groups Cache groups to check.
     * @throws Exception If failed.
     */
    protected void defragmentAndValidateSizesDecreasedAfterDefragmentation(int gridId, CacheGroupContext... groups) throws Exception {
        for (CacheGroupContext grp : groups) {
            final long[] oldPartLen = partitionSizes(grp);

            startGrid(0);

            waitForDefragmentation(0);

            stopGrid(0);

            final long[] newPartLen = partitionSizes(grp);

            boolean atLeastOneSmaller = false;

            for (int p = 0; p < oldPartLen.length; p++) {
                assertTrue(newPartLen[p] <= oldPartLen[p]);

                if (newPartLen[p] < oldPartLen[p])
                    atLeastOneSmaller = true;
            }

            assertTrue(atLeastOneSmaller);
        }
    }

}
