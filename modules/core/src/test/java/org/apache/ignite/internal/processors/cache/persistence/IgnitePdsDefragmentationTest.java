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
import java.lang.reflect.Field;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
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
import org.apache.ignite.internal.processors.cache.persistence.freelist.AbstractFreeList;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.mxbean.IgniteMXBean;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
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

        cfg.setDataStorageConfiguration(new DataStorageConfiguration());

//        CacheConfiguration<?, ?> cache1Cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
//            .setAtomicityMode(TRANSACTIONAL)
//            .setGroupName(GRP_NAME)
//            .setAffinity(new RendezvousAffinityFunction(false, PARTS));
//
//        CacheConfiguration<?, ?> cache2Cfg = new CacheConfiguration<>(CACHE_2_NAME)
//            .setAtomicityMode(TRANSACTIONAL)
//            .setGroupName(GRP_NAME)
//            .setExpiryPolicyFactory(new PolicyFactory())
//            .setAffinity(new RendezvousAffinityFunction(false, PARTS));
//
//        cfg.setCacheConfiguration(cache1Cfg, cache2Cfg);

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
        final int cycles = 100;

        final long maxRegionSize = 256 * IgniteUtils.MB;
        final int pageSize = 4 * 1024;

        final int deleteChance = 100;
        final boolean randomize = true;
        final boolean persistence = true;
        final float metaPercent = 0.05f;

        final int maxDataCnt = (int)(((double)maxRegionSize * (1.0f - metaPercent))
            / new TheEpicData(0, false).dataSize());

        final int transactionSize = 0;

        IgniteConfiguration cfg = getConfiguration();
        cfg.getDataStorageConfiguration().setWalMode(WALMode.NONE);
        cfg.getDataStorageConfiguration().setWalSegmentSize(8 * (int)IgniteUtils.MB);
        cfg.getDataStorageConfiguration().setWalSegments(8);
        cfg.getDataStorageConfiguration().setMaxWalArchiveSize(128 * IgniteUtils.MB);
        cfg.getDataStorageConfiguration().setCheckpointFrequency(3000);

        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setMaxSize(maxRegionSize);
        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setInitialSize(maxRegionSize);
        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setPersistenceEnabled(persistence);
        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setMetricsEnabled(true);
        cfg.getDataStorageConfiguration().setPageSize(pageSize);

        CacheConfiguration<Integer, TheEpicData> cacheCfg = new CacheConfiguration<>("defragCache");
        cacheCfg.setAtomicityMode(transactionSize > 0 ? TRANSACTIONAL : ATOMIC);
        cacheCfg.setIndexedTypes(Integer.class, TheEpicData.class);

        cfg.setWorkDirectory("/tmp/ignite");

        IgniteEx ig = startGrid(cfg);

        ig.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, TheEpicData> cache = ig.getOrCreateCache(cacheCfg);

        AtomicLong totalLoad = new AtomicLong();

        File writterF = new File(((IgniteMXBean)ig).getIgniteHome() +"/results/defragTest.log");
        writterF.delete();
        writterF.createNewFile();

        List<Integer> keys = IntStream.range(0, maxDataCnt).boxed().collect(Collectors.toList());
        AtomicInteger keyIdx = new AtomicInteger();

        File cacheDir = new File(cfg.getWorkDirectory() + "/db/" +
            ig.name().replaceAll("\\.", "_") + "/cache-" + cacheCfg.getName());

        try (PrintWriter writter = new PrintWriter(writterF)){
            for (int c = 0; c < cycles; ++c) {
                writter.println("Cycle " + (c + 1) + " / " + cycles + " ...");
                writter.flush();

                totalLoad.set(0);
                keyIdx.set(0);
                Collections.shuffle(keys);

                GridTestUtils.runMultiThreaded(new Callable<Object>() {
                    @Override public Object call() {
                        Transaction tx = null;
                        int txCnt = 0;

                        for (int i = keyIdx.getAndIncrement(); i < keys.size(); i = keyIdx.incrementAndGet()) {
                            if (transactionSize > 0 && txCnt == 0)
                                tx = ig.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);

                            try {
                                TheEpicData data = new TheEpicData(keys.get(i), randomize);

                                cache.put(keys.get(i), data);

                                if (transactionSize > 0 && ++txCnt >= transactionSize) {
                                    txCnt = 0;
                                    tx.commit();
                                }

                                totalLoad.addAndGet(data.dataSize());
                            }
                            catch (Exception e) {
                                writter.println(new Date() + "Unable to put data: " + e.getMessage());
                                break;
                            }
                        }

                        if (txCnt > 0) {
                            try {
                                tx.commit();
                            }
                            catch (Exception ignored) {
                                // No-op.
                            }
                        }

                        return null;
                    }
                }, Math.max(1, Runtime.getRuntime().availableProcessors() / 4), "defragPutter");

                writter.println("Load: " + totalLoad + "b (" + IgniteUtils.sizeInMegabytes(totalLoad.get()) + "mb)." +
                    " Records: " + maxDataCnt + String.format(". Util.: %.1f", 100.0 * totalLoad.get() / maxRegionSize) +
                    "%." + (persistence ?
                    " Persist size: " + IgniteUtils.sizeInMegabytes(IgniteUtils.dirSize(cacheDir.toPath())) + "mb."
                    : "")
                );

                writter.flush();

                Collections.shuffle(keys);
                keyIdx.set(0);

                if (deleteChance < 0)
                    cache.clear();
                else if (deleteChance >= 100)
                    cache.removeAll();
                else {
                    GridTestUtils.runMultiThreaded(new Callable<Object>() {
                        @Override public Object call() {
                            Random rnd = new Random();

                            for (int i = keyIdx.getAndIncrement(); i < keys.size(); i = keyIdx.incrementAndGet())
                                if (rnd.nextInt(100) < deleteChance)
                                    cache.remove(keys.get(i));

                            return null;
                        }
                    }, Math.max(1, Runtime.getRuntime().availableProcessors() / 4), "defragRemover");
                }

                IgniteCacheProxyImpl<Integer, byte[]> cacheImpl = IgniteUtils.field(cache, "delegate");

                Map<String, Long> reuseCnt = new HashMap<>();
                Map<String, Long> partlyFreeCnt = new HashMap<>();

                for (int partNum = 0; partNum < cacheImpl.context().topology().partitions(); partNum++) {
//                    iterate(cacheImpl.context().topology().localPartition(partNum));
                    GridDhtLocalPartition partition = cacheImpl.context().topology().localPartition(partNum);
                    IgniteCacheOffheapManager.CacheDataStore partDataStore = partition.dataStore();
                    AbstractFreeList<?> freeList = (AbstractFreeList<?>)partDataStore.rowStore().freeList();

                    for (int i = 0; i < freeList.bucketsCount(); ++i) {
                        if (freeList.bucketSize(i) == 0)
                            continue;

                        Map<String, Long> counter = i == 255 ? reuseCnt : partlyFreeCnt;

                        counter.putIfAbsent(freeList.name(), freeList.bucketSize(i));
//                        System.err.println("TEST | Bucket size " + i + " of list " + freeList.name() + ": " + freeList.bucketSize(i) + " of part " + partition.id());
                    }
                }

                writter.println("Total free pages: " + reuseCnt.values().stream().reduce(0L, Long::sum) +
                    ". Total partly free pages: " + partlyFreeCnt.values().stream().reduce(0L, Long::sum));

                writter.println("Finished cycle " + (c + 1) + '.');
                writter.println();
                writter.flush();
            }
        }
    }

    /** */
//    private void iterate(GridDhtLocalPartition partition) throws IgniteCheckedException {
//        IgniteCacheOffheapManager.CacheDataStore partDataStore = partition.dataStore();
//        PageMemory partPageMem = partition.group().dataRegion().pageMemory();
//        long metaPageId = partDataStore.tree().getMetaPageId();
//        long metaPage = partPageMem.acquirePage(partition.group().groupId(), metaPageId);
//        AbstractFreeList<?> freeList = (AbstractFreeList<?>)partDataStore.rowStore().freeList();
//
//        for (int i = 0; i < freeList.bucketsCount(); ++i) {
//            if (freeList.bucketSize(i) > 0)
//                System.err.println("TEST | Bucket size " + i + " of list " + freeList.name() + ": " + freeList.bucketSize(i) + " of part " + partition.id());
//        }
//
//        try {
//            long metaPageAddr = partPageMem.readLock(partition.group().groupId(), metaPageId, metaPage);
//
//            try {
//                BPlusMetaIO metaIO = PageIO.getPageIO(metaPageAddr);
//
//                long rootLvl = metaIO.getRootLevel(metaPageAddr);
//                long lvlCnt = metaIO.getLevelsCount(metaPageAddr);
//
//                for (int lvl = 0; lvl < lvlCnt; lvl++) {
//                    long firstPageId = metaIO.getFirstPageId(metaPageAddr, lvl);
//
//                   // PageIO pageIO = PageIO.getPageIO()
//                }
//
//                //return metaIO.getFirstPageId(metaPageAddr, 0);
//            }
//            finally {
//                partPageMem.readUnlock(partition.group().groupId(), metaPageId, metaPage);
//            }
//        }
//        finally {
//            partPageMem.releasePage(partition.group().groupId(), metaPageId, metaPage);
//        }
//    }

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

    /** */
    private static class TheEpicData {
        /** */
        private long long1;
        /** */
        @QuerySqlField()
        private long long2;
        /** */
        @QuerySqlField(index = true)
        private long long3;
        /** */
        private byte byte1;
        /** */
        @QuerySqlField()
        private byte byte2;
        /** */
        private int id;
        /** */
        @QuerySqlField(index = true)
        private int int2;
        /** */
        @QuerySqlField()
        private int int3;
        /** */
        private String str1 = "adfgsaf  aaad  53453 4c";
        /** */
        @QuerySqlField(index = true)
        private String indexedStr = "sdfsdgf65 sdfds rjfgj sagsertg54dfg53";
        /** */
        @QuerySqlField()
        private String str3 = "abc657dfzhgdsfsgsdgsdfgsdfxc vjljoldofiujoip3u gadfg5474567 5oijoijopzkjgbsg";
        /** */
        private byte[] raw1 = new byte[700];
        /** */
        private byte[] raw2 = new byte[500];
        /** */
        private byte[] raw3 = new byte[12_000];
        /** */
        private byte[] raw4 = new byte[55_000];

        /** */
        public TheEpicData(int id, boolean randomize) {
            this.id = id;

            if (randomize) {
                Random rnd = new Random();

                raw1 = rndBytes(raw1.length, rnd);
                raw2 = rndBytes(raw2.length, rnd);
                raw3 = rndBytes(raw3.length, rnd);
                raw4 = rndBytes(raw4.length, rnd);

                indexedStr = rndStr(indexedStr.length(), rnd);
                str3 = rndStr(str3.length(), rnd);
            }
        }

        public long getLong1() {
            return long1;
        }

        public void setLong1(long long1) {
            this.long1 = long1;
        }

        public long getLong2() {
            return long2;
        }

        public void setLong2(long long2) {
            this.long2 = long2;
        }

        public long getLong3() {
            return long3;
        }

        public void setLong3(long long3) {
            this.long3 = long3;
        }

        public byte getByte1() {
            return byte1;
        }

        public void setByte1(byte byte1) {
            this.byte1 = byte1;
        }

        public byte getByte2() {
            return byte2;
        }

        public void setByte2(byte byte2) {
            this.byte2 = byte2;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getInt2() {
            return int2;
        }

        public void setInt2(int int2) {
            this.int2 = int2;
        }

        public int getInt3() {
            return int3;
        }

        public void setInt3(int int3) {
            this.int3 = int3;
        }

        public String getStr1() {
            return str1;
        }

        public void setStr1(String str1) {
            this.str1 = str1;
        }

        public String getIndexedStr() {
            return indexedStr;
        }

        public void setIndexedStr(String indexedStr) {
            this.indexedStr = indexedStr;
        }

        public String getStr3() {
            return str3;
        }

        public void setStr3(String str3) {
            this.str3 = str3;
        }

        public byte[] getRaw1() {
            return raw1;
        }

        public void setRaw1(byte[] raw1) {
            this.raw1 = raw1;
        }

        public byte[] getRaw2() {
            return raw2;
        }

        public void setRaw2(byte[] raw2) {
            this.raw2 = raw2;
        }

        public byte[] getRaw3() {
            return raw3;
        }

        public void setRaw3(byte[] raw3) {
            this.raw3 = raw3;
        }

        public byte[] getRaw4() {
            return raw4;
        }

        public void setRaw4(byte[] raw4) {
            this.raw4 = raw4;
        }

        /** */
        public int dataSize() throws IllegalAccessException {
            int res = 0;

            for (Field f : TheEpicData.class.getDeclaredFields()) {
                if (f.getType() == int.class || f.getType() == float.class)
                    res += 4;
                else if (f.getType() == long.class || f.getType() == double.class)
                    res += 8;
                else if (f.getType() == short.class)
                    res += 2;
                else if (f.getType() == byte.class)
                    res += 1;
                else if (f.getType() == byte[].class)
                    res += 4 + ((byte[])f.get(this)).length;
                else if (f.getType() == String.class)
                    res += 4 + ((String)f.get(this)).getBytes().length;
                else
                    throw new RuntimeException("Unknown field type: " + f.getType().getName());
            }

            return res;
        }

        /** */
        private static byte[] rndBytes(int maxLen, Random rnd) {
            if (rnd == null)
                rnd = new Random();

            byte[] res = new byte[maxLen / 2 + rnd.nextInt(maxLen / 2)];

            rnd.nextBytes(res);

            return res;
        }

        /** */
        private static String rndStr(int maxLen, Random rnd) {
            if (rnd == null)
                rnd = new Random();

            return rnd.ints(48, 122)
                .filter(i -> (i < 57 || i > 65) && (i < 90 || i > 97))
                .mapToObj(i -> (char)i)
                .limit(maxLen / 2 + rnd.nextInt(maxLen / 2))
                .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)
                .toString();
        }
    }
}
