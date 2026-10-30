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

package org.apache.ignite.internal.processors.cache.expiry;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;

/**
 * Tests the {@code ExpiredEntriesCount} cache metric that shows the number of entries that have already expired
 * but have not been removed from the cache yet.
 */
@RunWith(Parameterized.class)
public class ExpiredEntriesCountMetricTest extends GridCommonAbstractTest {
    /** Number of entries used by the tests. */
    private static final int ENTRIES_CNT = 100;

    /** */
    @Parameterized.Parameter
    public boolean pds;

    /** */
    @Parameterized.Parameters(name = "pds={0}")
    public static Collection<?> parameters() {
        return F.asList(false, true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setPersistenceEnabled(pds)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Checks that only entries with an expire time in the past are counted as expired ones.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExpiredEntriesCount() throws Exception {
        IgniteEx ignite = startGrid();

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheConfiguration(Duration.ONE_DAY));

        GridCacheContext<?, ?> cctx = ignite.context().cache().cache(DEFAULT_CACHE_NAME).context();

        for (int i = 0; i < ENTRIES_CNT; i++)
            cache.put(i, i);

        long now = U.currentTimeMillis();

        // Nothing has expired yet.
        assertEquals(0, metricValue(ignite));
        assertEquals(0, cctx.ttl().expiredCount(now));

        // All the entries are expired from the point of view of the moment after their expiration.
        long futTime = now + TimeUnit.DAYS.toMillis(2);

        assertEquals(ENTRIES_CNT, cctx.ttl().expiredCount(futTime));
        assertEquals(ENTRIES_CNT, cctx.offheap().expiredSize(futTime, cctx.cacheId()));
    }

    /**
     * Checks that the metric drops down to zero after expired entries are removed by the background cleanup.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExpiredEntriesCountAfterCleanup() throws Exception {
        IgniteEx ignite = startGrid();

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(
            cacheConfiguration(new Duration(TimeUnit.MILLISECONDS, 500)).setStatisticsEnabled(true));

        for (int i = 0; i < ENTRIES_CNT; i++)
            cache.put(i, i);

        assertTrue("Expired entries were not removed by the TTL cleanup worker.",
            GridTestUtils.waitForCondition(() -> cache.size() == 0, 10_000));

        assertEquals(0, metricValue(ignite));
        assertTrue(cache.localMetrics().getCacheEvictions() >= ENTRIES_CNT);
    }

    /**
     * Checks that the metric shows entries that are already expired but not removed yet when the cleanup of
     * expired entries is postponed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExpiredEntriesCountWithPostponedCleanup() throws Exception {
        String throttleTimeout = System.getProperty(IgniteSystemProperties.IGNITE_UNWIND_THROTTLING_TIMEOUT);

        // Postpone the cleanup of expired entries, so that expired entries can be observed in a stable way.
        System.setProperty(IgniteSystemProperties.IGNITE_UNWIND_THROTTLING_TIMEOUT, "600000");

        try {
            IgniteEx ignite = startGrid();

            ignite.cluster().state(ClusterState.ACTIVE);

            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(
                cacheConfiguration(new Duration(TimeUnit.MILLISECONDS, 3_000)));

            for (int i = 0; i < ENTRIES_CNT; i++)
                cache.put(i, i);

            // Let the TTL cleanup worker make sure that there is nothing to remove and postpone the next attempt.
            U.sleep(1_000);

            assertEquals(0, metricValue(ignite));

            assertTrue("Expired entries are not reported by the metric.",
                GridTestUtils.waitForCondition(() -> metricValue(ignite) == ENTRIES_CNT, 10_000));
        }
        finally {
            if (throttleTimeout == null)
                System.clearProperty(IgniteSystemProperties.IGNITE_UNWIND_THROTTLING_TIMEOUT);
            else
                System.setProperty(IgniteSystemProperties.IGNITE_UNWIND_THROTTLING_TIMEOUT, throttleTimeout);
        }
    }

    /**
     * @param ttl Entry time to live.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(Duration ttl) {
        return new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setExpiryPolicyFactory(FactoryBuilder.factoryOf(new CreatedExpiryPolicy(ttl)));
    }

    /**
     * Reads the ExpiredEntriesCount metric from the metric registry.
     *
     * @param ignite Ignite instance.
     * @return Metric value.
     */
    private long metricValue(IgniteEx ignite) {
        MetricRegistryImpl mreg = ignite.context().metric().registry(cacheMetricsRegistryName(DEFAULT_CACHE_NAME, false));

        LongMetric metric = mreg.findMetric("ExpiredEntriesCount");

        assertNotNull("The ExpiredEntriesCount metric is not registered.", metric);

        return metric.value();
    }
}
