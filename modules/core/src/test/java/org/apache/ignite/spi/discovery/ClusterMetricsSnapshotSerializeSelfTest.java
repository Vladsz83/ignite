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

package org.apache.ignite.spi.discovery;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

import static org.junit.Assert.assertNotEquals;

/**
 * Grid discovery metrics test.
 */
@GridCommonTest(group = "Utils")
public class ClusterMetricsSnapshotSerializeSelfTest extends GridCommonAbstractTest {
    /** Metrics serialized by Ignite 1.0 */
    private static final byte[] METRICS_V1 = {0, 0, 0, 22, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 27, 0, 0, 0, 15, 64,
        (byte)-32, 0, 0, 0, 0, 0, 26, 0, 0, 0, 14, 64, (byte)-64, 0, 0, 0, 0, 0, 23, 0, 0, 0, 9, 64, 64, 0, 0, 0, 0, 0,
        39, 0, 0, 0, 36, 0, 0, 0, 37, 0, 0, 0, 0, 0, 0, 0, 25, 0, 0, 0, 0, 0, 0, 0, 13, 64, 20, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 12, 64, 16, 0, 0, 0, 0, 0, 0, (byte)-1, (byte)-1, (byte)-1, (byte)-1, 0,
        0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 38, 0, 0, 0, 1, 64, 65, 0, 0, 0, 0, 0, 0, (byte)-65, (byte)-16, 0, 0,
        0, 0, 0, 0, (byte)-65, (byte)-16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 18, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0,
        0, 0, 0, 0, 17, 0, 0, 0, 0, 0, 0, 0, 19, 0, 0, 0, 0, 0, 0, 0, 48, 0, 0, 0, 0, 0, 0, 0, 29, 0, 0, 0, 0, 0, 0, 0,
        31, 0, 0, 0, 0, 0, 0, 0, 28, 0, 0, 0, 0, 0, 0, 0, 30, 0, 0, 0, 0, 0, 0, 0, 47, 0, 0, 0, 0, 0, 0, 0, 33,
        (byte)-1, (byte)-1, (byte)-1, (byte)-1, (byte)-1, (byte)-1, (byte)-1, (byte)-1, 0, 0, 0, 0, 0, 0, 0, 41, 0, 0,
        0, 35, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 40, 0, 0, 0, 16, (byte)-1, (byte)-1, (byte)-1, (byte)-1, (byte)-1,
        (byte)-1, (byte)-1, (byte)-1, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0, 43, 0, 0, 0, 44, 0, 0, 0, 0, 0, 0, 0, 45, 0, 0,
        0, 46, (byte)-1, (byte)-1, (byte)-1, (byte)-1};

    /** */
    public ClusterMetricsSnapshotSerializeSelfTest() {
        super(false /*don't start grid*/);
    }

    /** */
    @Test
    public void testMetricsSize() {
        byte[] data = new byte[ClusterMetricsSnapshot.METRICS_SIZE];

        // Test serialization.
        int off = ClusterMetricsSnapshot.serialize(data, 0, createMetrics());

        assert off == ClusterMetricsSnapshot.METRICS_SIZE;

        // Test deserialization.
        ClusterMetrics res = ClusterMetricsSnapshot.deserialize(data, 0);

        assert res != null;
    }

    /** Every transferred field must get a distinct value, otherwise the checks below prove nothing. */
    @Test
    public void testEveryFieldCovered() throws Exception {
        ClusterMetrics metrics = createMetrics();

        ClusterMetricsSnapshot dflt = new ClusterMetricsSnapshot();

        Collection<Object> vals = new HashSet<>();

        for (Field f : transferredFields()) {
            assertNotEquals("Not set by createMetrics(): " + f.getName(), f.get(dflt), f.get(metrics));

            assertTrue("Duplicate value: " + f.getName(), vals.add(f.get(metrics)));
        }
    }

    /** The copying constructor reads the getters, so it also proves every getter returns its own field. */
    @Test
    public void testCopy() throws Exception {
        ClusterMetrics metrics = createMetrics();

        assertTransferredFieldsEqual(metrics, new ClusterMetricsSnapshot(metrics));
    }

    /** */
    @Test
    public void testSerialization() throws Exception {
        byte[] data = new byte[ClusterMetricsSnapshot.METRICS_SIZE];

        ClusterMetrics metrics1 = createMetrics();

        // Test serialization.
        int off = ClusterMetricsSnapshot.serialize(data, 0, metrics1);

        assertEquals(ClusterMetricsSnapshot.METRICS_SIZE, off);

        // Test deserialization. Last update time is not transferred, deserialize() sets the receiving time instead.
        assertTransferredFieldsEqual(metrics1, ClusterMetricsSnapshot.deserialize(data, 0));
    }

    /**
     * Checks compatibility with old serialized metrics.
     */
    @Test
    public void testMetricsCompatibility() {
        ClusterMetrics metrics = ClusterMetricsSnapshot.deserialize(METRICS_V1, 0);

        assert metrics != null;
    }

    /**
     * @return Test metrics.
     */
    private ClusterMetrics createMetrics() {
        ClusterMetricsSnapshot metrics = new ClusterMetricsSnapshot();

        int v = 0;

        metrics.totalCpus(++v);
        metrics.averageActiveJobs(++v);
        metrics.averageCancelledJobs(++v);
        metrics.averageJobExecuteTime(++v);
        metrics.averageJobWaitTime(++v);
        metrics.averageRejectedJobs(++v);
        metrics.averageWaitingJobs(++v);
        metrics.currentActiveJobs(++v);
        metrics.currentCancelledJobs(++v);
        metrics.currentIdleTime(++v);
        metrics.currentJobExecuteTime(++v);
        metrics.currentJobWaitTime(++v);
        metrics.currentRejectedJobs(++v);
        metrics.currentWaitingJobs(++v);
        metrics.currentDaemonThreadCount(++v);
        metrics.heapMemoryCommitted(++v);
        metrics.heapMemoryInitialized(++v);
        metrics.heapMemoryMaximum(++v);
        metrics.heapMemoryUsed(++v);
        metrics.lastUpdateTime(++v);
        metrics.maximumActiveJobs(++v);
        metrics.maximumCancelledJobs(++v);
        metrics.maximumJobExecuteTime(++v);
        metrics.maximumJobWaitTime(++v);
        metrics.maximumRejectedJobs(++v);
        metrics.maximumWaitingJobs(++v);
        metrics.nonHeapMemoryCommitted(++v);
        metrics.nonHeapMemoryInitialized(++v);
        metrics.nonHeapMemoryMaximum(++v);
        metrics.nonHeapMemoryUsed(++v);
        metrics.maximumThreadCount(++v);
        metrics.startTime(++v);
        metrics.currentCpuLoad(++v);
        metrics.currentThreadCount(++v);
        metrics.totalCancelledJobs(++v);
        metrics.totalExecutedJobs(++v);
        metrics.totalIdleTime(++v);
        metrics.totalRejectedJobs(++v);
        metrics.totalStartedThreadCount(++v);
        metrics.upTime(++v);
        metrics.sentMessagesCount(++v);
        metrics.sentBytesCount(++v);
        metrics.receivedMessagesCount(++v);
        metrics.receivedBytesCount(++v);
        metrics.outboundMessagesQueueSize(++v);
        metrics.nonHeapMemoryTotal(++v);
        metrics.heapMemoryTotal(++v);
        metrics.totalNodes(++v);
        metrics.totalJobsExecutionTime(++v);
        metrics.currentPmeDuration(++v);
        metrics.averageCpuLoad(++v);
        metrics.currentGcCpuLoad(++v);
        metrics.lastDataVersion(++v);
        metrics.nodeStartTime(++v);
        metrics.totalExecutedTasks(++v);

        return metrics;
    }

    /**
     * @param exp Expected metrics.
     * @param act Actual metrics.
     */
    private void assertTransferredFieldsEqual(Object exp, Object act) throws Exception {
        for (Field f : transferredFields())
            assertEquals(f.getName(), f.get(exp), f.get(act));
    }

    /**
     * The transferred state is kept in the public fields, {@code @Order} itself is not visible at runtime. Last
     * update time is public too, but it is not transferred: the receiver stamps its own time.
     *
     * @return Fields the node metrics are transferred with.
     */
    private Collection<Field> transferredFields() {
        Collection<Field> res = new ArrayList<>();

        for (Field f : ClusterMetricsSnapshot.class.getDeclaredFields()) {
            if (Modifier.isPublic(f.getModifiers()) && !Modifier.isStatic(f.getModifiers())
                && !"lastUpdateTime".equals(f.getName()))
                res.add(f);
        }

        assertFalse(res.isEmpty());

        return res;
    }
}
