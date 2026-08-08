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

    /** The test values must stay unique, otherwise a getter reading a neighbour field goes unnoticed. */
    @Test
    public void testDistinctValues() throws Exception {
        ClusterMetrics metrics = createMetrics();

        Collection<Double> vals = new HashSet<>();

        for (Field f : transferredFields())
            assertTrue("Duplicate test value: " + f.getName(), vals.add(((Number)f.get(metrics)).doubleValue()));
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

        metrics.totalCpus(1);
        metrics.averageActiveJobs(2);
        metrics.averageCancelledJobs(3);
        metrics.averageJobExecuteTime(4);
        metrics.averageJobWaitTime(5);
        metrics.averageRejectedJobs(6);
        metrics.averageWaitingJobs(7);
        metrics.currentActiveJobs(8);
        metrics.currentCancelledJobs(9);
        metrics.currentIdleTime(11);
        metrics.currentJobExecuteTime(12);
        metrics.currentJobWaitTime(13);
        metrics.currentRejectedJobs(14);
        metrics.currentWaitingJobs(15);
        metrics.currentDaemonThreadCount(16);
        metrics.heapMemoryCommitted(17);
        metrics.heapMemoryInitialized(18);
        metrics.heapMemoryMaximum(19);
        metrics.heapMemoryUsed(20);
        metrics.lastUpdateTime(21);
        metrics.maximumActiveJobs(22);
        metrics.maximumCancelledJobs(23);
        metrics.maximumJobExecuteTime(24);
        metrics.maximumJobWaitTime(25);
        metrics.maximumRejectedJobs(26);
        metrics.maximumWaitingJobs(27);
        metrics.nonHeapMemoryCommitted(28);
        metrics.nonHeapMemoryInitialized(29);
        metrics.nonHeapMemoryMaximum(30);
        metrics.nonHeapMemoryUsed(31);
        metrics.maximumThreadCount(32);
        metrics.startTime(33);
        metrics.currentCpuLoad(34);
        metrics.currentThreadCount(35);
        metrics.totalCancelledJobs(36);
        metrics.totalExecutedJobs(37);
        metrics.totalIdleTime(38);
        metrics.totalRejectedJobs(39);
        metrics.totalStartedThreadCount(40);
        metrics.upTime(41);
        metrics.sentMessagesCount(42);
        metrics.sentBytesCount(43);
        metrics.receivedMessagesCount(44);
        metrics.receivedBytesCount(45);
        metrics.outboundMessagesQueueSize(46);
        metrics.nonHeapMemoryTotal(47);
        metrics.heapMemoryTotal(48);
        metrics.totalNodes(49);
        metrics.totalJobsExecutionTime(50);
        metrics.currentPmeDuration(51);
        metrics.averageCpuLoad(52);
        metrics.currentGcCpuLoad(53);
        metrics.lastDataVersion(54);
        metrics.nodeStartTime(55);
        metrics.totalExecutedTasks(56);

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
