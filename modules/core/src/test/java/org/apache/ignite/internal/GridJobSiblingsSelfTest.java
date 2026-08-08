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

package org.apache.ignite.internal;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobSibling;
import org.apache.ignite.compute.ComputeTaskContinuousMapper;
import org.apache.ignite.compute.ComputeTaskNoResultCache;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.resources.TaskContinuousMapperResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Checks how a remote job obtains its siblings: from the job request or from the task node.
 */
public class GridJobSiblingsSelfTest extends GridCommonAbstractTest {
    /** Number of jobs the task maps. */
    private static final int JOB_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * A task annotated with {@link ComputeTaskNoResultCache} keeps no siblings in its session, so the job request
     * carries an empty sibling list. An empty list must not be confused with a missing one: collapsing the two makes
     * every job ask the task node for the siblings, which costs a synchronous round trip per job and, once the task
     * has finished, blocks the job until the network timeout and then answers with nothing.
     */
    @Test
    public void testNoResultCacheTask() throws Exception {
        checkSiblings(NoResultCacheTask.class, 0, false);
    }

    /** A task that caches results sends all of its siblings within the job request. */
    @Test
    public void testResultCacheTask() throws Exception {
        checkSiblings(ResultCacheTask.class, JOB_CNT, false);
    }

    /**
     * A continuous task maps its jobs one by one, so the job request carries no siblings at all and the job has to
     * ask the task node. Covers the sibling ids travelling back within {@link GridJobSiblingsResponse}.
     */
    @Test
    public void testContinuousTask() throws Exception {
        checkSiblings(ContinuousTask.class, JOB_CNT, true);
    }

    /**
     * Runs the task on the remote node only and checks whether the jobs asked the task node for the siblings.
     *
     * @param taskCls Task to run.
     * @param expSibCnt Number of siblings each job must see.
     * @param expReq Whether the jobs are expected to request the siblings from the task node.
     */
    private void checkSiblings(Class<? extends ComputeTaskSplitAdapter<Object, Object>> taskCls, int expSibCnt,
        boolean expReq) throws Exception {
        startGrids(2);

        TestRecordingCommunicationSpi jobNodeSpi = TestRecordingCommunicationSpi.spi(grid(1));

        jobNodeSpi.record(GridJobSiblingsRequest.class);

        grid(0).compute(grid(0).cluster().forRemotes()).execute(taskCls, expSibCnt);

        List<Object> reqs = jobNodeSpi.recordedMessages(true);

        assertEquals("Unexpected sibling requests: " + reqs, expReq, !reqs.isEmpty());
    }

    /** */
    @ComputeTaskNoResultCache
    private static class NoResultCacheTask extends SiblingsCheckTask {
        // No-op.
    }

    /** */
    private static class ResultCacheTask extends SiblingsCheckTask {
        // No-op.
    }

    /** */
    private static class ContinuousTask extends SiblingsCheckTask {
        /** Makes the task continuous: the siblings are no longer known when a job request is built. */
        @TaskContinuousMapperResource
        private ComputeTaskContinuousMapper mapper;
    }

    /** Maps the jobs, each checking the siblings it sees. */
    private abstract static class SiblingsCheckTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            return Arrays.asList(new SiblingsCheckJob((Integer)arg), new SiblingsCheckJob((Integer)arg));
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /** */
    private static class SiblingsCheckJob extends ComputeJobAdapter {
        /** */
        @TaskSessionResource
        private ComputeTaskSession ses;

        /** */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /** @param expSibCnt Expected number of siblings. */
        SiblingsCheckJob(int expSibCnt) {
            super(expSibCnt);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() {
            int expSibCnt = (Integer)argument(0);

            Collection<ComputeJobSibling> sibs = ses.getJobSiblings();

            if (sibs == null || sibs.size() != expSibCnt)
                throw new IgniteException("Unexpected siblings [exp=" + expSibCnt + ", actual=" + sibs + ']');

            // The job is a sibling of itself, so a correct id list always mentions it.
            if (expSibCnt > 0 && ses.getJobSibling(jobCtx.getJobId()) == null)
                throw new IgniteException("Own job id is missing [jobId=" + jobCtx.getJobId() + ", sibs=" + sibs + ']');

            return null;
        }
    }
}
