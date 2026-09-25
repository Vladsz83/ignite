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

package org.apache.ignite.util;

import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.snapshot.SnapshotListCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;
import static org.junit.Assume.assumeTrue;

/** Test for the command '--snapshot list'. */
public class GridCommandHandlerListSnapshotTest extends GridCommandHandlerAbstractTest {
    /** */
    @Parameter(1)
    public boolean customPath;

    /** */
    @Parameter(2)
    public boolean separatedWorkDir;

    /** */
    @Parameters(name = "customPath={1},ownWorkDir={2}")
    public static Collection<?> parameters() {
        return GridTestUtils.cartesianProduct(
            commandHandlers(),
            F.asList(false, true), // Use custom snapshot path;
            F.asList(false, true) // Separated (own) work directory.
        );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        /** Handy if test running is interrupted and {@link #afterTest()} isn't invoked. */
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void cleanPersistenceDir() throws Exception {
        super.cleanPersistenceDir();

        // Also cleans separated snapshot working directories and custom snapshot paths.
        try (DirectoryStream<Path> files = newDirectoryStream(Paths.get(U.defaultWorkDirectory()))) {
            for (Path path : files)
                U.delete(path);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (separatedWorkDir)
            cfg.setWorkDirectory(new File(U.defaultWorkDirectory(), igniteInstanceName).getAbsolutePath());

        return cfg;
    }

    /** */
    @Test
    public void testSnapshotListSingleSnapshot() throws Exception {
        // A custom snapshot path actually puts snapshots in a shared directory. This skews the results when dedicated
        // work directories are set.
        assumeTrue(!customPath || !separatedWorkDir);

        int entriesCnt = 100;
        int initNodes = 3;

        IgniteEx ig = (IgniteEx)startGridsMultiThreaded(initNodes);

        ig.cluster().state(ACTIVE);

        createCacheAndPreload(ig, entriesCnt);

        File cstSnpsRoot = customPath
            ? new File(grid(0).context().pdsFolderResolver().fileTree().snapshotsRoot(), "ex_snapshots")
            : null;

        // Test listing when no snapshots exist.
       // injectTestSystemOut();

//        if (customPath)
//            assertEquals(EXIT_CODE_OK, execute(newCommandHandler(), "--snapshot", "list", "--src",
//                cstSnpsRoot.getAbsolutePath()));
//        else
//            assertEquals(EXIT_CODE_OK, execute(newCommandHandler(), "--snapshot", "list"));
//
//        String out = testOut.toString();
//
//        assertTrue("Expected 'No snapshots found' message, got: " + out,
//            out.contains(SnapshotListCommand.NO_SNAPSHOTS_PREF));
//
//        testOut.reset();

        // Create a snapshot.
        snp(ig).createSnapshot("testSnapshot", customPath ? cstSnpsRoot.getAbsolutePath() : null, false, false)
            .get(getTestTimeout());

        // TODO: add incremental

//        // Add some data and create an incremental snapshot.
//        try (IgniteDataStreamer<Object, Object> streamer = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
//            for (int i = entriesCnt; i < entriesCnt + 50; ++i)
//                streamer.addData(i, i);
//        }

//        snp(ig).createSnapshot("testSnapshot", customPath ? cstSnpsRoot.getAbsolutePath() : null, true, false)
//            .get(getTestTimeout());

        injectTestSystemOut();

        // Now list snapshots - should find "testSnapshot" on all server nodes.
        if (customPath)
            assertEquals(EXIT_CODE_OK, execute(newCommandHandler(), "--snapshot", "list", "--src",
                cstSnpsRoot.getAbsolutePath()));
        else
            assertEquals(EXIT_CODE_OK, execute(newCommandHandler(), "--snapshot", "list"));

        var out = testOut.toString();

        assertFalse("Expected snapshot to be listed, got: " + out,
            out.contains(SnapshotListCommand.NO_SNAPSHOTS_PREF));
        assertTrue("Expected 'Node' in output, got: " + out,
            out.contains(SnapshotListCommand.NODE_PREF));
        assertTrue("Expected 'testSnapshot' in output, got: " + out,
            out.contains("testSnapshot"));

        // With separated work directories, all nodes should have the snapshot.
        if (separatedWorkDir) {
            assertTrue("Expected [cnt=" + initNodes + "] nodes in output, got: " + out,
                countNodeOccurrences(out) == initNodes);
        }

        testOut.reset();

        // Test with non-existent snapshot path.
        assertEquals(EXIT_CODE_OK, execute(newCommandHandler(), "--snapshot", "list", "--src",
            new File(U.defaultWorkDirectory(), "non_existent").getAbsolutePath()));

        out = testOut.toString();

        assertTrue("Expected 'No snapshots found' for non-existent path, got: " + out,
            out.contains(SnapshotListCommand.NO_SNAPSHOTS_PREF));
    }

    /** */
    @Test
    public void testSnapshotListMultipleSnapshots() throws Exception {
        int entriesCnt = 100;
        int initNodes = 2;

        IgniteEx ig = (IgniteEx)startGridsMultiThreaded(initNodes);

        ig.cluster().state(ACTIVE);

        createCacheAndPreload(ig, entriesCnt);

        // Create first snapshot.
        snp(ig).createSnapshot("snp1", null, false, false).get(getTestTimeout());

        // Add data.
        try (IgniteDataStreamer<Object, Object> streamer = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = entriesCnt; i < entriesCnt + 50; ++i)
                streamer.addData(i, i);
        }

        // Create second snapshot.
        snp(ig).createSnapshot("snp2", null, false, false).get(getTestTimeout());

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute(newCommandHandler(), "--snapshot", "list"));

        String out = testOut.toString();

        assertFalse("Expected snapshots to be listed, got: " + out,
            out.contains(SnapshotListCommand.NO_SNAPSHOTS_PREF));
        assertTrue("Expected 'snp1' in output, got: " + out,
            out.contains("snp1"));
        assertTrue("Expected 'snp2' in output, got: " + out,
            out.contains("snp2"));
    }

    /**
     * Counts occurrences of the node prefix in the output.
     */
    private int countNodeOccurrences(String output) {
        int cnt = 0;
        int idx = 0;

        while ((idx = output.indexOf(SnapshotListCommand.NODE_PREF, idx)) != -1) {
            cnt++;
            idx += SnapshotListCommand.NODE_PREF.length();
        }

        return cnt;
    }
}
