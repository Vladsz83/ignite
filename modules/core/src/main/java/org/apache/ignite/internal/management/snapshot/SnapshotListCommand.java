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

package org.apache.ignite.internal.management.snapshot;

import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotListProcessResult;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/** Snapshot list command. */
public class SnapshotListCommand extends AbstractSnapshotCommand<SnapshotListCommandArg, SnapshotListProcessResult> {
    /** */
    public static final String DESC = "Lists all snapshots on all online server nodes with their sizes";

    /** */
    public static final String NO_SNAPSHOTS_PREF = "No snapshots found on current server nodes";

    /** */
    public static final String NODE_PREF = "Node ";

    /** */
    public static final String SNP_PREF = "  ";

    /** */
    public static final String SIZE_SUFFIX = ": ";

    /**
     * {@inheritDoc}
     */
    @Override public String description() {
        return DESC;
    }

    /** {@inheritDoc} */
    @Override public Class<SnapshotListCommandArg> argClass() {
        return SnapshotListCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<SnapshotListTask> taskClass() {
        return SnapshotListTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(SnapshotListCommandArg arg, SnapshotListProcessResult res, Consumer<String> printer) {
        boolean anyFound = false;

        for (Map.Entry<UUID, String> entry : res.nodeConsistentIds().entrySet()) {
            UUID nodeId = entry.getKey();
            String consId = entry.getValue();

            Map<String, Long> sizes = res.nodeSnapshotSizes().get(nodeId);
            Map<String, String> statuses = res.nodeSnapshotStatuses().get(nodeId);

            boolean hasSnapshots = (!F.isEmpty(sizes)) || (!F.isEmpty(statuses));

            if (hasSnapshots)
                anyFound = true;
        }

        if (!anyFound) {
            printer.accept(NO_SNAPSHOTS_PREF);

            return;
        }

        for (Map.Entry<UUID, String> entry : res.nodeConsistentIds().entrySet()) {
            UUID nodeId = entry.getKey();
            String consId = entry.getValue();

            Map<String, Long> sizes = res.nodeSnapshotSizes().get(nodeId);
            Map<String, String> statuses = res.nodeSnapshotStatuses().get(nodeId);

            boolean hasSnapshots = (!F.isEmpty(sizes)) || (!F.isEmpty(statuses));

            if (!hasSnapshots)
                continue;

            printer.accept(NODE_PREF + consId + " [" + nodeId + "]:");

            if (sizes != null) {
                for (Map.Entry<String, Long> snp : sizes.entrySet()) {
                    printer.accept(SNP_PREF + snp.getKey() + SIZE_SUFFIX + U.humanReadableByteCount(snp.getValue()));
                }
            }

            if (statuses != null) {
                for (Map.Entry<String, String> snp : statuses.entrySet()) {
                    printer.accept(SNP_PREF + snp.getKey() + SIZE_SUFFIX + snp.getValue());
                }
            }

            printer.accept(U.nl());
        }
    }
}
