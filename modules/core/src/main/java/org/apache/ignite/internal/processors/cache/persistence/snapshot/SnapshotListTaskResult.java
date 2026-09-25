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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.snapshot.SnapshotListTask;

/** Accumulated result of {@link SnapshotListTask}. */
public final class SnapshotListTaskResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Lists of snapshots per node. */
    @Order(0)
    Map<UUID, List<SnapshotData>> nodesResults = new HashMap<>();

    /** Default constructor for serialization purposes. */
    public SnapshotListTaskResult() {
        // No-op.
    }

    /** */
    public void add(UUID nodeId, String snpName, long size, long creationTime) {
        nodesResults.compute(nodeId, (nid, nodeSnps) -> {
            if(nodeSnps == null)
                nodeSnps = new ArrayList<>();

            nodeSnps.add(new SnapshotData(snpName, size, creationTime));

            return nodeSnps;
        });
    }

    /** */
    public void compose(SnapshotListTaskResult other) {
        nodesResults.putAll(other.nodesResults);
    }

    /** */
    public static class SnapshotData extends IgniteDataTransferObject {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** */
        @Order(0)
        String name;

        /** */
        @Order(1)
        long size;

        /** */
        @Order(2)
        long creationTime;

        /** Empty constructor for serialization purposes. */
        public SnapshotData() {
            // No-op.
        }

        /** */
        private SnapshotData(String name, long size, long creationTime) {
            this.name = name;
            this.size = size;
            this.creationTime = creationTime;
        }

        /** */
        public String name() {
            return name;
        }

        /** */
        public long size() {
            return size;
        }

        /** */
        public long creationTime() {
            return creationTime;
        }
    }
}
