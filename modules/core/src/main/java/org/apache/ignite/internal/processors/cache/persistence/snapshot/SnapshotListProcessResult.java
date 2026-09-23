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

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

/** Result of {@link SnapshotListProcess}. */
public final class SnapshotListProcessResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Per-node snapshot sizes: nodeId -> (snapshotName -> size in bytes). */
    @Order(0)
    Map<UUID, Map<String, Long>> nodeSnapshotSizes;

    /** Per-node snapshot statuses: nodeId -> (snapshotName -> status) for transient snapshots. */
    @Order(1)
    Map<UUID, Map<String, String>> nodeSnapshotStatuses;

    /** Node consistent IDs: nodeId -> consistentId. */
    @Order(2)
    Map<UUID, String> nodeConsistentIds;

    /** Default constructor for {@link MessageFactory}. */
    public SnapshotListProcessResult() {
        // No-op.
    }

    /** */
    public SnapshotListProcessResult(
        Map<UUID, Map<String, Long>> nodeSnapshotSizes,
        Map<UUID, Map<String, String>> nodeSnapshotStatuses,
        Map<UUID, String> nodeConsistentIds
    ) {
        this.nodeSnapshotSizes = nodeSnapshotSizes;
        this.nodeSnapshotStatuses = nodeSnapshotStatuses;
        this.nodeConsistentIds = nodeConsistentIds;
    }

    /** */
    public Map<UUID, Map<String, Long>> nodeSnapshotSizes() {
        return Collections.unmodifiableMap(nodeSnapshotSizes);
    }

    /** */
    public Map<UUID, Map<String, String>> nodeSnapshotStatuses() {
        return Collections.unmodifiableMap(nodeSnapshotStatuses);
    }

    /** */
    public Map<UUID, String> nodeConsistentIds() {
        return Collections.unmodifiableMap(nodeConsistentIds);
    }
}
