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

import java.util.Map;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Single-node result of the snapshot list distributed process.
 *
 * @see SnapshotListProcess
 */
public class SnapshotListResponse implements Message {
    /** Snapshot name -> size in bytes (for available snapshots). */
    @Order(0)
    @Nullable Map<String, Long> snapshotSizes;

    /** Snapshot name -> status string (for snapshots in transient state: CREATING, DELETING). */
    @Order(1)
    @Nullable Map<String, String> snapshotStatuses;

    /** Default constructor for {@link MessageFactory}. */
    public SnapshotListResponse() {
        // No-op.
    }

    /**
     * @param sizes Snapshot sizes (name -> size bytes).
     * @param statuses Snapshot statuses (name -> status) for transient snapshots.
     */
    SnapshotListResponse(@Nullable Map<String, Long> sizes, @Nullable Map<String, String> statuses) {
        this.snapshotSizes = sizes;
        this.snapshotStatuses = statuses;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotListResponse.class, this);
    }
}
