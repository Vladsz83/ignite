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

import java.io.File;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Cluster snapshot list distributed process request.
 *
 * @see SnapshotListProcess
 */
public class SnapshotListRequest implements Message {
    /** Request ID. */
    @Order(0)
    UUID reqId;

    /** Snapshot directory path. */
    @Order(1)
    @Nullable String snpPath;

    /** Resolved absolute path. Transient. */
    @GridToStringExclude
    @Nullable File resolvedPath;

    /** Default constructor for {@link MessageFactory}. */
    public SnapshotListRequest() {
        // No-op.
    }

    /**
     * @param reqId Request ID.
     * @param snpPath Snapshot directory path.
     */
    SnapshotListRequest(UUID reqId, @Nullable String snpPath) {
        this.reqId = reqId;

        if (!F.isEmpty(snpPath))
            snpPath = snpPath.trim();

        this.snpPath = F.isEmpty(snpPath) ? null : snpPath;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotListRequest.class, this);
    }
}
