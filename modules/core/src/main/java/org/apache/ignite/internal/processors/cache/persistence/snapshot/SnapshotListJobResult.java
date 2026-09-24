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
import java.util.List;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.jetbrains.annotations.Nullable;

/** Result of {@link SnapshotListTaskResult}. */
public final class SnapshotListJobResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    private @Nullable List<SnapshotData> snapshotsData;

    /** Default constructor for serialization purposes. */
    public SnapshotListJobResult() {
        // No-op.
    }

    /** */
    public void accept(String snpName, long size, long creationTime) {
        if (snapshotsData == null)
            snapshotsData = new ArrayList<>();

        snapshotsData.add(new SnapshotData(snpName, size, creationTime));
    }

    /** */
    public void acceptCreating(String snpName) {
        if (snapshotsData == null)
            snapshotsData = new ArrayList<>();

        snapshotsData.add(new SnapshotData(snpName, "creating"));
    }

    /** */
    public static class SnapshotData {
        /** */
        private String name;

        /** */
        private long size;

        /** */
        private long creationTime;

        /** */
        private @Nullable String status;

        /** */
        public SnapshotData(String name, long size, long creationTime) {
            this.name = name;
            this.size = size;
            this.creationTime = creationTime;
        }

        /** */
        public SnapshotData(String name, String status) {
            this.name = name;
            this.status = status;
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

        /** */
        public @Nullable String status() {
            return status;
        }
    }
}
