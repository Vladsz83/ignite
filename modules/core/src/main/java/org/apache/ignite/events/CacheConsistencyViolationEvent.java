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

package org.apache.ignite.events;

import java.util.Map;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteExperimental;

import static org.apache.ignite.events.EventType.EVT_CONSISTENCY_VIOLATION;

/**
 * <b>This is an experimental API.</b>
 * <p>
 * Event indicates a consistency violation.
 * <p>
 * Grid events are used for notification about what happens within the grid. Note that by
 * design Ignite keeps all events generated on the local node locally and it provides
 * APIs for performing a distributed queries across multiple nodes:
 * <ul>
 *      <li>
 *          {@link org.apache.ignite.IgniteEvents#remoteQuery(org.apache.ignite.lang.IgnitePredicate, long, int...)} -
 *          asynchronously querying events occurred on the nodes specified, including remote nodes.
 *      </li>
 *      <li>
 *          {@link org.apache.ignite.IgniteEvents#localQuery(org.apache.ignite.lang.IgnitePredicate, int...)} -
 *          querying only local events stored on this local node.
 *      </li>
 *      <li>
 *          {@link org.apache.ignite.IgniteEvents#localListen(org.apache.ignite.lang.IgnitePredicate, int...)} -
 *          listening to local grid events (events from remote nodes not included).
 *      </li>
 * </ul>
 * User can also wait for events using method
 * {@link org.apache.ignite.IgniteEvents#waitForLocal(org.apache.ignite.lang.IgnitePredicate, int...)}.
 * <h1 class="header">Events and Performance</h1>
 * Note that by default all events in Ignite are enabled and therefore generated and stored
 * by whatever event storage SPI is configured. Ignite can and often does generate thousands events per seconds
 * under the load and therefore it creates a significant additional load on the system. If these events are
 * not needed by the application this load is unnecessary and leads to significant performance degradation.
 * <p>
 * It is <b>highly recommended</b> to enable only those events that your application logic requires
 * by using {@link org.apache.ignite.configuration.IgniteConfiguration#getIncludeEventTypes()} method in Ignite configuration.
 * Note that certain events are required for Ignite's internal operations and such events will still be generated but not stored by
 * event storage SPI if they are disabled in Ignite configuration.
 *
 * @see EventType#EVT_CONSISTENCY_VIOLATION
 */
@IgniteExperimental
public class CacheConsistencyViolationEvent extends EventAdapter {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** Represents original values of entries.*/
    final Map<Object, Map<ClusterNode, EntryInfo>> entries;

    /**
     * Creates a new instance of CacheConsistencyViolationEvent.
     *
     * @param node Local node.
     * @param msg Event message.
     * @param entries Collection of original entries.
     */
    public CacheConsistencyViolationEvent(
        ClusterNode node,
        String msg,
        Map<Object, Map<ClusterNode, EntryInfo>> entries) {
        super(node, msg, EVT_CONSISTENCY_VIOLATION);

        this.entries = entries;
    }

    /**
     * Returns a mapping of keys to a collection of original entries.
     *
     * @return Collection of original entries.
     */
    public Map<Object, Map<ClusterNode, EntryInfo>> getEntries() {
        return entries;
    }

    /**
     * Inconsistent entry info.
     */
    public interface EntryInfo {
        /**
         * Value.
         */
        public Object getValue();

        /**
         * Version.
         */
        public CacheEntryVersion getVersion();

        /**
         * Located at the primary node.
         */
        public boolean isPrimary();

        /**
         * Marked as correct during the fix.
         */
        public boolean isCorrect();
    }
}
