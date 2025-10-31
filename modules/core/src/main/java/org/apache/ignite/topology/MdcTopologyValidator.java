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

package org.apache.ignite.topology;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Multi-Datacenter Topology Validator.
 *
 * <p>This class is used to validate the cluster topology in a multi-datacenter (MDC) environment 
 * by enforcing rules based on the visibility of datacenters.
 * It provides protection against split-brain scenarios during datacenter failures or unavailability due to network issues.</p>
 *
 * <p>
 * In order to use MdcTopologyValidator one has to specify datacenter ID attribute on each server node.
 * Datacenter ID is an arbitrary string that can be set with {@link IgniteSystemProperties#IGNITE_DATA_CENTER_ID}
 * system property on the node startup.
 * All server nodes belonging to the same datacenter should specify the same datacenter ID, and nodes in different datacenters
 * should have different datacenter IDs.
 * </p>
 * <p>The validator supports two modes of operation:</p>
 * <ul>
 *     <li><strong>Majority-based validation:</strong> When an odd number of datacenters are defined, the validator enables 
 *         data modification operations in the cluster segment that contain a majority of datacenters. 
 *         Any segment containing a minority of datacenters is considered as invalid with only read operations available.</li>
 *     <li><strong>Main Datacenter validation:</strong> When an even number of datacenters are defined, a main datacenter
 *         should be specified. The cluster segment remains write-accessible as long as the main datacenter is visible from
 *         all nodes of that segment.</li>
 * </ul>
 *
 * <p><strong>Usage Requirements:</strong></p>
 * <ul>
 *     <li>If number of datacenters is even, specify a main datacenter via {@link #setMainDatacenter(String)}.
 *     Set of datacenters could be left null.</li>
 *     <li>If number of datacenters is odd, set of datacenter IDs must be specified via {@link #setDatacenters(Set)}.
 *     Main datacenter setting is ignored and could be left null.</li>
 *     
 * </ul>
 *
 * <p><strong>Example:</strong></p>
 * <pre>
 * MdcTopologyValidator mdcValidator = new MdcTopologyValidator();
 * mdcValidator.setDatacenters(Set.of("DC1", "DC2", "DC3"));
 * 
 * CacheConfiguration cacheCfg = new CacheConfiguration("example-cache")
 *     .setTopologyValidator(mdcValidator)
 *     // other cache properties.
 * </pre>
 *
 * <p><strong>Note:</strong> This class is marked with the {@link IgniteExperimental} annotation and may change in future releases.</p>
 *
 * @see TopologyValidator
 * @since Apache Ignite 2.18
 */
@IgniteExperimental
public class MdcTopologyValidator implements TopologyValidator {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Set<String> dcs;

    /** */
    private String mainDc;

    /** @param datacenters Datacenters.*/
    public void setDatacenters(Set<String> datacenters) {
        dcs = datacenters;
    }

    /** @param mainDatacenter Main datacenter.*/
    public void setMainDatacenter(String mainDatacenter) {
        mainDc = mainDatacenter;
    }

    /** */
    public void checkConfiguration() {
        if (dcs == null && mainDc == null)
            throw new IllegalStateException("Either set of datacenters or main datacenter should be specified.");

        if (dcs != null && dcs.isEmpty())
            throw new IllegalStateException("Please provide a non-empty set of datacenters.");

        if (mainDc != null && dcs != null && dcs.size() % 2 == 1)
            throw new IllegalStateException("Uneven number of datacenters cannot be used along with main datacenter. " +
                "Please remove main datacenter setting or specify even number of datacenters.");
    }

    /** {@inheritDoc} */
    @Override public boolean validate(Collection<ClusterNode> nodes) {
        Stream<ClusterNode> servers = nodes.stream().filter(node -> !node.isClient());

        if (mainDc != null)
            return servers.anyMatch(n -> n.dataCenterId() != null && n.dataCenterId().equals(mainDc));

        long visible = servers.map(ClusterNode::dataCenterId).distinct().count();
        int half = dcs.size() / 2;

        return visible > half;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;

        MdcTopologyValidator validator = (MdcTopologyValidator)o;

        return Objects.equals(dcs, validator.dcs) && Objects.equals(mainDc, validator.mainDc);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(dcs, mainDc);
    }
}
