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

package org.apache.ignite.internal.processors.cache;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.marshaller.Marshallers.jdk;

/**
 * Checks how {@link GridCacheEntryInfo} encodes the expiration on the wire. The expiration semantics itself is covered
 * by the TTL and the expiry policy suites, here is only what those cannot see.
 */
public class GridCacheEntryInfoSerializationTest extends GridCommonAbstractTest {
    /** */
    private static final int CACHE_ID = 42;

    /** */
    private static final GridCacheVersion VER = new GridCacheVersion(1, 2, 3);

    /** */
    private final MessageFactory<?> msgFactory = new IgniteMessageFactoryImpl<>(
        new MessageFactoryProvider[] {new CoreMessagesProvider(jdk(), jdk())});

    /** The mark of a never expiring entry has to survive the transfer, so it has to live in a transferred field. */
    @Test
    public void testNeverExpiringEntryStaysNeverExpiring() {
        assertEquals(0, writeAndReadBack(entryInfo(0)).expireTime());
    }

    /** The wire format compresses a long, so a sentinel far from zero costs 10 bytes per entry instead of 1. */
    @Test
    public void testNeverExpiringEntryIsTheCheapestOnTheWire() {
        int neverExpiring = write(entryInfo(0)).position();
        int expiring = write(entryInfo(U.currentTimeMillis() + 60_000)).position();

        assertTrue("A never expiring entry takes " + neverExpiring + " bytes on the wire while an expiring one " +
            "takes only " + expiring, neverExpiring < expiring);
    }

    /** A negative remainder marks the never expiring entry, so an entry past its expiration must not transfer one. */
    @Test
    public void testAlreadyExpiredEntryDoesNotBecomeEternal() {
        long rcvd = writeAndReadBack(entryInfo(U.currentTimeMillis() - 60_000)).expireTime();

        assertTrue("Expired entry lost its expiration", rcvd > 0);
        assertTrue("Expired entry is not expired anymore: " + rcvd, rcvd <= U.currentTimeMillis());
    }

    /** */
    private GridCacheEntryInfo entryInfo(long expireTime) {
        return new GridCacheEntryInfo(CACHE_ID, null, null, VER, expireTime, 0);
    }

    /** */
    private GridCacheEntryInfo writeAndReadBack(GridCacheEntryInfo info) {
        ByteBuffer buf = write(info);

        buf.flip();

        DirectMessageReader reader = new DirectMessageReader(msgFactory, null);

        reader.setBuffer(buf);

        GridCacheEntryInfo res = (GridCacheEntryInfo)reader.readMessage(false);

        assertNotNull("Message is not fully read", res);

        return res;
    }

    /** @return Buffer positioned right after the written message. */
    private ByteBuffer write(GridCacheEntryInfo info) {
        ByteBuffer buf = ByteBuffer.allocate(1024);

        DirectMessageWriter writer = new DirectMessageWriter(msgFactory);

        writer.setBuffer(buf);

        assertTrue("Message is not fully written", writer.writeMessage(info, false));

        return buf;
    }
}
