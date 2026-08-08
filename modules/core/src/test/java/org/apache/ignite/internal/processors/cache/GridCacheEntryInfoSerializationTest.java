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
 * Checks how {@link GridCacheEntryInfo} carries the expiration time over the wire. The key and the value are left
 * null: they need a cache object context to marshal and take no part in the expiration.
 */
public class GridCacheEntryInfoSerializationTest extends GridCommonAbstractTest {
    /** */
    private static final int CACHE_ID = 42;

    /** */
    private static final GridCacheVersion VER = new GridCacheVersion(1, 2, 3);

    /** The expiration is re-based on a clock that {@link U#currentTimeMillis()} keeps 10 ms coarse. */
    private static final long CLOCK_TOLERANCE = 1_000;

    /** */
    private final MessageFactory<?> msgFactory = new IgniteMessageFactoryImpl<>(
        new MessageFactoryProvider[] {new CoreMessagesProvider(jdk(), jdk())});

    /** A never expiring entry keeps the expiration unset on the other side. */
    @Test
    public void testNeverExpiringEntry() {
        assertEquals(0, writeAndReadBack(entryInfo(0)).expireTime());
    }

    /** An expiring entry arrives with the same time left to live, counted from the receiver clock. */
    @Test
    public void testExpiringEntry() {
        long expireTime = U.currentTimeMillis() + 60_000;

        long rcvd = writeAndReadBack(entryInfo(expireTime)).expireTime();

        assertTrue("Expire time is not preserved: " + rcvd + " instead of " + expireTime,
            Math.abs(rcvd - expireTime) < CLOCK_TOLERANCE);
    }

    /** An entry past its expiration time stays expired on the other side instead of becoming eternal. */
    @Test
    public void testAlreadyExpiredEntry() {
        long rcvd = writeAndReadBack(entryInfo(U.currentTimeMillis() - 60_000)).expireTime();

        assertTrue("Expired entry lost its expiration", rcvd > 0);
        assertTrue("Expired entry is not expired anymore: " + rcvd, rcvd <= U.currentTimeMillis());
    }

    /**
     * Most caches have no expiry policy, so a never expiring entry is the common case and must stay the cheapest one.
     * The wire format compresses a long, so a sentinel far from zero costs 10 bytes per entry instead of 1.
     */
    @Test
    public void testNeverExpiringEntryIsTheCheapestOnTheWire() {
        int neverExpiring = write(entryInfo(0)).position();
        int expiring = write(entryInfo(U.currentTimeMillis() + 60_000)).position();

        assertTrue("A never expiring entry takes " + neverExpiring + " bytes on the wire while an expiring one " +
            "takes only " + expiring, neverExpiring < expiring);
    }

    /** Writing the same entry twice gives the same result: marshalling does not mutate the message. */
    @Test
    public void testRepeatedWriteIsIdempotent() {
        GridCacheEntryInfo info = entryInfo(U.currentTimeMillis() + 60_000);

        assertEquals(write(info).position(), write(info).position());

        long first = writeAndReadBack(info).expireTime();
        long second = writeAndReadBack(info).expireTime();

        assertTrue("Repeated marshalling shifted the expire time: " + first + " then " + second,
            Math.abs(second - first) < CLOCK_TOLERANCE);
    }

    /**
     * @param expireTime Absolute expiration time, {@code 0} if the entry never expires.
     * @return Entry info to transfer.
     */
    private GridCacheEntryInfo entryInfo(long expireTime) {
        return new GridCacheEntryInfo(CACHE_ID, null, null, VER, expireTime, 0);
    }

    /**
     * @param info Entry info to transfer.
     * @return Entry info read back from its own wire form.
     */
    private GridCacheEntryInfo writeAndReadBack(GridCacheEntryInfo info) {
        ByteBuffer buf = write(info);

        buf.flip();

        DirectMessageReader reader = new DirectMessageReader(msgFactory, null);

        reader.setBuffer(buf);

        GridCacheEntryInfo res = (GridCacheEntryInfo)reader.readMessage(false);

        assertNotNull("Message is not fully read", res);

        return res;
    }

    /**
     * @param info Entry info to transfer.
     * @return Buffer positioned right after the written message.
     */
    private ByteBuffer write(GridCacheEntryInfo info) {
        ByteBuffer buf = ByteBuffer.allocate(1024);

        DirectMessageWriter writer = new DirectMessageWriter(msgFactory);

        writer.setBuffer(buf);

        assertTrue("Message is not fully written", writer.writeMessage(info, false));

        return buf;
    }
}
