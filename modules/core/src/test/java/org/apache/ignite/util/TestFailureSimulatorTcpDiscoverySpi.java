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

package org.apache.ignite.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.SocketChannel;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

/**
 * Tcp Discovery able to simulate network failure on client socket.
 */
public class TestFailureSimulatorTcpDiscoverySpi extends TcpDiscoverySpi {
    /**
     * If not {@code null}, enables network timeout simulation. Controls direction of traffic droppage:
     * negative incoming (read(...)), positive for outgoing (write(...)), 0 for both.
     */
    private volatile Integer timeout;

    /**  */
    private volatile int direction;

    /** {@inheritDoc} */
    @Override protected Socket createSocket0() throws IOException {
        return new SocketWrap(super.createSocket0());
    }

    /**
     * Enables simulation of network timeout.
     *
     * @param direction If negative, enables timeout simulation for incomming traffic. If positive, enables timeout
     *                  simulation for outgoing traffic. Set 0 to simlate failure for both traffics.
     * @param delay     Milliseconds of awaiting before raising {@code SocketTimeoutException}.
     * @see SocketWrap#simulateTimeout(boolean)
     */
    public synchronized void simulateNetworkTimeout(int direction, int delay) {
        if (timeout != null)
            throw new IllegalStateException("Failure simulation is already set.");

        timeout = delay;

        this.direction = direction;
    }

    /** TODO */
    public synchronized void disableNetworkTimeoutSimalation() {
        timeout = null;
    }

    /**
     * If enabled and required, simulates network timeout and  raises {@code SocketTimeoutException}.
     *
     * @param read If {@code true}, the operation is considered as reading. Otherwise, as writting.
     * @see #simulateNetworkTimeout(int, int)
     */
    private void simulateTimeout(boolean read) throws SocketTimeoutException {
        if (timeout == null || read && direction > 0 || !read && direction < 0)
            return;

        try {
            Thread.sleep(timeout);
        }
        catch (InterruptedException ignored) {
            // No-op.
        }

        throw new SocketTimeoutException("Simulated failure after delay: " + timeout + "ms.");
    }

    /**
     * Network failure simulator.
     */
    private class SocketWrap extends Socket {
        /** The real socket to simulate failure of. */
        private final Socket delegate;

        /**
         * Constructor.
         *
         * @param sock The real socket to simulate failure of.
         */
        private SocketWrap(Socket sock) {
            delegate = sock;
        }

        /** {@inheritDoc} */
        @Override public OutputStream getOutputStream() throws IOException {
            OutputStream src = delegate.getOutputStream();

            return new OutputStream() {
                /** {@inheritDoc} */
                @Override public void write(byte[] b) throws IOException {
                    simulateTimeout(false);

                    src.write(b);
                }

                /** {@inheritDoc} */
                @Override public void write(byte[] b, int off, int len) throws IOException {
                    simulateTimeout(false);

                    src.write(b, off, len);
                }

                /** {@inheritDoc} */
                @Override public void write(int b) throws IOException {
                    simulateTimeout(false);

                    src.write(b);
                }

                /** {@inheritDoc} */
                @Override public void flush() throws IOException {
                    simulateTimeout(false);

                    src.flush();
                }

                /** {@inheritDoc} */
                @Override public void close() throws IOException {
                    src.close();
                }
            };
        }

        /** {@inheritDoc} */
        @Override public InputStream getInputStream() throws IOException {
            InputStream src = delegate.getInputStream();

            return new InputStream() {
                @Override public int read(byte[] b) throws IOException {
                    simulateTimeout(true);

                    return src.read(b);
                }

                @Override public int read(byte[] b, int off, int len) throws IOException {
                    simulateTimeout(true);

                    return src.read(b, off, len);
                }

                @Override public long skip(long n) throws IOException {
                    simulateTimeout(true);

                    return src.skip(n);
                }

                @Override public int available() throws IOException {
                    return src.available();
                }

                @Override public void close() throws IOException {
                    src.close();
                }

                @Override public synchronized void mark(int readlimit) {
                    src.mark(readlimit);
                }

                @Override public synchronized void reset() throws IOException {
                    src.reset();
                }

                @Override public boolean markSupported() {
                    return src.markSupported();
                }

                @Override public int read() throws IOException {
                    simulateTimeout(true);

                    return src.read();
                }
            };
        }

        /** {@inheritDoc} */
        @Override public SocketChannel getChannel() {
            throw new UnsupportedOperationException("Failure simulation for socket channel is not supported yet.");
        }

        /** {@inheritDoc} */
        @Override public void connect(SocketAddress endpoint) throws IOException {
            simulateTimeout(false);

            delegate.connect(endpoint);
        }

        /** {@inheritDoc} */
        @Override public void connect(SocketAddress endpoint, int timeout) throws IOException {
            simulateTimeout(false);

            delegate.connect(endpoint, timeout);
        }

        /** {@inheritDoc} */
        @Override public void bind(SocketAddress bindpoint) throws IOException {
            delegate.bind(bindpoint);
        }

        /** {@inheritDoc} */
        @Override public InetAddress getInetAddress() {
            return delegate.getInetAddress();
        }

        /** {@inheritDoc} */
        @Override public InetAddress getLocalAddress() {
            return delegate.getLocalAddress();
        }

        /** {@inheritDoc} */
        @Override public int getPort() {
            return delegate.getPort();
        }

        /** {@inheritDoc} */
        @Override public int getLocalPort() {
            return delegate.getLocalPort();
        }

        /** {@inheritDoc} */
        @Override public SocketAddress getRemoteSocketAddress() {
            return delegate.getRemoteSocketAddress();
        }

        /** {@inheritDoc} */
        @Override public SocketAddress getLocalSocketAddress() {
            return delegate.getLocalSocketAddress();
        }

        /** {@inheritDoc} */
        @Override public void setTcpNoDelay(boolean on) throws SocketException {
            delegate.setTcpNoDelay(on);
        }

        /** {@inheritDoc} */
        @Override public boolean getTcpNoDelay() throws SocketException {
            return delegate.getTcpNoDelay();
        }

        /** {@inheritDoc} */
        @Override public void setSoLinger(boolean on, int linger) throws SocketException {
            delegate.setSoLinger(on, linger);
        }

        /** {@inheritDoc} */
        @Override public int getSoLinger() throws SocketException {
            return delegate.getSoLinger();
        }

        /** {@inheritDoc} */
        @Override public void sendUrgentData(int data) throws IOException {
            delegate.sendUrgentData(data);
        }

        /** {@inheritDoc} */
        @Override public void setOOBInline(boolean on) throws SocketException {
            delegate.setOOBInline(on);
        }

        /** {@inheritDoc} */
        @Override public boolean getOOBInline() throws SocketException {
            return delegate.getOOBInline();
        }

        /** {@inheritDoc} */
        @Override public synchronized void setSoTimeout(int timeout) throws SocketException {
            delegate.setSoTimeout(timeout);
        }

        /** {@inheritDoc} */
        @Override public synchronized int getSoTimeout() throws SocketException {
            return delegate.getSoTimeout();
        }

        /** {@inheritDoc} */
        @Override public synchronized void setSendBufferSize(int size) throws SocketException {
            delegate.setSendBufferSize(size);
        }

        /** {@inheritDoc} */
        @Override public synchronized int getSendBufferSize() throws SocketException {
            return delegate.getSendBufferSize();
        }

        /** {@inheritDoc} */
        @Override public synchronized void setReceiveBufferSize(int size) throws SocketException {
            delegate.setReceiveBufferSize(size);
        }

        /** {@inheritDoc} */
        @Override public synchronized int getReceiveBufferSize() throws SocketException {
            return delegate.getReceiveBufferSize();
        }

        /** {@inheritDoc} */
        @Override public void setKeepAlive(boolean on) throws SocketException {
            delegate.setKeepAlive(on);
        }

        /** {@inheritDoc} */
        @Override public boolean getKeepAlive() throws SocketException {
            return delegate.getKeepAlive();
        }

        /** {@inheritDoc} */
        @Override public void setTrafficClass(int tc) throws SocketException {
            delegate.setTrafficClass(tc);
        }

        /** {@inheritDoc} */
        @Override public int getTrafficClass() throws SocketException {
            return delegate.getTrafficClass();
        }

        /** {@inheritDoc} */
        @Override public void setReuseAddress(boolean on) throws SocketException {
            delegate.setReuseAddress(on);
        }

        /** {@inheritDoc} */
        @Override public boolean getReuseAddress() throws SocketException {
            return delegate.getReuseAddress();
        }

        /** {@inheritDoc} */
        @Override public synchronized void close() throws IOException {
            delegate.close();
        }

        /** {@inheritDoc} */
        @Override public void shutdownInput() throws IOException {
            delegate.shutdownInput();
        }

        /** {@inheritDoc} */
        @Override public void shutdownOutput() throws IOException {
            delegate.shutdownOutput();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return delegate.toString();
        }

        /** {@inheritDoc} */
        @Override public boolean isConnected() {
            return delegate.isConnected();
        }

        /** {@inheritDoc} */
        @Override public boolean isBound() {
            return delegate.isBound();
        }

        /** {@inheritDoc} */
        @Override public boolean isClosed() {
            return delegate.isClosed();
        }

        /** {@inheritDoc} */
        @Override public boolean isInputShutdown() {
            return delegate.isInputShutdown();
        }

        /** {@inheritDoc} */
        @Override public boolean isOutputShutdown() {
            return delegate.isOutputShutdown();
        }

        /** {@inheritDoc} */
        @Override public void setPerformancePreferences(int connectionTime, int latency, int bandwidth) {
            delegate.setPerformancePreferences(connectionTime, latency, bandwidth);
        }
    }
}
