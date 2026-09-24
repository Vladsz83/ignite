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

package org.apache.ignite.internal.management.snapshot;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotListJobResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotListTaskResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/** */
@GridInternal
public class SnapshotListTask extends VisorMultiNodeTask<SnapshotListCommandArg, SnapshotListTaskResult, SnapshotListJobResult> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<SnapshotListCommandArg, SnapshotListJobResult> job(SnapshotListCommandArg arg) {
        return new SnapshotListJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected SnapshotListTaskResult reduce0(List<ComputeJobResult> list) throws IgniteException {
        return null;
    }

    /** */
    public static T2<Long, FileTime> calculateDirectorySizeVisitor(File dirPath, AtomicInteger skippedCnt) throws IOException {
        AtomicLong totalSize = new AtomicLong(0);

        AtomicReference<FileTime> createTime = new AtomicReference<>();

        Files.walkFileTree(dirPath.toPath(), new SimpleFileVisitor<>() {
            @Override public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                createTime.compareAndSet(null, attrs.creationTime());

                return super.preVisitDirectory(dir, attrs);
            }

            @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                try {
                    // Use attrs instead of Files.size() for efficiency.
                    if (attrs.isRegularFile())
                        totalSize.addAndGet(attrs.size());
                }
                catch (Exception e) {
                    // File may have been deleted between walk start and here.
                    skippedCnt.incrementAndGet();
                }

                return FileVisitResult.CONTINUE;
            }

            @Override public FileVisitResult visitFileFailed(Path file, IOException exc) {
                // File/directory became inaccessible (permission denied, deleted, etc.)
                skippedCnt.incrementAndGet();

                return FileVisitResult.CONTINUE;
            }
        });

        return new T2<>(totalSize.get(), createTime.get());
    }

    /** */
    private static class SnapshotListJob extends SnapshotJob<SnapshotListCommandArg, SnapshotListJobResult> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Snapshot list task argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected SnapshotListJob(SnapshotListCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected SnapshotListJobResult run(SnapshotListCommandArg arg) {
            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

            SnapshotListJobResult jobRes = new SnapshotListJobResult();

            try {
                File resolvedPath = resolveSnapshotsPath(arg.src());

                // TODO: support being deleted too.
                String ignoreName = detectIgnoredName(resolvedPath);

                AtomicInteger skippedCnt = new AtomicInteger();

                for (String snpName : snpMgr.localSnapshotNames(resolvedPath.getAbsolutePath())) {
                    if(snpName.equals(ignoreName)) {
                        jobRes.acceptCreating(snpName);

                        continue;
                    }

                    File snpPath = new File(resolvedPath, snpName);

                    skippedCnt.set(0);

                    var snpRes = calculateDirectorySizeVisitor(snpPath, skippedCnt);

                    jobRes.accept(snpName, snpRes.get1(), snpRes.get2());
                }
            } catch (Exception e) {
                throw new IgniteException("Failed to list local snapshots [src=" + arg.src() + ']', e);
            }

            return jobRes;
        }

        /** */
        private @Nullable String detectIgnoredName(File curPath) {
            var createRq = ignite.context().cache().context().snapshotMgr().currentCreateRequest();

            if (createRq != null) {
                File createRoot = new SnapshotFileTree(ignite.context(), createRq.snapshotName(), createRq.snapshotPath()).root();

                if (createRoot.equals(curPath))
                    return createRq.snapshotName();
            }

            return null;
        }

        /** TODO: move to a sahred with the deletion process place. */
        private File resolveSnapshotsPath(@Nullable String src) throws IOException {
            File res = ignite.context().pdsFolderResolver().fileTree().snapshotsRoot();

            if (src != null) {
                var srcF = new File(src);

                if (srcF.isAbsolute())
                    res = srcF;
                else
                    res = new File(res, src);
            }

            return res.getCanonicalFile();
        }
    }
}
