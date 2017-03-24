/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.spiller;

import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.memory.LocalMemoryContext;
import com.facebook.presto.operator.SpillContext;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Path;
import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.OUT_OF_SPILL_SPACE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.nio.file.Files.getFileStore;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class FileSingleStreamSpillerFactory
        implements SingleStreamSpillerFactory
{
    private final ListeningExecutorService executor;
    private final PagesSerdeFactory serdeFactory;
    private final List<Path> spillPaths;
    private final SpillerStats spillerStats;
    private final double minimumFreeSpaceThreshold;
    private int roundRobinIndex;

    @Inject
    public FileSingleStreamSpillerFactory(BlockEncodingSerde blockEncodingSerde, SpillerStats spillerStats, FeaturesConfig featuresConfig)
    {
        this(
                listeningDecorator(newFixedThreadPool(
                        requireNonNull(featuresConfig, "featuresConfig is null").getSpillerThreads(),
                        daemonThreadsNamed("binary-spiller-%s"))),
                blockEncodingSerde,
                spillerStats,
                requireNonNull(featuresConfig, "featuresConfig is null").getSpillerSpillPaths(),
                requireNonNull(featuresConfig, "featuresConfig is null").getSpillMaxUsedSpaceThreshold());
    }

    public FileSingleStreamSpillerFactory(
            ListeningExecutorService executor,
            BlockEncodingSerde blockEncodingSerde,
            SpillerStats spillerStats,
            List<Path> spillPaths,
            double maxUsedSpaceThreshold)
    {
        this.serdeFactory = new PagesSerdeFactory(requireNonNull(blockEncodingSerde, "blockEncodingSerde is null"), false);
        this.executor = requireNonNull(executor, "executor is null");
        this.spillerStats = requireNonNull(spillerStats, "spillerStats can not be null");
        requireNonNull(spillPaths, "spillPaths is null");
        checkArgument(spillPaths.size() >= 1, "At least one spill path required");
        this.spillPaths = ImmutableList.copyOf(spillPaths);
        spillPaths.forEach(path -> path.toFile().mkdirs());
        this.minimumFreeSpaceThreshold = requireNonNull(maxUsedSpaceThreshold, "maxUsedSpaceThreshold can not be null");
        this.roundRobinIndex = 0;
    }

    @Override
    public SingleStreamSpiller create(List<Type> types, SpillContext spillContext, LocalMemoryContext memoryContext)
    {
        return new FileSingleStreamSpiller(serdeFactory.createPagesSerde(), executor, getNextSpillPath(), spillerStats, spillContext, memoryContext);
    }

    private synchronized Path getNextSpillPath()
    {
        int spillPathsCount = spillPaths.size();
        for (int i = 0; i < spillPathsCount; ++i) {
            int pathIndex = (roundRobinIndex + i) % spillPathsCount;
            Path path = spillPaths.get(pathIndex);
            if (hasEnoughDiskSpace(path)) {
                roundRobinIndex = (roundRobinIndex + i + 1) % spillPathsCount;
                return path;
            }
        }
        throw new PrestoException(OUT_OF_SPILL_SPACE, "No free space available for spill");
    }

    private boolean hasEnoughDiskSpace(Path path)
    {
        try {
            FileStore fileStore = getFileStore(path);
            return fileStore.getUsableSpace() > fileStore.getTotalSpace() * (1.0 - minimumFreeSpaceThreshold);
        }
        catch (IOException e) {
            throw new PrestoException(OUT_OF_SPILL_SPACE, "Cannot determine free space for spiell", e);
        }
    }
}
