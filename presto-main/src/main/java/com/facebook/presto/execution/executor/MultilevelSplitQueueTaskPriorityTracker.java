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
package com.facebook.presto.execution.executor;

import javax.annotation.concurrent.GuardedBy;

import static java.util.Objects.requireNonNull;

public class MultilevelSplitQueueTaskPriorityTracker
        implements TaskPriorityTracker
{
    private final MultilevelSplitQueue splitQueue;

    @GuardedBy("this")
    private long scheduledNanos;
    @GuardedBy("this")
    private volatile MultiLevelSplitQueuePriority priority = new MultiLevelSplitQueuePriority(0, 0);

    public MultilevelSplitQueueTaskPriorityTracker(MultilevelSplitQueue splitQueue)
    {
        this.splitQueue = requireNonNull(splitQueue, "splitQueue is null");
    }

    @Override
    public synchronized Priority updatePriority(long durationNanos)
    {
        scheduledNanos += durationNanos;

        MultiLevelSplitQueuePriority newPriority = splitQueue.updatePriority(priority, durationNanos, scheduledNanos);

        priority = newPriority;
        return newPriority;
    }

    @Override
    public synchronized Priority resetPriority()
    {
        long levelMinPriority = splitQueue.getLevelMinPriority(priority.getLevel(), scheduledNanos);
        if (priority.getLevelPriority() < levelMinPriority) {
            MultiLevelSplitQueuePriority newPriority = new MultiLevelSplitQueuePriority(priority.getLevel(), levelMinPriority);
            priority = newPriority;
            return newPriority;
        }

        return priority;
    }

    @Override
    public synchronized long getScheduledNanos()
    {
        return scheduledNanos;
    }

    @Override
    public synchronized Priority getPriority()
    {
        return priority;
    }
}
