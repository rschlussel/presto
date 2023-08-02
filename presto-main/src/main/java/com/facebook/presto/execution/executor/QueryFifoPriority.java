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

import java.util.Objects;

public class QueryFifoPriority
        implements Priority, Comparable<QueryFifoPriority>
{
    public static final QueryFifoPriority LOWEST_PRIORITY = new QueryFifoPriority(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
    private final long splitStartTime;
    private final long pipelineStartTime;
    private final long taskStartTime;

    private final long queryStartTime;

    public QueryFifoPriority(long splitStartTime, long pipelineStartTime, long taskStartTime, long queryStartTime)
    {
        this.splitStartTime = splitStartTime;
        this.pipelineStartTime = pipelineStartTime;
        this.taskStartTime = taskStartTime;
        this.queryStartTime = queryStartTime;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryFifoPriority that = (QueryFifoPriority) o;
        return splitStartTime == that.splitStartTime && pipelineStartTime == that.pipelineStartTime && taskStartTime == that.taskStartTime && queryStartTime == that.queryStartTime;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(splitStartTime, pipelineStartTime, taskStartTime, queryStartTime);
    }

    @Override
    public int compareTo(Priority o)
    {
        return this.compareTo((QueryFifoPriority) o);
    }

    @Override
    public int compareTo(QueryFifoPriority o)
    {
        // order by start time of query, then task, pipeline, and split. Earlier starts are higher priority,
        // so we take the inverse of Long.compare
        if (queryStartTime != o.queryStartTime) {
            return -Long.compare(queryStartTime, o.queryStartTime);
        }
        if (taskStartTime != o.taskStartTime) {
            return -Long.compare(this.taskStartTime, o.taskStartTime);
        }
        if (pipelineStartTime != o.pipelineStartTime) {
            return -Long.compare(this.pipelineStartTime, o.pipelineStartTime);
        }
        return -Long.compare(this.splitStartTime, o.splitStartTime);
    }

    @Override
    public int getLevel()
    {
        return 0;
    }
}
