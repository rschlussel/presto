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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.iterative.Lookup;

import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.textLogicalPlan;
import static java.lang.String.format;

public final class PlanAssert
{
    private PlanAssert() {}

    public static void assertPlan(Session session, Metadata metadata, CostCalculator costCalculator, Plan actual, PlanMatchPattern pattern)
    {
        assertPlan(session, metadata, actual, costCalculator, Lookup.noLookup(), pattern);
    }

    public static void assertPlan(Session session, Metadata metadata, Plan actual, CostCalculator costCalculator, Lookup lookup, PlanMatchPattern pattern)
    {
        MatchResult matches = actual.getRoot().accept(new PlanMatchingVisitor(session, metadata, lookup, actual.getPlanNodeCosts()), pattern);
        if (!matches.isMatch()) {
            String logicalPlan = textLogicalPlan(actual.getRoot(), actual.getTypes(), metadata, costCalculator, session);
            throw new AssertionError(format("Plan does not match, expected [\n\n%s\n] but found [\n\n%s\n]", pattern, logicalPlan));
        }
    }
}
