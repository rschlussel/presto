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
package com.facebook.presto.sql.planner.iterative.properties;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Represents a provable maximum number of rows in a final or intermediate result by a PlanNode.
 * For example if a unique key is fully bound to constants by predicates the maxcard property
 * can be set to one. A limit or top operation might set maxcard to the value of their count argument.
 * The value is unknown until determined and set.
 */
public class MaxCardProperty
{
    private final Optional<Long> value;

    public MaxCardProperty()
    {
        this.value = Optional.empty();
    }

    public MaxCardProperty(Long value)
    {
        this.value = Optional.of(value);
    }

    public Optional<Long> getValue()
    {
        return value;
    }

    /**
     * True if this maxcard is more general than another. This is the case if a) neither maxcard is known or
     * b) this maxcard is known and the other maxcard is either unknown or is set to larger or equal value.
     * Note that two maxcard properties are equal if each is more general than the other.
     *
     * @param otherMaxCardProperty
     * @return True if maxCardProperty is more general than otherMaxCardProperty or False otherwise.
     */
    public boolean moreGeneral(MaxCardProperty otherMaxCardProperty)
    {
        return (!value.isPresent() && !otherMaxCardProperty.value.isPresent()) ||
                (value.isPresent() && (!otherMaxCardProperty.value.isPresent() || (otherMaxCardProperty.value.get() >= value.get())));
    }

    /**
     * True if maxcard is known and set to the value 1.
     * This guarantee can be used to eliminate redundant sorts, distincts, topN's, etc.
     *
     * @return True if maxcard is set and to the value 1.
     */
    public boolean isAtMostOne()
    {
        return this.isAtMost(1);
    }

    /**
     * True if maxcard is known and is at most n.
     *
     * @return True if maxcard is known and is at most n.
     */
    public boolean isAtMost(long n)
    {
        if (value.isPresent()) {
            return (value.get().longValue() <= n);
        }
        else {
            return false;
        }
    }

    /**
     * Performs the product of 2 maxCards if both have known values.
     * Used to compute the maxcard of a join.
     *
     * @param maxCard1
     * @param maxCard2
     */
    public static MaxCardProperty multiply(MaxCardProperty maxCard1, MaxCardProperty maxCard2)
    {
        //the product of empty and anything else is empty
        if (!maxCard1.getValue().isPresent() || !maxCard2.getValue().isPresent()) {
            return new MaxCardProperty();
        }
        //new value is present and so multiply the current value if it is present

        return new MaxCardProperty(maxCard1.getValue().get() * maxCard2.getValue().get());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("value", (this.value.isPresent() ? value.get() : "null"))
                .toString();
    }
}
