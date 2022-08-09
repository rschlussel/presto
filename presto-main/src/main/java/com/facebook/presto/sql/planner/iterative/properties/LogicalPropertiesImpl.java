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

import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.LogicalProperties;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.AssignmentUtils;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * Provides an implementation of interface LogicalProperties along with a set
 * of builders that various PlanNode's can use to compute their logical properties.
 * <p>
 * The logical properties of a PlanNode represent properties that hold for the final
 * or intermediate result produced by the PlanNode and are a function of the logical properties
 * of the PlanNode's source(s) and the operation performed by the PlanNode.
 * For example, and AggregationNode with a single grouping key
 * would add a unique key to the properties of its input source.
 * <p>
 * Note that for this implementation to work effectively it must sit behind the TranslateExpressions
 * optimizer as it does not currently deal with original expressions. The TranslateExpressions
 * functionality should ultimately be moved earlier into query compilation as opposed to
 * extending this implementation with support for original expressions.
 */
public class LogicalPropertiesImpl
        implements LogicalProperties
{
    private final MaxCardProperty maxCardProperty;
    private final KeyProperty keyProperty;
    private final EquivalenceClassProperty equivalenceClassProperty;

    public LogicalPropertiesImpl(EquivalenceClassProperty equivalenceClassProperty, MaxCardProperty maxCardProperty, KeyProperty keyProperty)
    {
        this.equivalenceClassProperty = requireNonNull(equivalenceClassProperty, "equivalenceClassProperty is null");
        this.maxCardProperty = requireNonNull(maxCardProperty, "maxCardProperty is null");
        this.keyProperty = requireNonNull(keyProperty, "keyProperty is null");
    }

    public MaxCardProperty getMaxCardProperty()
    {
        return maxCardProperty;
    }

    public KeyProperty getKeyProperty()
    {
        return keyProperty;
    }

    public EquivalenceClassProperty getEquivalenceClassProperty()
    {
        return equivalenceClassProperty;
    }

    /**
     * Determines if one set of logical properties is more general than another set.
     * A set of logical properties is more general than another set if they can satisfy
     * any requirement the other can satisfy. See the corresponding moreGeneral method
     * for each of the individual properties to get more detail on the overall semantics.
     *
     * @param otherLogicalProperties
     * @return True if this logicalproperties is more general than otherLogicalProperties or False otherwise.
     */
    private boolean isMoreGeneralThan(LogicalPropertiesImpl otherLogicalProperties)
    {
        requireNonNull(otherLogicalProperties, "otherLogicalProperties is null");
        return (this.maxCardProperty.moreGeneral(otherLogicalProperties.maxCardProperty) &&
                this.keyProperty.moreGeneral(otherLogicalProperties.keyProperty) &&
                this.equivalenceClassProperty.isMoreGeneralThan(otherLogicalProperties.equivalenceClassProperty));
    }

    /**
     * Determines if two sets of logical properties are equivalent.
     * Two sets of logical properties are equivalent if each is more general than the other.
     *
     * @param otherLogicalProperties
     * @return True if this and otherLogicalProperties are equivalent or False otherwise.
     */
    public boolean equals(LogicalPropertiesImpl otherLogicalProperties)
    {
        requireNonNull(otherLogicalProperties, "otherLogicalProperties is null");
        return ((this.isMoreGeneralThan(otherLogicalProperties)) && otherLogicalProperties.isMoreGeneralThan(this));
    }

    /**
     * Produces the inverse mapping of the provided assignments.
     * The inverse mapping is used to propagate individual properties across a project operation
     * by rewriting the property's variable references to those of the
     * output of the project operation as per the provided assignments.
     */
    private static Map<VariableReferenceExpression, VariableReferenceExpression> inverseVariableAssignments(Assignments assignments)
    {
        //TODO perhaps put this in AssignmentsUtils or ProjectUtils
        requireNonNull(assignments, "assignments is null");
        Map<VariableReferenceExpression, VariableReferenceExpression> inverseVariableAssignments = new HashMap<>();
        for (Map.Entry<VariableReferenceExpression, RowExpression> e : assignments.entrySet()) {
            if (e.getValue() instanceof VariableReferenceExpression) {
                inverseVariableAssignments.put((VariableReferenceExpression) e.getValue(), e.getKey());
            }
        }
        return inverseVariableAssignments;
    }

    /**
     * Encapsulates normalization of the key property in alignment with equivalence class property,
     * and possible setting of max card property if a one record condition is detected.
     * The key property is modified. Maxcard will be modified if a one record condition is detected.
     */
    private static LogicalPropertiesImpl normalize(KeyProperty originalKeyProperty, MaxCardProperty originalMaxCardProperty, EquivalenceClassProperty originalEquivalenceClassProperty)
    {
        KeyProperty keyProperty = originalKeyProperty;
        MaxCardProperty maxCardProperty = originalMaxCardProperty;
        EquivalenceClassProperty equivalenceClassProperty = originalEquivalenceClassProperty;

        if (maxCardProperty.isAtMostOne()) {
            keyProperty = new KeyProperty();
        }
        Optional<KeyProperty> normalizedKeyProperty = keyProperty.normalize(equivalenceClassProperty);
        if (normalizedKeyProperty.isPresent()) {
            keyProperty = normalizedKeyProperty.get();
        }

        if (keyProperty.isEmpty()) {
            maxCardProperty = new MaxCardProperty(1L);
        }

        return new LogicalPropertiesImpl(equivalenceClassProperty, maxCardProperty, keyProperty);
    }

    @Override
    public boolean isDistinct(Set<VariableReferenceExpression> keyVars)
    {
        return this.keyRequirementSatisfied(new Key(keyVars));
    }

    @Override
    public boolean isAtMostSingleRow()
    {
        return this.isAtMost(1);
    }

    @Override
    public boolean isAtMost(long n)
    {
        return maxCardProperty.isAtMost(n);
    }

    private boolean keyRequirementSatisfied(Key keyRequirement)
    {
        requireNonNull(keyRequirement, "keyRequirement is null");
        if (maxCardProperty.isAtMostOne()) {
            return true;
        }
        Optional<Key> normalizedKeyRequirement = keyRequirement.normalize(equivalenceClassProperty);
        if (normalizedKeyRequirement.isPresent()) {
            return keyProperty.satisfiesKeyRequirement(keyRequirement);
        }
        else {
            return false;
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("KeyProperty", keyProperty)
                .add("EquivalenceClassProperty", equivalenceClassProperty)
                .add("MaxCardProperty", maxCardProperty)
                .toString();
    }

    /**
     * This logical properties builder should be used by PlanNode's that simply
     * propagate source properties without changes. For example, a SemiJoin node
     * propagates the inputs of its non-filtering source without adding new properties.
     * A SortNode also propagates the logical properties of its source without change.
     */
    public static class PropagateBuilder
    {
        private final LogicalPropertiesImpl sourceProperties;

        PropagateBuilder(LogicalPropertiesImpl sourceProperties)
        {
            this.sourceProperties = requireNonNull(sourceProperties, "sourceProperties is null");
        }

        LogicalPropertiesImpl build()
        {
            return new LogicalPropertiesImpl(sourceProperties.getEquivalenceClassProperty(), sourceProperties.getMaxCardProperty(), sourceProperties.getKeyProperty());
        }
    }

    /**
     * This logical properties builder is used by a TableScanNode to initialize logical properties from catalog constraints.
     */
    public static class TableScanBuilder
    {
        List<Set<VariableReferenceExpression>> keys;

        TableScanBuilder(List<Set<VariableReferenceExpression>> keys)
        {
            this.keys = requireNonNull(keys, "keys is null");
        }

        LogicalPropertiesImpl build()
        {
            KeyProperty keyProperty = new KeyProperty(
                    keys.stream()
                            .map(Key::new)
                            .collect(toImmutableSet()));
            return new LogicalPropertiesImpl(new EquivalenceClassProperty(), new MaxCardProperty(), keyProperty);
        }
    }

    /**
     * This logical properties builder should be used by PlanNode's that apply predicates.
     * The application of conjunct predicates that equate attributes and constants effects changes to the equivalence class property.
     * When equivalence classes change, specifically when equivalence class heads change, properties that keep a canonical form
     * in alignment with equivalence classes will be affected.
     */
    public static class FilterBuilder
    {
        private final LogicalPropertiesImpl sourceProperties;
        private final RowExpression predicate;
        private final FunctionResolution functionResolution;

        FilterBuilder(LogicalPropertiesImpl sourceProperties, RowExpression predicate, FunctionResolution functionResolution)
        {
            this.sourceProperties = requireNonNull(sourceProperties, "sourceProperties is null");
            this.predicate = requireNonNull(predicate, "predicate is null");
            this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        }

        LogicalPropertiesImpl build()
        {
            KeyProperty keyProperty = sourceProperties.getKeyProperty();
            MaxCardProperty maxCardProperty = sourceProperties.getMaxCardProperty();
            EquivalenceClassProperty sourceEquivalenceProperty = sourceProperties.getEquivalenceClassProperty();
            EquivalenceClassProperty equivalenceClassProperty = sourceEquivalenceProperty.addPredicate(predicate, functionResolution);
            if (!equivalenceClassProperty.equals(sourceEquivalenceProperty)) {
                return normalize(keyProperty, maxCardProperty, equivalenceClassProperty);
            }
            return new LogicalPropertiesImpl(equivalenceClassProperty, maxCardProperty, keyProperty);
        }
    }

    /**
     * This logical properties builder should be used by PlanNode's that project their
     * source properties. For example, a ProjectNode and AggregationNode project their
     * source properties. The former might also reassign property variable references.
     */
    public static class ProjectBuilder
    {
        private final LogicalPropertiesImpl sourceProperties;
        private final Assignments assignments;

        ProjectBuilder(LogicalPropertiesImpl sourceProperties, Assignments assignments)
        {
            this.sourceProperties = requireNonNull(sourceProperties, "sourceProperties is null");
            this.assignments = requireNonNull(assignments, "assignments is null");
        }

        LogicalPropertiesImpl build()
        {
            KeyProperty keyProperty = sourceProperties.getKeyProperty();
            MaxCardProperty maxCardProperty = sourceProperties.getMaxCardProperty();
            EquivalenceClassProperty equivalenceClassProperty = sourceProperties.getEquivalenceClassProperty();

            //project both equivalence classes and key property
            Map<VariableReferenceExpression, VariableReferenceExpression> inverseVariableAssignments = inverseVariableAssignments(assignments);
            keyProperty = keyProperty.project(new InverseVariableMappingsWithEquivalence(equivalenceClassProperty, inverseVariableAssignments));
            equivalenceClassProperty = equivalenceClassProperty.project(inverseVariableAssignments);
            return normalize(keyProperty, maxCardProperty, equivalenceClassProperty);
        }
    }

    /**
     * This logical properties builder should be used by PlanNode's that propagate their
     * source properties and add a limit. For example, TopNNode and LimitNode.
     */
    public static class PropagateAndLimitBuilder
    {
        private final LogicalPropertiesImpl sourceProperties;
        private final long limit;

        PropagateAndLimitBuilder(LogicalPropertiesImpl sourceProperties, long limit)
        {
            requireNonNull(sourceProperties, "sourceProperties is null");
            this.sourceProperties = sourceProperties;
            this.limit = limit;
        }

        LogicalPropertiesImpl build()
        {
            KeyProperty keyProperty = sourceProperties.getKeyProperty();

            long maxCardinality = limit;
            if (sourceProperties.getMaxCardProperty().getValue().isPresent()) {
                maxCardinality = Math.min(sourceProperties.getMaxCardProperty().getValue().get(), limit);
            }
            MaxCardProperty maxCardProperty = new MaxCardProperty(maxCardinality);

            EquivalenceClassProperty equivalenceClassProperty = sourceProperties.getEquivalenceClassProperty();
            if (maxCardProperty.isAtMostOne()) {
                keyProperty = new KeyProperty();
            }
            return new LogicalPropertiesImpl(equivalenceClassProperty, maxCardProperty, keyProperty);
        }
    }

    /**
     * This logical properties builder should be used by PlanNode's that propagate their source
     * properties and add a unique key. For example, an AggregationNode with a single grouping key
     * propagates it's input properties and adds the grouping key attributes as a new unique key.
     * The resulting properties are projected by the provided output variables.
     */
    public static class AggregationBuilder
    {
        private final LogicalPropertiesImpl sourceProperties;
        private final Key key;
        private final List<VariableReferenceExpression> outputVariables;

        AggregationBuilder(LogicalPropertiesImpl sourceProperties, Set<VariableReferenceExpression> keyVariables, List<VariableReferenceExpression> outputVariables, FunctionResolution functionResolution)
        {
            this.sourceProperties = requireNonNull(sourceProperties, "sourceProperties is null");
            requireNonNull(keyVariables, "keyVariables is null");
            checkArgument(!keyVariables.isEmpty(), "keyVariables is empty");
            this.key = new Key(keyVariables);
            this.outputVariables = requireNonNull(outputVariables, "outputVariables is null");
        }

        LogicalPropertiesImpl build()
        {
            MaxCardProperty maxCardProperty = sourceProperties.getMaxCardProperty();
            EquivalenceClassProperty equivalenceClassProperty = sourceProperties.getEquivalenceClassProperty();
            //add the new key and normalize the key property unless there is a single row in the input
            Set<Key> keys = ImmutableSet.<Key>builder()
                    .addAll(sourceProperties.getKeyProperty().getKeys())
                    .add(key)
                    .build();
            LogicalPropertiesImpl logicalProperties = normalize(new KeyProperty(keys), maxCardProperty, equivalenceClassProperty);
            //project the properties using the output variables to ensure only the interesting constraints propagate
            ProjectBuilder projectBuilder = new ProjectBuilder(logicalProperties,
                    AssignmentUtils.identityAssignments(this.outputVariables));
            return projectBuilder.build();
        }
    }

    /**
     * This logical properties builder should be used by PlanNode's that propagate their source
     * properties, add a unique key, and also limit the result. For example, a DistinctLimitNode.
     */
    public static class DistinctLimitBuilder
    {
        private final LogicalPropertiesImpl sourceProperties;
        private final Set<VariableReferenceExpression> keyVariables;
        private final long limit;
        private final List<VariableReferenceExpression> outputVariables;
        private final FunctionResolution functionResolution;

        DistinctLimitBuilder(LogicalPropertiesImpl sourceProperties, Set<VariableReferenceExpression> keyVariables, Long limit, List<VariableReferenceExpression> outputVariables, FunctionResolution functionResolution)
        {
            requireNonNull(sourceProperties, "sourceProperties is null");
            requireNonNull(keyVariables, "keyVariables is null");
            requireNonNull(outputVariables, "outputVariables is null");
            requireNonNull(limit, "limit is null");
            checkArgument(!keyVariables.isEmpty(), "keyVariables is empty");
            this.sourceProperties = sourceProperties;
            this.keyVariables = keyVariables;
            this.outputVariables = outputVariables;
            this.limit = limit;
            this.functionResolution = functionResolution;
        }

        LogicalPropertiesImpl build()
        {
            AggregationBuilder aggregationBuilder = new AggregationBuilder(sourceProperties, keyVariables, outputVariables, functionResolution);
            PropagateAndLimitBuilder propagateAndLimitBuilder = new PropagateAndLimitBuilder(aggregationBuilder.build(), limit);
            return propagateAndLimitBuilder.build();
        }
    }

    /**
     * This logical properties builder should be used by PlanNode's that join two input sources
     * where both input sources contribute variables to the join output (e.g. JoinNode vs. SemiJoinNode).
     * Propagation of the source properties of the join requires a sophisticated analysis of the characteristics of the join.
     * <p>
     * Key and MaxCard Properties...
     * <p>
     * - An inner or left join propagates the key property and maxcard property of the left source if the join is n-to-1,
     * meaning that each row of the left source matches at most one row of the right source. Determining that a join is n-to-1
     * involves forming a key requirement from the equi-join attributes of the right table and querying the logical properties
     * of the right table to determine if those attributes form a unique key. Semi-joins are inherently n-to1.
     * <p>
     * - Conversely, an inner or right join can propagate the key property and maxcard property of the right source if the join is 1-to-n.
     * If an inner join is 1-to-1, which is the case when it is both n-to-1 and 1-to-n, then it follows from the above that the key property
     * of the join result comprises the union of the left source keys and right source keys.
     * <p>
     * - If an inner join is instead m-to-n, meaning that it is neither n-to-1 nor 1-to-n, the key property of the join is formed by
     * concatenating the left source and right source key properties. Concatenating two key properties forms a new key for every
     * possible combination of keys. For example, if key property KP1 has key {A} and key {B,C} and key property KP2 has key {D}
     * and key {E} the concatenating KP1 and KP2 would yield a key property with keys {A,D}, {A,E}, {B,C,D} and {B,C,E}.
     * An m-to-n join propagates the product of the left source MaxCardProperty and right source MaxCardProperty if the values are both known.
     * <p>
     * - Full outer joins do not propagate source key or maxcard properties as they can inject null rows into the result.
     * <p>
     * EquivalenceClass Property ..
     * <p>
     * - The equivalence class property of an inner or left join adds the equivalence classes of the left source.
     * <p>
     * - The equivalence class property of an inner or right join adds the equivalence classes of the right source.
     * <p>
     * - The equivalence class property of an inner join is then updated with any new equivalences resulting from the application of
     * equi-join predicates, or equality conjuncts applied as filters.
     * <p>
     * It follows from the above that inner joins combine the left and right source equivalence classes and that full outer joins do
     * not propagate equivalence classes.
     * Finally, the key property is normalized with the equivalence classes of the join, and both key and equivalence properties are
     * projected with the join’s output attributes.
     */
    public static class JoinBuilder
    {
        private final LogicalPropertiesImpl leftProperties;
        private final LogicalPropertiesImpl rightProperties;
        private final List<JoinNode.EquiJoinClause> equijoinPredicates;
        private final JoinNode.Type joinType;
        private final Optional<RowExpression> filterPredicate;
        private final List<VariableReferenceExpression> outputVariables;
        private final FunctionResolution functionResolution;

        JoinBuilder(LogicalPropertiesImpl leftProperties,
                LogicalPropertiesImpl rightProperties,
                List<JoinNode.EquiJoinClause> equijoinPredicates,
                JoinNode.Type joinType,
                Optional<RowExpression> filterPredicate,
                List<VariableReferenceExpression> outputVariables,
                FunctionResolution functionResolution)
        {

            this.leftProperties = requireNonNull(leftProperties, "leftProperties is null");
            this.rightProperties = requireNonNull(rightProperties, "rightProperties is null");
            this.equijoinPredicates = requireNonNull(equijoinPredicates, "equijoinPredicates is null");
            this.joinType = requireNonNull(joinType, "joinType is null");
            this.filterPredicate = requireNonNull(filterPredicate, "filterPredicate is null");
            this.outputVariables = requireNonNull(outputVariables, "outputVariables is null");
            this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        }

        LogicalPropertiesImpl build()
        {
            Set<VariableReferenceExpression> rightJoinVariables = this.equijoinPredicates.stream().map(predicate -> predicate.getRight()).collect(Collectors.toSet());
            Set<VariableReferenceExpression> leftJoinVariables = this.equijoinPredicates.stream().map(predicate -> predicate.getLeft()).collect(Collectors.toSet());

            MaxCardProperty maxCardProperty;
            KeyProperty keyProperty = new KeyProperty();
            //if n-to-1 inner or left join then propagate left source keys and maxcard
            if ((rightProperties.getMaxCardProperty().isAtMostOne() || (!rightJoinVariables.isEmpty() && rightProperties.isDistinct(rightJoinVariables))) &&
                    ((joinType == INNER || joinType == LEFT) || (joinType == FULL && leftProperties.getMaxCardProperty().isAtMost(1)))) {
                keyProperty = leftProperties.getKeyProperty();
                maxCardProperty = leftProperties.getMaxCardProperty();
            }
            //if 1-to-n inner or right join then propagate right source keys and maxcard
            else if ((leftProperties.getMaxCardProperty().isAtMostOne() || (!leftJoinVariables.isEmpty() && leftProperties.isDistinct(leftJoinVariables))) &&
                    ((joinType == INNER || joinType == RIGHT) || (joinType == FULL && rightProperties.getMaxCardProperty().isAtMost(1)))) {
                keyProperty = rightProperties.getKeyProperty();
                maxCardProperty = rightProperties.getMaxCardProperty();
            }
            //if an n-to-m then multiply maxcards and, if inner join, concatenate keys
            else {
                maxCardProperty = MaxCardProperty.multiply(leftProperties.maxCardProperty, rightProperties.maxCardProperty);
                if (joinType == INNER) {
                    keyProperty = leftProperties.getKeyProperty().concat(rightProperties.getKeyProperty());
                }
            }

            EquivalenceClassProperty equivalenceClassProperty = new EquivalenceClassProperty();
            //propagate left source equivalence classes if nulls cannot be injected
            if (joinType == INNER || joinType == LEFT) {
                equivalenceClassProperty.combineWith(leftProperties.equivalenceClassProperty);
            }

            //propagate right source equivalence classes if nulls cannot be injected
            if (joinType == INNER || joinType == RIGHT) {
                equivalenceClassProperty.combineWith(rightProperties.equivalenceClassProperty);
            }

            //update equivalence classes with equijoin predicates, note that if nulls are injected, equivalence does not hold propagate
            if (joinType == INNER) {
                equijoinPredicates.stream().forEach(joinVariables -> equivalenceClassProperty.addPredicate(joinVariables.getLeft(), joinVariables.getRight()));

                //update equivalence classes with any residual filter predicate
                if (filterPredicate.isPresent()) {
                    equivalenceClassProperty.addPredicate(filterPredicate.get(), functionResolution);
                }
            }

            //since we likely merged equivalence class from left and right source we will normalize the key property
            LogicalPropertiesImpl logicalProperties = normalize(keyProperty, maxCardProperty, equivalenceClassProperty);

            //project the resulting properties by the output variables
            ProjectBuilder projectBuilder = new ProjectBuilder(logicalProperties,
                    AssignmentUtils.identityAssignments(this.outputVariables));
            return projectBuilder.build();
        }
    }

    /**
     * This is a helper method for project operations where variable references are reassigned.
     * It uses equivalence classes to facilitate the reassignment. For example, if a key
     * is normalized to equivalence class head X with equivalence class member Y and there is a reassignment
     * of Y to YY then the variable X will be reassigned to YY assuming there is no direct
     * reassignment of X to another variable reference. Useful equivalent mappings are
     * determined lazily and cached.
     */
    public static class InverseVariableMappingsWithEquivalence
    {
        private final EquivalenceClassProperty equivalenceClassProperty;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> inverseMappings;

        InverseVariableMappingsWithEquivalence(EquivalenceClassProperty equivalenceClassProperty,
                Map<VariableReferenceExpression, VariableReferenceExpression> inverseMappings)
        {
            requireNonNull(equivalenceClassProperty, "equivalenceClassProperty is null");
            requireNonNull(inverseMappings, "inverseMappings is null");
            this.equivalenceClassProperty = equivalenceClassProperty;
            this.inverseMappings = inverseMappings;
        }

        private boolean containsKey(VariableReferenceExpression variable)
        {
            if (!inverseMappings.containsKey(variable)) {
                //try to find a reverse mapping of an equivalent variable, update mappings
                RowExpression head = equivalenceClassProperty.getEquivalenceClassHead(variable);
                List<RowExpression> equivalentVariables = new ArrayList<>();
                equivalentVariables.add(head);
                equivalentVariables.addAll(equivalenceClassProperty.getEquivalenceClasses(head));
                for (RowExpression e : equivalentVariables) {
                    if (e instanceof VariableReferenceExpression &&
                            inverseMappings.containsKey(e)) {
                        inverseMappings.put(variable, inverseMappings.get(e));
                        break;
                    }
                }
            }
            return inverseMappings.containsKey(variable);
        }

        /**
         * Returns a direct or equivalent mapping of the provided variable reference.
         */
        public Optional<VariableReferenceExpression> get(VariableReferenceExpression variable)
        {
            requireNonNull(variable, "variable is null");
            if (containsKey(variable)) {
                return Optional.of(inverseMappings.get(variable));
            }
            else {
                return Optional.empty();
            }
        }
    }
}
