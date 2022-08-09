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

import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static org.glassfish.jersey.internal.util.collection.ImmutableCollectors.toImmutableSet;

/**
 * Represents a collection of primary or unique key constraints that hold for a final or
 * intermediate result set produced by a PlanNode.
 */
public class KeyProperty
{
    private final Set<Key> keys;

    public KeyProperty()
    {
        this.keys = ImmutableSet.of();
    }

    public KeyProperty(Set<Key> keys)
    {
        this.keys = ImmutableSet.copyOf(requireNonNull(keys, "keys is null"));
    }

    public Set<Key> getKeys()
    {
        return ImmutableSet.copyOf(keys);
    }

    /**
     * Determines if one key property is more general than another.
     * A key property is more general than another if it can satisfy any key requirement the other can satisfy.
     *
     * @param otherKeyProperty
     * @return True keyProperty is more general than otherKeyProperty or False otherwise.
     */
    public boolean moreGeneral(KeyProperty otherKeyProperty)
    {
        requireNonNull(otherKeyProperty, "otherKeyProperty is null");
        return ((keys.isEmpty() && otherKeyProperty.keys.isEmpty()) ||
                (otherKeyProperty.keys.stream().allMatch(k -> satisfiesKeyRequirement(k))));
    }

    /**
     * Determines if this key property satisfies a key requirement.
     * This is true if any of the keys in the collection satisfies the key requirement.
     *
     * @param keyRequirement
     * @return True if keyRequirement is satisfied by this key property or False otherwise.
     */
    public boolean satisfiesKeyRequirement(Key keyRequirement)
    {
        requireNonNull(keyRequirement, "keyRequirement is null");
        return keys.stream().anyMatch(k -> k.keySatisifiesRequirement(keyRequirement));
    }

    /**
     * Reduces key property to a concise cannonical form wherein each individual key is
     * reduced to a canonical form by removing redundant variables and replacing any remaining variables
     * with their equivalence class heads. Moreover, no keys in the normalized key
     * property are redundant with respect to the others.
     * Note that if any key is fully bound to constants an empty result is
     * returned, signaling that at most a single record is in the result set constrained
     * by this key property.
     *
     * @param equivalenceClassProperty
     * @return A normalized version of this key property or empty if any key is fully bound to constants.
     */
    public Optional<KeyProperty> normalize(EquivalenceClassProperty equivalenceClassProperty)
    {
        requireNonNull(equivalenceClassProperty, "equivalenceClassProperty is null");
        Set<Key> nonRedundantKeys = removeRedundantKeys(keys);
        ImmutableSet.Builder<Key> normalizedKeys = ImmutableSet.<Key>builder();
        for (Key key : nonRedundantKeys) {
            Optional<Key> normalizedKey = key.normalize(equivalenceClassProperty);
            if (!normalizedKey.isPresent()) {
                return Optional.empty();
            }
            else {
                normalizedKeys.add(normalizedKey.get());
            }
        }
        return Optional.of(new KeyProperty(normalizedKeys.build()));
    }

    /**
     * Takes a set of keys and returns a set with redundant keys removed
     * E.g. if {orderkey} is in the set, then the key {orderkey, orderpriority}
     * would represent a redundant key.
     *
     * @param keys
     */
    private static Set<Key> removeRedundantKeys(Set<Key> keys)
    {
        return keys.stream()
                .filter(key -> isRedundant(key, keys))
                .collect(toImmutableSet());
    }

    public static boolean isRedundant(Key keyToCheck, Set<Key> keys)
    {
        return keys.stream().anyMatch(keyToCompare ->
                !keyToCheck.equals(keyToCompare) && keyToCompare.keySatisifiesRequirement(keyToCheck));
    }

    /**
     * Returns a projected version of this key property.
     * Variables in each key are mapped to output variables in the context beyond the project operation.
     * It is possible that this operation projects all keys from the key property.
     *
     * @param inverseVariableMappings
     * @return A projected version of this key property.
     */
    public KeyProperty project(LogicalPropertiesImpl.InverseVariableMappingsWithEquivalence inverseVariableMappings)
    {
        requireNonNull(inverseVariableMappings, "inverseVariableMappings is null");
        Set<Key> keys = this.keys.stream().map(key -> key.project(inverseVariableMappings))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableSet());

        return new KeyProperty(keys);
    }

    /**
     * Empties all keys from the key property.
     */
    public boolean isEmpty()
    {
        return keys.isEmpty();
    }

    /**
     * Returns a version of this key property wherein each key is concatenated with all keys in the provided key property
     * A concatenated key property results from a join operation where concatenated keys of the left and
     * right join inputs form unique constraints on the join result.
     *
     * @param toConcatKeyProp
     * @return a version of this key concatenated with the provided key.
     */
    public KeyProperty concat(KeyProperty toConcatKeyProp)
    {
        requireNonNull(toConcatKeyProp, "toConcatKeyProp is null");
        ImmutableSet.Builder<Key> result = ImmutableSet.builder();
        for (Key thisKey : this.keys) {
            for (Key toConcatKey : toConcatKeyProp.keys) {
                result.add(thisKey.concat(toConcatKey));
            }
        }
        return new KeyProperty(result.build());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("keys", String.join(",", keys.stream().map(Key::toString).collect(Collectors.toList())))
                .toString();
    }
}
