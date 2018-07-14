/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.internals.onetomany.CombinedKey;
import org.apache.kafka.streams.kstream.internals.onetomany.CombinedKeySerde;
import org.apache.kafka.streams.kstream.internals.onetomany.KTableKTableRangeJoin;
import org.apache.kafka.streams.kstream.internals.onetomany.KTableRepartitionerProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.onetomany.CombinedKeyLeftKeyPartitioner;
import org.apache.kafka.streams.kstream.internals.onetomany.RepartitionedRightKeyValueGetterProviderAndProcessorSupplier;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.RocksDbKeyValueBytesStoreSupplier;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * The implementation class of {@link KTable}.
 *
 * @param <K> the key type
 * @param <S> the source's (parent's) value type
 * @param <V> the value type
 */
public class KTableImpl<K, S, V> extends AbstractStream<K> implements KTable<K, V> {

    // TODO: these two fields can be package-private after KStreamBuilder is removed
    public static final String SOURCE_NAME = "KTABLE-SOURCE-";

    public static final String STATE_STORE_NAME = "STATE-STORE-";

    private static final String FILTER_NAME = "KTABLE-FILTER-";

    private static final String FOREACH_NAME = "KTABLE-FOREACH-";

    private static final String JOINTHIS_NAME = "KTABLE-JOINTHIS-";

    private static final String JOINOTHER_NAME = "KTABLE-JOINOTHER-";

    private static final String MAPVALUES_NAME = "KTABLE-MAPVALUES-";

    private static final String MERGE_NAME = "KTABLE-MERGE-";

    private static final String PRINTING_NAME = "KSTREAM-PRINTER-";

    private static final String SELECT_NAME = "KTABLE-SELECT-";

    private static final String TOSTREAM_NAME = "KTABLE-TOSTREAM-";

    public static final String BY_RANGE = "KTABLE-JOIN-BYRANGE-";

    private static final String REPARTITION_NAME = "KTABLE-REPARTITION-";

    private final ProcessorSupplier<?, ?> processorSupplier;

    private final KeyValueMapper<K, V, String> defaultKeyValueMapper;

    private final String queryableStoreName;
    private final boolean isQueryable;

    private boolean sendOldValues = false;
    private final Serde<K> keySerde;
    private final Serde<V> valSerde;

    public KTableImpl(final InternalStreamsBuilder builder,
                      final String name,
                      final ProcessorSupplier<?, ?> processorSupplier,
                      final Set<String> sourceNodes,
                      final String queryableStoreName,
                      final boolean isQueryable) {
        super(builder, name, sourceNodes);
        this.processorSupplier = processorSupplier;
        this.queryableStoreName = queryableStoreName;
        this.keySerde = null;
        this.valSerde = null;
        this.isQueryable = isQueryable;
        this.defaultKeyValueMapper = new KeyValueMapper<K, V, String>() {
            @Override
            public String apply(K key, V value) {
                return String.format("%s, %s", key, value);
            }
        };
    }

    public KTableImpl(final InternalStreamsBuilder builder,
                      final String name,
                      final ProcessorSupplier<?, ?> processorSupplier,
                      final Serde<K> keySerde,
                      final Serde<V> valSerde,
                      final Set<String> sourceNodes,
                      final String queryableStoreName,
                      final boolean isQueryable) {
        super(builder, name, sourceNodes);
        this.processorSupplier = processorSupplier;
        this.queryableStoreName = queryableStoreName;
        this.keySerde = keySerde;
        this.valSerde = valSerde;
        this.isQueryable = isQueryable;
        this.defaultKeyValueMapper = new KeyValueMapper<K, V, String>() {
            @Override
            public String apply(K key, V value) {
                return String.format("%s, %s", key, value);
            }
        };
    }

    @Override
    public String queryableStoreName() {
        if (!isQueryable) {
            return null;
        }
        return this.queryableStoreName;
    }

    @SuppressWarnings("deprecation")
    private KTable<K, V> doFilter(final Predicate<? super K, ? super V> predicate,
                                  final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier,
                                  final boolean isFilterNot) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        String name = builder.newProcessorName(FILTER_NAME);
        String internalStoreName = null;
        if (storeSupplier != null) {
            internalStoreName = storeSupplier.name();
        }
        KTableProcessorSupplier<K, V, V> processorSupplier = new KTableFilter<>(this, predicate, isFilterNot, internalStoreName);
        builder.internalTopologyBuilder.addProcessor(name, processorSupplier, this.name);
        if (storeSupplier != null) {
            builder.internalTopologyBuilder.addStateStore(storeSupplier, name);
            return new KTableImpl<>(builder, name, processorSupplier, this.keySerde, this.valSerde, sourceNodes, internalStoreName, true);
        } else {
            return new KTableImpl<>(builder, name, processorSupplier, sourceNodes, this.queryableStoreName, false);
        }
    }

    private KTable<K, V> doFilter(final Predicate<? super K, ? super V> predicate,
                                  final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materialized,
                                  final boolean filterNot) {
        String name = builder.newProcessorName(FILTER_NAME);

        KTableProcessorSupplier<K, V, V> processorSupplier = new KTableFilter<>(this,
                                                                                predicate,
                                                                                filterNot,
                                                                                materialized.storeName());
        builder.internalTopologyBuilder.addProcessor(name, processorSupplier, this.name);

        final StoreBuilder builder = new KeyValueStoreMaterializer<>(materialized).materialize();
        this.builder.internalTopologyBuilder.addStateStore(builder, name);

        return new KTableImpl<>(this.builder,
                                name,
                                processorSupplier,
                                this.keySerde,
                                this.valSerde,
                                sourceNodes,
                                builder.name(),
                                true);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate) {
        return filter(predicate, (String) null);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate,
                               final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        return doFilter(predicate, new MaterializedInternal<>(materialized, builder, FILTER_NAME), false);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate,
                               final String queryableStoreName) {
        org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier = null;
        if (queryableStoreName != null) {
            storeSupplier = keyValueStore(this.keySerde, this.valSerde, queryableStoreName);
        }
        return doFilter(predicate, storeSupplier, false);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate,
                               final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doFilter(predicate, storeSupplier, false);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate) {
        return filterNot(predicate, (String) null);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
                                  final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        return doFilter(predicate, new MaterializedInternal<>(materialized, builder, FILTER_NAME), true);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
                                  final String queryableStoreName) {
        org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier = null;
        if (queryableStoreName != null) {
            storeSupplier = keyValueStore(this.keySerde, this.valSerde, queryableStoreName);
        }
        return doFilter(predicate, storeSupplier, true);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
                                  final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doFilter(predicate, storeSupplier, true);
    }

    @SuppressWarnings("deprecation")
    private <V1> KTable<K, V1> doMapValues(final ValueMapper<? super V, ? extends V1> mapper,
                                           final Serde<V1> valueSerde,
                                           final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(mapper);
        String name = builder.newProcessorName(MAPVALUES_NAME);
        String internalStoreName = null;
        if (storeSupplier != null) {
            internalStoreName = storeSupplier.name();
        }
        KTableProcessorSupplier<K, V, V1> processorSupplier = new KTableMapValues<>(this, mapper, internalStoreName);
        builder.internalTopologyBuilder.addProcessor(name, processorSupplier, this.name);
        if (storeSupplier != null) {
            builder.internalTopologyBuilder.addStateStore(storeSupplier, name);
            return new KTableImpl<>(builder, name, processorSupplier, this.keySerde, valueSerde, sourceNodes, internalStoreName, true);
        } else {
            return new KTableImpl<>(builder, name, processorSupplier, sourceNodes, this.queryableStoreName, false);
        }
    }

    @Override
    public <V1> KTable<K, V1> mapValues(final ValueMapper<? super V, ? extends V1> mapper) {
        return mapValues(mapper, null, (String) null);
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper,
                                        final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal
                = new MaterializedInternal<>(materialized, builder, MAPVALUES_NAME);
        final String name = builder.newProcessorName(MAPVALUES_NAME);
        final KTableProcessorSupplier<K, V, VR> processorSupplier = new KTableMapValues<>(this,
                                                                                          mapper,
                                                                                          materializedInternal.storeName());
        builder.internalTopologyBuilder.addProcessor(name, processorSupplier, this.name);
        builder.internalTopologyBuilder.addStateStore(new KeyValueStoreMaterializer<>(materializedInternal)
                                                              .materialize(),
                                                      name);
        return new KTableImpl<>(builder, name, processorSupplier, sourceNodes, this.queryableStoreName, true);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <V1> KTable<K, V1> mapValues(final ValueMapper<? super V, ? extends V1> mapper,
                                        final Serde<V1> valueSerde,
                                        final String queryableStoreName) {
        org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier = null;
        if (queryableStoreName != null) {
            storeSupplier = keyValueStore(this.keySerde, valueSerde, queryableStoreName);
        }
        return doMapValues(mapper, valueSerde, storeSupplier);
    }

    @SuppressWarnings("deprecation")
    @Override
    public  <V1> KTable<K, V1> mapValues(final ValueMapper<? super V, ? extends V1> mapper,
                                         final Serde<V1> valueSerde,
                                         final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doMapValues(mapper, valueSerde, storeSupplier);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void print() {
        print(null, null, this.name);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void print(final String label) {
        print(null, null, label);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void print(final Serde<K> keySerde,
                      final Serde<V> valSerde) {
        print(keySerde, valSerde, this.name);
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    @Override
    public void print(final Serde<K> keySerde,
                      final Serde<V> valSerde,
                      final String label) {
        Objects.requireNonNull(label, "label can't be null");
        final String name = builder.newProcessorName(PRINTING_NAME);
        builder.internalTopologyBuilder.addProcessor(name, new KStreamPrint<>(new PrintForeachAction(null, defaultKeyValueMapper, label)), this.name);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void writeAsText(final String filePath) {
        writeAsText(filePath, this.name, null, null);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void writeAsText(final String filePath,
                            final String label) {
        writeAsText(filePath, label, null, null);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void writeAsText(final String filePath,
                            final Serde<K> keySerde,
                            final Serde<V> valSerde) {
        writeAsText(filePath, this.name, keySerde, valSerde);
    }

    /**
     * @throws TopologyException if file is not found
     */
    @SuppressWarnings({"unchecked", "deprecation"})
    @Override
    public void writeAsText(final String filePath,
                            final String label,
                            final Serde<K> keySerde,
                            final Serde<V> valSerde) {
        Objects.requireNonNull(filePath, "filePath can't be null");
        Objects.requireNonNull(label, "label can't be null");
        if (filePath.trim().isEmpty()) {
            throw new TopologyException("filePath can't be an empty string");
        }
        String name = builder.newProcessorName(PRINTING_NAME);
        try {
            PrintWriter printWriter = new PrintWriter(filePath, StandardCharsets.UTF_8.name());
            builder.internalTopologyBuilder.addProcessor(name, new KStreamPrint<>(new PrintForeachAction(printWriter, defaultKeyValueMapper, label)), this.name);
        } catch (final FileNotFoundException | UnsupportedEncodingException e) {
            throw new TopologyException(String.format("Unable to write stream to file at [%s] %s", filePath, e.getMessage()));
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void foreach(final ForeachAction<? super K, ? super V> action) {
        Objects.requireNonNull(action, "action can't be null");
        String name = builder.newProcessorName(FOREACH_NAME);
        KStreamPeek<K, Change<V>> processorSupplier = new KStreamPeek<>(new ForeachAction<K, Change<V>>() {
            @Override
            public void apply(K key, Change<V> value) {
                action.apply(key, value.newValue);
            }
        }, false);
        builder.internalTopologyBuilder.addProcessor(name, processorSupplier, this.name);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KTable<K, V> through(final Serde<K> keySerde,
                                final Serde<V> valSerde,
                                final StreamPartitioner<? super K, ? super V> partitioner,
                                final String topic,
                                final String queryableStoreName) {
        to(keySerde, valSerde, partitioner, topic);

        return builder.table(topic,
                             new ConsumedInternal<>(keySerde, valSerde, new FailOnInvalidTimestamp(), null),
                             new MaterializedInternal<>(Materialized.<K, V, KeyValueStore<Bytes, byte[]>>with(keySerde, valSerde),
                                     builder,
                                     KTableImpl.TOSTREAM_NAME));
    }

    @SuppressWarnings("deprecation")
    @Override
    public KTable<K, V> through(final Serde<K> keySerde,
                                final Serde<V> valSerde,
                                final StreamPartitioner<? super K, ? super V> partitioner,
                                final String topic,
                                final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        to(keySerde, valSerde, partitioner, topic);

        final ConsumedInternal<K, V> consumed = new ConsumedInternal<>(Consumed.with(keySerde, valSerde, new FailOnInvalidTimestamp(), null));
        return builder.table(topic, consumed, storeSupplier);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KTable<K, V> through(final Serde<K> keySerde,
                                final Serde<V> valSerde,
                                final StreamPartitioner<? super K, ? super V> partitioner,
                                final String topic) {
        return through(keySerde, valSerde, partitioner, topic, (String) null);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KTable<K, V> through(final Serde<K> keySerde,
                                final Serde<V> valSerde,
                                final String topic,
                                final String queryableStoreName) {
        return through(keySerde, valSerde, null, topic, queryableStoreName);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KTable<K, V> through(final Serde<K> keySerde,
                                final Serde<V> valSerde,
                                final String topic,
                                final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return through(keySerde, valSerde, null, topic, storeSupplier);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KTable<K, V> through(final Serde<K> keySerde,
                                final Serde<V> valSerde,
                                final String topic) {
        return through(keySerde, valSerde, null, topic, (String) null);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KTable<K, V> through(final StreamPartitioner<? super K, ? super V> partitioner,
                                final String topic,
                                final String queryableStoreName) {
        return through(null, null, partitioner, topic, queryableStoreName);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KTable<K, V> through(final StreamPartitioner<? super K, ? super V> partitioner,
                                final String topic,
                                final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return through(null, null, partitioner, topic, storeSupplier);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KTable<K, V> through(final StreamPartitioner<? super K, ? super V> partitioner,
                                final String topic) {
        return through(null, null, partitioner, topic, (String) null);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KTable<K, V> through(final String topic,
                                final String queryableStoreName) {
        return through(null, null, null, topic, queryableStoreName);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KTable<K, V> through(final String topic,
                                final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return through(null, null, null, topic, storeSupplier);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KTable<K, V> through(final String topic) {
        return through(null, null, null, topic, (String) null);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void to(final String topic) {
        to(null, null, null, topic);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void to(final StreamPartitioner<? super K, ? super V> partitioner,
                   final String topic) {
        to(null, null, partitioner, topic);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void to(final Serde<K> keySerde,
                   final Serde<V> valSerde,
                   final String topic) {
        this.toStream().to(keySerde, valSerde, null, topic);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void to(final Serde<K> keySerde,
                   final Serde<V> valSerde,
                   final StreamPartitioner<? super K, ? super V> partitioner,
                   final String topic) {
        this.toStream().to(keySerde, valSerde, partitioner, topic);
    }

    @Override
    public KStream<K, V> toStream() {
        String name = builder.newProcessorName(TOSTREAM_NAME);

        builder.internalTopologyBuilder.addProcessor(name, new KStreamMapValues<K, Change<V>, V>(new ValueMapper<Change<V>, V>() {
            @Override
            public V apply(Change<V> change) {
                return change.newValue;
            }
        }), this.name);

        return new KStreamImpl<>(builder, name, sourceNodes, false);
    }

    @Override
    public <K1> KStream<K1, V> toStream(final KeyValueMapper<? super K, ? super V, ? extends K1> mapper) {
        return toStream().selectKey(mapper);
    }

    @Override
    public <V1, R> KTable<K, R> join(final KTable<K, V1> other,
                                     final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return doJoin(other, joiner, null, false, false);
    }

    @Override
    public <VO, VR> KTable<K, VR> join(final KTable<K, VO> other,
                                       final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                       final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        return doJoin(other, joiner, new MaterializedInternal<>(materialized, builder, MERGE_NAME), false, false);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <V1, R> KTable<K, R> join(final KTable<K, V1> other,
                                     final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                     final Serde<R> joinSerde,
                                     final String queryableStoreName) {
        return doJoin(other, joiner, false, false, joinSerde, queryableStoreName);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <V1, R> KTable<K, R> join(final KTable<K, V1> other,
                                     final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                     final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doJoin(other, joiner, false, false, storeSupplier);
    }

    @Override
    public <V1, R> KTable<K, R> outerJoin(final KTable<K, V1> other,
                                          final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return doJoin(other, joiner, null, true, true);
    }

    @Override
    public <VO, VR> KTable<K, VR> outerJoin(final KTable<K, VO> other,
                                            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return doJoin(other, joiner, new MaterializedInternal<>(materialized, builder, MERGE_NAME), true, true);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <V1, R> KTable<K, R> outerJoin(final KTable<K, V1> other,
                                          final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                          final Serde<R> joinSerde,
                                          final String queryableStoreName) {
        return doJoin(other, joiner, true, true, joinSerde, queryableStoreName);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <V1, R> KTable<K, R> outerJoin(final KTable<K, V1> other,
                                          final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                          final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doJoin(other, joiner, true, true, storeSupplier);
    }

    @Override
    public <V1, R> KTable<K, R> leftJoin(final KTable<K, V1> other,
                                         final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return doJoin(other, joiner, null, true, false);
    }

    @Override
    public <VO, VR> KTable<K, VR> leftJoin(final KTable<K, VO> other,
                                           final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                           final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return doJoin(other,
                      joiner,
                      new MaterializedInternal<>(materialized, builder, MERGE_NAME),
                      true,
                      false);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <V1, R> KTable<K, R> leftJoin(final KTable<K, V1> other,
                                         final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                         final Serde<R> joinSerde,
                                         final String queryableStoreName) {
        return doJoin(other, joiner, true, false, joinSerde, queryableStoreName);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <V1, R> KTable<K, R> leftJoin(final KTable<K, V1> other,
                                         final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                         final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doJoin(other, joiner, true, false, storeSupplier);
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    private <V1, R> KTable<K, R> doJoin(final KTable<K, V1> other,
                                        final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                        final boolean leftOuter,
                                        final boolean rightOuter,
                                        final Serde<R> joinSerde,
                                        final String queryableStoreName) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");

        final org.apache.kafka.streams.processor.StateStoreSupplier storeSupplier
            = queryableStoreName == null ? null : keyValueStore(this.keySerde, joinSerde, queryableStoreName);

        return doJoin(other, joiner, leftOuter, rightOuter, storeSupplier);
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    private <V1, R> KTable<K, R> doJoin(final KTable<K, V1> other,
                                        final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                        final boolean leftOuter,
                                        final boolean rightOuter,
                                        final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        final String joinMergeName = builder.newProcessorName(MERGE_NAME);
        final String internalQueryableName = storeSupplier == null ? null : storeSupplier.name();
        final KTable<K, R> result = buildJoin((AbstractStream<K>) other,
                                              joiner,
                                              leftOuter,
                                              rightOuter,
                                              joinMergeName,
                                              internalQueryableName);

        if (internalQueryableName != null) {
            builder.internalTopologyBuilder.addStateStore(storeSupplier, joinMergeName);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private <VO, VR> KTable<K, VR> doJoin(final KTable<K, VO> other,
                                          final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                          final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materialized,
                                          final boolean leftOuter,
                                          final boolean rightOuter) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        final String internalQueryableName = materialized == null ? null : materialized.storeName();
        final String joinMergeName = builder.newProcessorName(MERGE_NAME);
        final KTable<K, VR> result = buildJoin((AbstractStream<K>) other,
                                               joiner,
                                               leftOuter,
                                               rightOuter,
                                               joinMergeName,
                                               internalQueryableName);

        if (materialized != null) {
            final StoreBuilder<KeyValueStore<K, VR>> storeBuilder
                    = new KeyValueStoreMaterializer<>(materialized).materialize();
            builder.internalTopologyBuilder.addStateStore(storeBuilder, joinMergeName);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private <V1, R> KTable<K, R> buildJoin(final AbstractStream<K> other,
                                           final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                           final boolean leftOuter,
                                           final boolean rightOuter,
                                           final String joinMergeName,
                                           final String internalQueryableName) {
        final Set<String> allSourceNodes = ensureJoinableWith(other);

        if (leftOuter) {
            enableSendingOldValues();
        }
        if (rightOuter) {
            ((KTableImpl) other).enableSendingOldValues();
        }

        final String joinThisName = builder.newProcessorName(JOINTHIS_NAME);
        final String joinOtherName = builder.newProcessorName(JOINOTHER_NAME);


        final KTableKTableAbstractJoin<K, R, V, V1> joinThis;
        final KTableKTableAbstractJoin<K, R, V1, V> joinOther;

        if (!leftOuter) { // inner
            joinThis = new KTableKTableJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
            joinOther = new KTableKTableJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        } else if (!rightOuter) { // left
            joinThis = new KTableKTableLeftJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
            joinOther = new KTableKTableRightJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        } else { // outer
            joinThis = new KTableKTableOuterJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
            joinOther = new KTableKTableOuterJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        }

        final KTableKTableJoinMerger<K, R> joinMerge = new KTableKTableJoinMerger<>(
                new KTableImpl<K, V, R>(builder, joinThisName, joinThis, sourceNodes, this.queryableStoreName, false),
                new KTableImpl<K, V1, R>(builder, joinOtherName, joinOther, ((KTableImpl<K, ?, ?>) other).sourceNodes,
                        ((KTableImpl<K, ?, ?>) other).queryableStoreName, false),
                internalQueryableName
        );

        builder.internalTopologyBuilder.addProcessor(joinThisName, joinThis, this.name);
        builder.internalTopologyBuilder.addProcessor(joinOtherName, joinOther, ((KTableImpl) other).name);
        builder.internalTopologyBuilder.addProcessor(joinMergeName, joinMerge, joinThisName, joinOtherName);
        builder.internalTopologyBuilder.connectProcessorAndStateStores(joinThisName, ((KTableImpl) other).valueGetterSupplier().storeNames());
        builder.internalTopologyBuilder.connectProcessorAndStateStores(joinOtherName, valueGetterSupplier().storeNames());
        return new KTableImpl<>(builder, joinMergeName, joinMerge, allSourceNodes, internalQueryableName, internalQueryableName != null);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <K1, V1> KGroupedTable<K1, V1> groupBy(final KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> selector,
                                                  final Serde<K1> keySerde,
                                                  final Serde<V1> valueSerde) {
        return groupBy(selector, Serialized.with(keySerde, valueSerde));
    }

    @Override
    public <K1, V1> KGroupedTable<K1, V1> groupBy(final KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> selector) {
        return this.groupBy(selector, Serialized.<K1, V1>with(null, null));
    }

    @Override
    public <K1, V1> KGroupedTable<K1, V1> groupBy(final KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> selector,
                                                  final Serialized<K1, V1> serialized) {
        Objects.requireNonNull(selector, "selector can't be null");
        Objects.requireNonNull(serialized, "serialized can't be null");
        String selectName = builder.newProcessorName(SELECT_NAME);

        KTableProcessorSupplier<K, V, KeyValue<K1, V1>> selectSupplier = new KTableRepartitionMap<>(this, selector);

        // select the aggregate key and values (old and new), it would require parent to send old values
        builder.internalTopologyBuilder.addProcessor(selectName, selectSupplier, this.name);
        this.enableSendingOldValues();
        final SerializedInternal<K1, V1> serializedInternal  = new SerializedInternal<>(serialized);
        return new KGroupedTableImpl<>(builder,
                                       selectName,
                                       this.name,
                                       serializedInternal.keySerde(),
                                       serializedInternal.valueSerde());
    }

    @SuppressWarnings("unchecked")
    KTableValueGetterSupplier<K, V> valueGetterSupplier() {
        if (processorSupplier instanceof KTableSource) {
            KTableSource<K, V> source = (KTableSource<K, V>) processorSupplier;
            return new KTableSourceValueGetterSupplier<>(source.storeName);
        } else if (processorSupplier instanceof KStreamAggProcessorSupplier) {
            return ((KStreamAggProcessorSupplier<?, K, S, V>) processorSupplier).view();
        } else {
            return ((KTableProcessorSupplier<K, S, V>) processorSupplier).view();
        }
    }

    @SuppressWarnings("unchecked")
    void enableSendingOldValues() {
        if (!sendOldValues) {
            if (processorSupplier instanceof KTableSource) {
                KTableSource<K, ?> source = (KTableSource<K, V>) processorSupplier;
                source.enableSendingOldValues();
            } else if (processorSupplier instanceof KStreamAggProcessorSupplier) {
                ((KStreamAggProcessorSupplier<?, K, S, V>) processorSupplier).enableSendingOldValues();
            } else {
                ((KTableProcessorSupplier<K, S, V>) processorSupplier).enableSendingOldValues();
            }
            sendOldValues = true;
        }
    }

    boolean sendingOldValueEnabled() {
        return sendOldValues;
    }

    @Override
    public <V0, KL, VL, KR, VR> KTable<KR, V0> oneToManyJoin(KTable<KR, VR> other,
                                                            ValueMapper<VR, KL> keyExtractor,
                                                            final ValueJoiner<VL, VR, V0> joiner,
                                                            final Materialized<KR, V0, KeyValueStore<Bytes, byte[]>> materialized,
                                                            Serde<KL> thisKeySerde,
                                                            Serde<KR> otherKeySerde,
                                                            Serde<VR> otherValueSerde,
                                                            Serde<V0> joinedValueSerde) {

        return doOneToManyJoin(other, keyExtractor, joiner, new MaterializedInternal<>(materialized, builder, MERGE_NAME),
                 thisKeySerde, otherKeySerde, otherValueSerde, joinedValueSerde);
    }


    @SuppressWarnings("unchecked")
    private <V0, KL, VL, KR, VR> KTable<KR, V0> doOneToManyJoin(KTable<KR, VR> other,
                                                                ValueMapper<VR, KL> keyExtractor,
                                                                final ValueJoiner<VL, VR, V0> joiner,
                                                                final MaterializedInternal<KR, V0, KeyValueStore<Bytes, byte[]>> materialized,
                                                                Serde<KL> thisKeySerde,
                                                                Serde<KR> otherKeySerde,
                                                                Serde<VR> otherValueSerde,
                                                                Serde<V0> joinedValueSerde) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        final String internalQueryableName = materialized == null ? null : materialized.storeName();
        final String joinMergeName = builder.newProcessorName(MERGE_NAME);
        final KTable<KR, V0> result = buildOneToManyJoin(other,
                keyExtractor,
                joiner,
                joinMergeName,
                materialized,
                thisKeySerde,
                otherKeySerde,
                otherValueSerde,
                joinedValueSerde);
        return result;
    }



    //Currently, the left side of the join contains the one.
    //The right side contains the many.
    //Need to separate joiner. Currently expects [Result, V2, V1], but we want [V1,V2,Result]
    //thisValueSerde expects: Result, we want it to be FlyerTypes... right?
    private <V0, KL, VL, KR, VR> KTable<KR, V0> buildOneToManyJoin(KTable<KR, VR> other,
                                                               ValueMapper<VR, KL> keyExtractor,
                                                               final ValueJoiner<VL, VR, V0> joiner,
                                                               final String joinMergeName,
                                                               final MaterializedInternal<KR, V0, KeyValueStore<Bytes, byte[]>> materialized,
                                                               Serde<KL> thisKeySerde,
                                                               Serde<KR> otherKeySerde,
                                                               Serde<VR> otherValueSerde,
                                                               Serde<V0> joinedValueSerde) {
        ((KTableImpl<?, ?, ?>) other).enableSendingOldValues();

        //TODO - leftOuter == false?
        enableSendingOldValues();

        InternalTopologyBuilder topology = builder.internalTopologyBuilder;

        String repartitionerName = builder.newProcessorName(REPARTITION_NAME);
        final String repartitionTopicName = name + "-" + JOINOTHER_NAME;

        topology.addInternalTopic(repartitionTopicName);
        final String repartitionProcessorName = repartitionerName + "-" + SELECT_NAME;
        final String repartitionSourceName = repartitionerName + "-source";
        final String repartitionSinkName = repartitionerName + "-sink";
        final String joinOnThisTableName = repartitionerName + "-table";

        // repartition original => intermediate topic
        KTableRepartitionerProcessorSupplier<KL, KR, VR> repartitionProcessor =
                new KTableRepartitionerProcessorSupplier<>(keyExtractor);

        //Add the processor for rekeying the right key
        topology.addProcessor(repartitionProcessorName, repartitionProcessor, ((AbstractStream<?>) other).name);

        //Create a Serde for the Combined left and right key.
        CombinedKeySerde<KL, KR> combinedKeySerde = new CombinedKeySerde<>(thisKeySerde, otherKeySerde);

        //Create the partitioner that will just partition on the left key.
        CombinedKeyLeftKeyPartitioner<KL, KR, VR> partitioner = new CombinedKeyLeftKeyPartitioner<>(combinedKeySerde, repartitionTopicName);

        //Takes the results of the partitioner and sinks them to an internal topic, properly repartitioned according to the left foreign key.
        topology.addSink(repartitionSinkName, repartitionTopicName,
                combinedKeySerde.serializer(), otherValueSerde.serializer(),
                partitioner, repartitionProcessorName);

        //Re-read partitioned topic, copartitioned with the left table keys.
        topology.addSource(null, repartitionSourceName, new FailOnInvalidTimestamp(), combinedKeySerde.deserializer(), otherValueSerde.deserializer(), repartitionTopicName);

        //This is the right side's processor. It does two main things:
        // 1) Loads the data into a stateStore, to be accessed by the KTableKTableRangeJoin processor (the left side's processor).
        // 2) Drives the join logic from the right.
        //    Uses the left key from CombinedKey to access the left value from the left statestore.
        //    Applies the join logic.
        //    Returns the data keyed on the RightKey. Discards the CombinedKey as it is no longer needed after this stage.
        //TODO -  repartitionTopicName is used as the stateStoreName under the hood. Make it clearer?
        String joinByRangeName = builder.newProcessorName(BY_RANGE);
        final RepartitionedRightKeyValueGetterProviderAndProcessorSupplier<KL, KR, VL, VR, V> joinOnThisTable =
                new RepartitionedRightKeyValueGetterProviderAndProcessorSupplier(repartitionTopicName, ((KTableImpl<?, ?, ?>) this).valueGetterSupplier(), joiner);
        topology.addProcessor(joinOnThisTableName, joinOnThisTable, repartitionSourceName);

        // Create a state store.
        // The RepartitionedRightKeyValueGetterProviderAndProcessorSupplier accesses it via the repartitionTopicName handle.
        // The state store is named repartitionTopicName. It is populated by the right processor and read by the left processor.
        KeyValueBytesStoreSupplier rdbs = new RocksDbKeyValueBytesStoreSupplier(repartitionTopicName);
        Materialized mat = Materialized.<CombinedKey<KL, KR>, VR>as(rdbs)
                .withCachingDisabled()
                .withKeySerde(combinedKeySerde)
                .withValueSerde(otherValueSerde);
        MaterializedInternal<CombinedKey<KL, KR>, VR, KeyValueStore<Bytes, byte[]>> repartitionedRangeScannableStore =
                new MaterializedInternal<CombinedKey<KL, KR>, VR, KeyValueStore<Bytes, byte[]>>(mat, builder, "SOMEFOO");

        //This connects the right processor with the state store in the topology.
        topology.addStateStore(new KeyValueStoreMaterializer<>(repartitionedRangeScannableStore).materialize(), joinOnThisTableName);

        //Performs Left-driven updates (ie: new One, updates the Many).
        //Produces with the Real Key.
        KTableRangeValueGetterSupplier<CombinedKey<KL, KR>, VR> f = joinOnThisTable.valueGetterSupplier();
        KTableKTableRangeJoin<KL, KR, VL, VR, V0> joinByRange
                = new KTableKTableRangeJoin<>(f, joiner);
        //Add the left processor to the topology.
        topology.addProcessor(joinByRangeName, joinByRange, this.name);

        //Join the left and the right outputs together into a new table.
        KTableImpl<KR, V, V0> myThis = new KTableImpl<>(builder, joinOnThisTableName, joinOnThisTable, sourceNodes, this.queryableStoreName, false);
        KTableImpl<KR, V, V0> myThat = new KTableImpl<>(builder, joinByRangeName, joinByRange, ((KTableImpl<K, ?, ?>) other).sourceNodes,
                ((KTableImpl<K, ?, ?>) other).queryableStoreName, false);

        Materialized myMat = Materialized.<KR, V0, KeyValueStore<Bytes, byte[]>>with(otherKeySerde, joinedValueSerde);
        MaterializedInternal<KR, V0, KeyValueStore<Bytes, byte[]>> myUselessMaterializedStore = new MaterializedInternal(myMat, builder, "SOME_HANDLE_NAME");

        //TODO - Figure out how to avoid materializing this...
        final KTableKTableJoinMerger<KR, V0> joinMerge = new KTableKTableJoinMerger<>(
                myThis,
                myThat,
                myUselessMaterializedStore.storeName());

        //Add the join processor to the topology.
        topology.addProcessor(joinMergeName, joinMerge, joinOnThisTableName, joinByRangeName);
        //Connect the left processor to the to the left valueGetter (state store or predecessor processor)
        //Connect the right processor to the repartitionedRangeScannableStore.
        topology.connectProcessorAndStateStores(joinOnThisTableName, valueGetterSupplier().storeNames());
        topology.connectProcessorAndStateStores(joinByRangeName, repartitionedRangeScannableStore.storeSupplier().get().name());

        //Ensure that the repartitionedSource and the sourceNodes from this table are correctly co-partitioned.
        //If they are not, this will ensure that they are repartitioned into the size of the largest partition count,
        //and are allocated such that the same partition number (with the same keys) are on the same node.
        HashSet<String> sourcesNeedCopartitioning = new HashSet<>();
        sourcesNeedCopartitioning.add(repartitionSourceName);
        sourcesNeedCopartitioning.addAll(sourceNodes);
        topology.copartitionSources(sourcesNeedCopartitioning);

        final StoreBuilder<KeyValueStore<KR, V0>> storeBuilder
                = new KeyValueStoreMaterializer<>(myUselessMaterializedStore).materialize();
        builder.internalTopologyBuilder.addStateStore(storeBuilder, joinMergeName);

        String asdfTableName = builder.newProcessorName(SOURCE_NAME);
        KTable asdf = new KTableImpl<>(builder,
                asdfTableName,
                joinMerge,
                sourcesNeedCopartitioning,
                myUselessMaterializedStore.storeName(),
                myUselessMaterializedStore.storeName() != null);

        topology.addProcessor(asdfTableName, joinMerge, joinMergeName);
        topology.connectProcessorAndStateStores(asdfTableName, myUselessMaterializedStore.storeName());

        String outputRepartitionSinkName = builder.newProcessorName(REPARTITION_NAME);
        String outputRepartitionSinkTopicName = outputRepartitionSinkName + "-Topic";
        topology.addInternalTopic(outputRepartitionSinkTopicName);

        asdf
            .toStream()
            .to(outputRepartitionSinkTopicName, Produced.with(otherKeySerde, joinedValueSerde));

        return builder.table(outputRepartitionSinkTopicName,
            new ConsumedInternal<>(otherKeySerde, joinedValueSerde, new FailOnInvalidTimestamp(), null),
            materialized);
    }
}
