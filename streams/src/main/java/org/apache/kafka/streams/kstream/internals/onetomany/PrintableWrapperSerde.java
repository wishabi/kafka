package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Factory for creating CombinedKey serializers / deserializers.
 */
public class PrintableWrapperSerde<V> implements Serde<PrintableWrapper<V>> {
    final private Serializer<PrintableWrapper<V>> serializer;
    final private Deserializer<PrintableWrapper<V>> deserializer;

    public PrintableWrapperSerde(Serde<V> serde) {
        this.serializer = new PrintableWrapperSerializer<>(serde.serializer());
        this.deserializer = new PrintableWrapperDeserializer<>(serde.deserializer());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs,isKey);
        deserializer.configure(configs,isKey);
    }

    @Override
    public void close() {
        deserializer.close();
        serializer.close();
    }

    @Override
    public Serializer<PrintableWrapper<V>> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<PrintableWrapper<V>> deserializer() {
        return deserializer;
    }

}
