package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;

class PrintableWrapperDeserializer<V> implements Deserializer<PrintableWrapper<V>> {

    private final Deserializer<V> deserializer;

    public PrintableWrapperDeserializer(Deserializer<V> deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //Don't need to configure them, as they are already configured. This is only a wrapper.
    }

    @Override
    public PrintableWrapper<V> deserialize(String topic, byte[] data) {
        //{4-byte value length}{value}{7-bits encoded boolean length}{1-bit}
        byte[] count = Arrays.copyOfRange(data,0,4);
        int offset = ByteBuffer.wrap(count).getInt();
        byte[] rawVal = Arrays.copyOfRange(data,4,4+offset);
        V value = deserializer.deserialize(topic, rawVal);

        byte[] printableLengthRaw = Arrays.copyOfRange(data, 4+offset, 4+offset+1);

        BitSet bits = BitSet.valueOf(printableLengthRaw);
        boolean printable = bits.get(0);

        return new PrintableWrapper<>(value, printable);
    }

    @Override
    public void close() {
        deserializer.close();
    }

}
