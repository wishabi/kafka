package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

class CombinedKeyDeserializer<KL,KR> implements Deserializer<CombinedKey<KL,KR>> {

    private final Deserializer<KL> leftDeserializer;
    private final Deserializer<KR> rightDeserializer;


    public CombinedKeyDeserializer(Deserializer<KL> leftDeserializer, Deserializer<KR> rightDeserializer) {
        this.leftDeserializer = leftDeserializer;
        this.rightDeserializer = rightDeserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //TODO - Bellemare - do I need to configure them or are they already done?
    }

    @Override
    public CombinedKey<KL, KR> deserialize(String topic, byte[] data) {
        //{leftKeySerialized}{4-byte right length}{rightKeySerialized}{4-byte left length}

        byte[] leftCount = Arrays.copyOfRange(data,0,4);
        int leftKeyLength = fourBytesToInt(leftCount);
        //System.out.println("leftKeyLength = " + leftKeyLength);

        byte[] leftKeyRaw = Arrays.copyOfRange(data,4,4+leftKeyLength);
        //System.out.println("leftKeyRaw = " + new String(leftKeyRaw));

        KL leftKey = leftDeserializer.deserialize(topic, leftKeyRaw);

        if (data.length == 4 + leftKeyLength) {
            return new CombinedKey<>(leftKey);
        } else {

            byte[] rightCount = Arrays.copyOfRange(data, 4+leftKeyLength, 4+leftKeyLength + 4);
            int rightKeyLength = fourBytesToInt(rightCount);
//            System.out.println("rightKeyLength = " + rightKeyLength);

            byte[] rightKeyRaw = Arrays.copyOfRange(data, 4+leftKeyLength + 4, 4+leftKeyLength + 4 + rightKeyLength);
//            System.out.println("rightKeyRaw = " + new String(rightKeyRaw));
            KR rightKey = rightDeserializer.deserialize(topic, rightKeyRaw);
            return new CombinedKey<>(leftKey, rightKey);
        }
    }

    @Override
    public void close() {
        leftDeserializer.close();
        rightDeserializer.close();
    }

    private int fourBytesToInt(byte[] fourBytes){
        if (fourBytes.length != 4) {
            throw new ArrayIndexOutOfBoundsException("Expected 4 bytes when deserializing the CombinedKey! Found " + fourBytes.length);
        }
        ByteBuffer wrapped = ByteBuffer.wrap(fourBytes);
        return wrapped.getInt();
    }

}
