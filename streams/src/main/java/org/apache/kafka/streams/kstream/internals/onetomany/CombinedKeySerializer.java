package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.common.serialization.Serializer;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

class CombinedKeySerializer<KL,KR> implements Serializer<CombinedKey<KL,KR>> {

    private final Serializer<KL> leftSerializer;
    private final Serializer<KR> rightSerializer;

    public CombinedKeySerializer(Serializer<KL> leftSerializer, Serializer<KR> rightSerializer) {
        this.leftSerializer = leftSerializer;
        this.rightSerializer = rightSerializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //Don't need to configure, they are already configured. This is just a wrapper.
    }

    @Override
    public byte[] serialize(String topic, CombinedKey<KL, KR> data) {
        byte[] leftSerializedData = leftSerializer.serialize(topic, data.getLeftKey());
        byte[] leftSize = numToBytes(leftSerializedData.length);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            output.write(leftSize);
            output.write(leftSerializedData);
            if (data.getRightKey() != null) {
                byte[] rightSerializedData = rightSerializer.serialize(topic, data.getRightKey());
                byte[] rightSize = numToBytes(rightSerializedData.length);
                output.write(rightSize);
                output.write(rightSerializedData);
            }
        } catch (IOException e){
            //TODO - Bellemare - yech. Handle the IO exception without passing it up.. ha.
            //System.out.println("IOException while handling serialization of CombinedKey " + e.toString());
        }

        return output.toByteArray();
    }

    private byte[] numToBytes(int num){
        ByteBuffer wrapped = ByteBuffer.allocate(4);
        wrapped.putInt(num);
        return wrapped.array();
    }

    @Override
    public void close() {
        this.leftSerializer.close();
        this.rightSerializer.close();
    }
}
