package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.common.serialization.Serializer;

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
        //TODO - Bellemare - Do I need to do this? I am passing in already configured serializers...
//        this.leftSerializer.configure(configs, isKey);
//        this.rightSerializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, CombinedKey<KL, KR> data) {
        //{leftKeySerialized}{4-byte right length}{rightKeySerialized}{4-byte left length}
        // Awkward placement due to prefix scanning requirements
        byte[] leftSerializedData = leftSerializer.serialize("dummyTopic", data.getLeftKey());
        byte[] leftSize = numToBytes(leftSerializedData.length);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            output.write(leftSerializedData);
            if (data.getRightKey() != null) {
                byte[] rightSerializedData = rightSerializer.serialize("dummyTopic", data.getRightKey());
                byte[] rightSize = numToBytes(rightSerializedData.length);
                output.write(rightSize);
                output.write(rightSerializedData);
            }
            output.write(leftSize);
        } catch (IOException e){
            //TODO - Handle the IO exception;
            System.out.println("ERROR SHOULD NOT BE HERE IN IOEXCEPTION");
        }

        return output.toByteArray();
    }

    private byte[] numToBytes(int num){
        ByteBuffer wrapped = ByteBuffer.allocate(4);
        wrapped.putInt(num);
        return wrapped.array();
        //return new byte[]{ (byte)(num >>> 24),(byte)(num >>> 16),(byte)(num >>> 8),(byte)num };
    }

    @Override
    public void close() {
        //TODO - Bellemare - Do I need to do this? I am passing in already configured serializers...
        this.leftSerializer.close();
        this.rightSerializer.close();
    }


}
