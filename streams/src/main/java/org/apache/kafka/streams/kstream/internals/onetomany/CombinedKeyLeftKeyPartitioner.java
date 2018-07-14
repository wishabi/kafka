package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class CombinedKeyLeftKeyPartitioner<KL,KR,V> implements StreamPartitioner<CombinedKey<KL,KR>, V> {

	private Serializer<KL> keySerializer;
	private String topic;

	public CombinedKeyLeftKeyPartitioner(CombinedKeySerde<KL,KR> keySerde, String topic){
		this.keySerializer = keySerde.getLeftSerializer();
		this.topic = topic;
	}

	@Override
	public Integer partition(CombinedKey<KL, KR> key, V value, int numPartitions) {
		byte[] data = keySerializer.serialize(topic, key.getLeftKey());
		int partition = Utils.toPositive(Utils.murmur2(data)) % numPartitions;
		return partition;
	}
}