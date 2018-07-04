package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class PartialKeyPartitioner<KL,KR,V> implements StreamPartitioner<CombinedKey<KL,KR>, V> {

	private Serializer<KL> keySerializer;
	private String topic;

	public PartialKeyPartitioner(CombinedKeySerde<KL,KR> keySerde, String topic){
		this.keySerializer = keySerde.getLeftSerializer();
		this.topic = topic;
	}

//	@Override
//	public Integer partition(K key, V value, int numPartitions) {
//		/**
//		 * maybe allow user to supply inner Streampartitioner?
//		 * only works if left side is murmurpartitioned in this case
//		 */
//		return Utils.toPositive(Utils.murmur2(keySerializer.serialize(topic, extractor.apply(key)))) % numPartitions;
//	}

	@Override
	public Integer partition(CombinedKey<KL, KR> key, V value, int numPartitions) {
		return Utils.toPositive(Utils.murmur2(keySerializer.serialize(topic, key.getLeftKey()))) % numPartitions;
	}
}