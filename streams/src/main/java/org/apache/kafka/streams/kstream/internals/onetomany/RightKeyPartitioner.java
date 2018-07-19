package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class RightKeyPartitioner<KR,V> implements StreamPartitioner<KR, V> {

	private final Serializer<KR> keySerializer;
	private final String topic;

	public RightKeyPartitioner(Serde<KR> keySerde, String topic){
		this.keySerializer = keySerde.serializer();
		this.topic = topic;
	}

	@Override
	public Integer partition(KR key, V value, int numPartitions) {
		byte[] data = keySerializer.serialize(topic, key);
		int partition = Utils.toPositive(Utils.murmur2(data)) % numPartitions;
		return partition;
	}
}