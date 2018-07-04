package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableRangeValueGetterSupplier;
import org.apache.kafka.streams.kstream.internals.KTableSourceValueGetterSupplier;
import org.apache.kafka.streams.kstream.internals.KTableValueGetter;
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class NonRangeKeyValueGetterProviderAndProcessorSupplier<KL,KR, VL, VR, V> implements ProcessorSupplier<CombinedKey<KL,KR>, VR>
{

    private final String topicName;
    private final KTableValueGetterSupplier<KL, VL> leftValueGetterSupplier;
    private final ValueJoiner<VL, VR, V> joiner;

    public NonRangeKeyValueGetterProviderAndProcessorSupplier(String topicName,
                                                              KTableValueGetterSupplier<KL, VL> leftValueGetter ,
                                                              ValueJoiner<VL, VR, V> joiner)
    {
        this.topicName = topicName;
        this.joiner = joiner;
	    this.leftValueGetterSupplier = leftValueGetter;
    }


    @Override
    public Processor<CombinedKey<KL,KR>, VR> get()
    {

        return new AbstractProcessor<CombinedKey<KL,KR>, VR>()
        {

            KeyValueStore<CombinedKey<KL,KR>, VR> store;
            KTableValueGetter<KL, VL> leftValues;

            @Override
            public void init(ProcessorContext context)
            {
                super.init(context);
                leftValues = leftValueGetterSupplier.get();
                leftValues.init(context);
                store = (KeyValueStore<CombinedKey<KL,KR>, VR>) context.getStateStore(topicName);
            }

            @Override
            public void process(CombinedKey<KL,KR> key, VR value)
            {
                VR oldVal = store.get(key);
                store.put(key, value);

                V newValue = null;
                V oldValue = null;
                VL value2 = null;

                if (value != null || oldVal != null) {
                    KL d = key.getLeftKey();
                    value2 = leftValues.get(d);
                }

                if (value != null && value2 != null)
                    newValue = joiner.apply(value2, value);

                if (oldVal != null && value2 != null)
                    oldValue = joiner.apply(value2, oldVal);

                //TODO - Bellemare - Am I using the right generic types in this class?
                if(oldValue != null || newValue != null) {
                    KR realKey = key.getRightKey();
                    context().forward(realKey, new Change<>(newValue, oldValue));
                }
            }
        };
    }


    public KTableRangeValueGetterSupplier<CombinedKey<KL,KR>,VR> valueGetterSupplier() {
    	return new KTableSourceValueGetterSupplier<>(topicName);
    }
}
