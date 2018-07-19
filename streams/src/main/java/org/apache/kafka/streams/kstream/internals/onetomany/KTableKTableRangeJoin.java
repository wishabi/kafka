package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableRangeValueGetter;
import org.apache.kafka.streams.kstream.internals.KTableRangeValueGetterSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;


public class KTableKTableRangeJoin<KL, KR, VL, VR, V> implements ProcessorSupplier<KL, Change<VL>> {

	private ValueJoiner<VL, VR, V> joiner;
	private KTableRangeValueGetterSupplier<CombinedKey<KL,KR>,VR> right;
	private final StateStore ref;

    //Performs Left-driven updates (ie: new One, updates the Many).
    public KTableKTableRangeJoin(KTableRangeValueGetterSupplier<CombinedKey<KL,KR>,VR> right,
                                 ValueJoiner<VL, VR, V> joiner,
                                 StateStore ref){

    	this.right = right;
        this.joiner = joiner;
        this.ref = ref;
    }

	@Override
    public Processor<KL, Change<VL>> get() {
        return new KTableKTableJoinProcessor(right);
    }
	

    private class KTableKTableJoinProcessor extends AbstractProcessor<KL, Change<VL>> {

		private KTableRangeValueGetter<CombinedKey<KL,KR>,VR> rightValueGetter;

        public KTableKTableJoinProcessor(KTableRangeValueGetterSupplier<CombinedKey<KL,KR>,VR> right) {
            this.rightValueGetter = right.get();
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            rightValueGetter.init(context);
        }

        /**
         * @throws StreamsException if key is null
         */
        @Override
        public void process(KL key, Change<VL> leftChange) {
            // the keys should never be null
            if (key == null)
                throw new StreamsException("Record key for KTable join operator should not be null.");

            //Wrap it in a combinedKey and let the serializer handle the prefixing.
            CombinedKey<KL,KR> prefixKey = new CombinedKey<>(key);

            //Flush the foreign state store, as we need all elements to be flushed for a proper range scan.
            ref.flush();
            final KeyValueIterator<CombinedKey<KL,KR>,VR> rightValues = rightValueGetter.prefixScan(prefixKey);

            while(rightValues.hasNext()){
                  KeyValue<CombinedKey<KL,KR>, VR> rightKeyValue = rightValues.next();
                  KR realKey = rightKeyValue.key.getRightKey();
                  VR value2 = rightKeyValue.value;
                  V newValue = null;
  				  V oldValue = null;

                  if (leftChange.oldValue != null) {
                	  oldValue = joiner.apply(leftChange.oldValue, value2);
                  }
                  
                  if (leftChange.newValue != null){
                      newValue = joiner.apply(leftChange.newValue, value2);
                  }

                  //TODO - Propagate a PrintableWrapper from here too - don't care about the offset. Everything needs to be printable.
                  //This is just to allow for easy sinking to the same topic as
                  //TODO - Probably rework this so that it's a different wrapper. We don't need the printable part anymore...
                  //Using -1 because we will not have race conditions from this side of the join to disambiguate with source offset.
                  PropagationWrapper<V> newWrappedVal = new PropagationWrapper<>(newValue, true, -1);
                  PropagationWrapper<V> oldWrappedVal = new PropagationWrapper<>(oldValue, true, -1);
                  context().forward(realKey, new Change<>(newWrappedVal, oldWrappedVal));
            }
        }
    }
}
