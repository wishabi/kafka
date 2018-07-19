package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class KTableRepartitionerProcessorSupplier<KL, KR, VR> implements ProcessorSupplier<KR, Change<VR>> {

	private final ValueMapper<VR, KL> mapper;

	public KTableRepartitionerProcessorSupplier(ValueMapper<VR,KL> extractor) {
		this.mapper = extractor;
	}
	
	@Override
	public Processor<KR, Change<VR>> get() {
		return new UnbindChangeProcessor(); 
	}
	
	private class UnbindChangeProcessor extends AbstractProcessor<KR, Change<VR>>
	{

		@Override
		public void init(final ProcessorContext context) {
			super.init(context);
		}

		@Override
		public void process(KR key, Change<VR> change) {
			
			if(change.oldValue != null)
			{
				KL leftOldKey = mapper.apply(change.oldValue);
				CombinedKey<KL,KR> combinedOldKey = new CombinedKey<>(leftOldKey, key);
				if(change.newValue != null)
				{
					KL extractedNewLeftKey = mapper.apply(change.newValue);
					CombinedKey<KL, KR> combinedNewKey = new CombinedKey<>(extractedNewLeftKey, key);

					//TODO - Requires equal to be defined. If not defined, should still resolve to same in the else-statement.
					if(leftOldKey.equals(extractedNewLeftKey))
					{
					    //Same foreign key. Just propagate onwards.
						context().forward(combinedNewKey, new PropagationWrapper<>(change.newValue, true, context().offset()));
					}
					else  
					{
					    //Different Foreign Key - delete the old key value and propagate the new one.
                        //Note that we indicate that we don't want to propagate the delete to the join output. It is set to false.
                        //This will be used by a downstream processor to delete it from the local state store, but not propagate it
                        //as a full delete. This avoids a race condition in the resolution of the output.
						context().forward(combinedOldKey, new PropagationWrapper<>(change.newValue, false, context().offset()));
						context().forward(combinedNewKey, new PropagationWrapper<>(change.newValue, true, context().offset()));
					}
				}
				else
				{
					context().forward(combinedOldKey, new PropagationWrapper<>(null, true, context().offset()));
				}
			}
			else
			{
				if(change.newValue != null)
				{
					KL extractedLeftKeyValue = mapper.apply(change.newValue);
					CombinedKey<KL, KR> newCombinedKeyValue = new CombinedKey<>(extractedLeftKeyValue, key);
					context().forward(newCombinedKeyValue, new PropagationWrapper<>(change.newValue, true, context().offset()));
				}
				else
				{
					//Both null
				}
			}
		}

		@Override
		public void close() {}
	}
}
