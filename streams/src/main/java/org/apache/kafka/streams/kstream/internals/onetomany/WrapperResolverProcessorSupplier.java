package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableRangeValueGetterSupplier;
import org.apache.kafka.streams.kstream.internals.KTableSourceValueGetterSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class WrapperResolverProcessorSupplier<KR, V>
        implements ProcessorSupplier<KR, Change<PrintableWrapper<V>>>
{

    private final String stateStoreName;

    //Right driven updates
    public WrapperResolverProcessorSupplier(String stateStoreName)
    {
        this.stateStoreName = stateStoreName;
    }


    @Override
    public Processor<KR, Change<PrintableWrapper<V>>> get()
    {
        return new AbstractProcessor<KR, Change<PrintableWrapper<V>>>()
        {
            KeyValueStore<KR, Long> offsetHighWaterStore;

            @Override
            public void init(ProcessorContext context)
            {
                super.init(context);
                this.offsetHighWaterStore = (KeyValueStore<KR, Long>) context.getStateStore(stateStoreName);
            }

            @Override
            public void process(KR key, Change<PrintableWrapper<V>> value)
            {
                //highwater = X, value(offset = x+1, null)      => update & send
                //highwater = X, value(offset = x+1, non-null)  => update & send
                //highwater = X, value(offset = x, null)        => Should not occur, as nulls are dropped previously in workflow
                //highwater = X, value(offset = x, non-null)    => Should not occur, as same -offset only exists as above.
                //highwater = X, value(offset = x-1, null)      => Do not send
                //highwater = X, value(offset = x-1, non-null)  => Do not send

                final Long highwater = offsetHighWaterStore.get(key);
                if (null == highwater || value.newValue.getOffset() >= highwater ) {
                    // > as its new highwater
                    // = as we want to resend in the case of a node failure, but only if it's not null.
                    //System.out.println("Highwater mark = " + highwater + ", value.newValue = " + value.newValue.getElem().toString());
                    offsetHighWaterStore.put(key, value.newValue.getOffset());
                    context().forward(key, new Change<>(value.newValue.getElem(), null));
                } else if (value.newValue.getOffset() == -1 ) {
                    //TODO - Is there a better way to forward from the left? Perhaps the topic metadata?
                    context().forward(key, new Change<>(value.newValue.getElem(), null));
                }
            }
        };
    }
}
