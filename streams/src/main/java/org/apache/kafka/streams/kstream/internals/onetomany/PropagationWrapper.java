package org.apache.kafka.streams.kstream.internals.onetomany;

public class PropagationWrapper<T> {

    private final T elem;
    private final boolean printable;
    private final long offset;

    public PropagationWrapper(T elem, boolean printable, long offset) {
        this.elem = elem;
        this.printable = printable;
        this.offset = offset;
    }

    public T getElem() {
        return elem;
    }

    public boolean isPrintable() {
        return printable;
    }

    public long getOffset() { return offset;}

}
