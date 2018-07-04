package org.apache.kafka.streams.kstream.internals.onetomany;


import org.apache.kafka.common.serialization.Serde;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class CombinedKey<KL, KR> {
    private KL leftKey;
    private KR rightKey;


    public CombinedKey(final KL leftKey, final KR rightKey) {
        this.leftKey = leftKey;
        this.rightKey = rightKey;
    }

    public KL getLeftKey() {
        return this.leftKey;
    }

    public KR getRightKey() {
        return this.rightKey;
    }
}
