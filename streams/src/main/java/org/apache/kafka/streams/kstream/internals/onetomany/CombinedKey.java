package org.apache.kafka.streams.kstream.internals.onetomany;

public class CombinedKey<KL, KR> {
    private final KL leftKey;
    private KR rightKey = null;

    public CombinedKey(final KL leftKey, final KR rightKey) {
        this.leftKey = leftKey;
        this.rightKey = rightKey;
    }

    public CombinedKey(final KL leftKey) {
        this.leftKey = leftKey;
    }

    public KL getLeftKey() {
        return this.leftKey;
    }

    public KR getRightKey() {
        return this.rightKey;
    }

    public boolean equals(KL leftKey, KR rightKey) {
        return this.leftKey.equals(leftKey) && this.rightKey.equals(rightKey);
    }
}
