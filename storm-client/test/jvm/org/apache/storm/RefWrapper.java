package org.apache.storm;

public final class RefWrapper<T> {
    private T ref;

    public T getRef() {
        return ref;
    }

    public void setRef(T ref) {
        this.ref = ref;
    }
}
