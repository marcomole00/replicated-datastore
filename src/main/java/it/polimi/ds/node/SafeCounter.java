package it.polimi.ds.node;

public class SafeCounter {
    private int value;
    public SafeCounter() {
        value = 0;
    }

    public synchronized int getValue() {
        return value;
    }

    public synchronized int getAndIncrement() {
        value++;
        return value-1;
    }
}
