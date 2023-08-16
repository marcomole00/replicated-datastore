package it.polimi.ds.utils;

public class Counter {

    private int value;

    public Counter(int value) {
        this.value = value;
    }

    public synchronized void increment() {
        value++;
    }

    public int getValue() {
        return value;
    }
}
