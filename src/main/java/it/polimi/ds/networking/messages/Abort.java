package it.polimi.ds.networking.messages;

public class Abort extends  Message{


    public Abort(String key) {
        super(key);

    }

    @Override
    public String toString() {
        return "Abort{" +
                "key='" + getKey() + '\'' +
                '}';
    }

}
