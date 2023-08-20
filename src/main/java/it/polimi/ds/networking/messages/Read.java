package it.polimi.ds.networking.messages;

public class Read extends  Message {


    public Read(String key) {
        super(key);
    }

    @Override
    public String toString() {
        return "Read{" +
                "key='" + getKey() + '\'' +
                '}';
    }

}
