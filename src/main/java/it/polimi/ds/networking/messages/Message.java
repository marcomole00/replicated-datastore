package it.polimi.ds.networking.messages;

public abstract class Message implements java.io.Serializable{
    private final String key;

    Message(String key) {
        this.key = key.replaceAll("\\s", ""); // remove all spaces
    }

    public String getKey() {
        return key;
    }
}
