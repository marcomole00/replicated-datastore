package it.polimi.ds.networking.messages;

public class Message {
    private final String key;

    Message(String key) {
        this.key = key.replaceAll("\\s", ""); // remove all spaces
    }

    public String getKey() {
        return key;
    }
}
