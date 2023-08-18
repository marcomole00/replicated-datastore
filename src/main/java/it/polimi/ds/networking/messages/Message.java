package it.polimi.ds.networking.messages;

public abstract class Message implements java.io.Serializable{
    private final String key;

    Message(String key) {
        if (key == null)
            this.key = null;
        else
            this.key = key.replaceAll("\\s", ""); // remove all spaces
    }

    public String getKey() {
        return key;
    }
}
