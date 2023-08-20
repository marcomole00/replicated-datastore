package it.polimi.ds.networking.messages;

public class PutRequest extends Message{
    private final String value;
    public PutRequest(String key, String value) {
        super(key);
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "PutRequest{" +
                "key='" + getKey() + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
