package it.polimi.ds.networking.messages;

public class GetResponse extends Message{
    private final String value;
    private final int version;
    public GetResponse (String key, String value, int version) {
        super(key);
        this.value = value;
        this.version = version;
    }

    public int getVersion() {
        return version;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "GetResponse{" +
                "key='" + getKey() + '\'' +
                ", value='" + value + '\'' +
                ", version=" + version +
                '}';
    }
}
