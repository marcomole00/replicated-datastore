package it.polimi.ds.networking.messages;

public class Write extends  Message{

    private final String value;
    private final int version;

    public Write(String key, String value, int version) {
        super(key);
        this.value = value;
        this.version = version;
    }


    public String getValue() {
        return value;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "Write{" +
                "key='" + getKey() + '\'' +
                ", value='" + value + '\'' +
                ", version=" + version +
                '}';
    }
}
