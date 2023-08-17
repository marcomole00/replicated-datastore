package it.polimi.ds.networking;

import java.util.Objects;

enum TopicType {
    STRING,
    ANY
}

public class Topic {
    final TopicType type;
    final String string;

    private Topic(TopicType type, String string) {
        this.type = type;
        this.string = string;
    }

     public static Topic any() {
         return new Topic(TopicType.ANY, null);
     }

     public static Topic fromString(String string) {
        return new Topic(TopicType.STRING, string);
     }

    public boolean isString() {
        return type == TopicType.STRING;
    }

    public String getString() {
        return string;
    }

    public boolean match(String string) {
         if (type == TopicType.ANY)
             return true;
         return this.string.equals(string);
     }

     public boolean contains(Topic t) {
         if (type == TopicType.ANY)
             return true;
         if (t.type == TopicType.ANY)
             return false;
         return string.equals(t.string);
     }

    @Override
    public String toString() {
        return "Topic{" +
                "type=" + type +
                ", string='" + string + '\'' +
                '}';
    }
}
