package it.polimi.ds.networking;

import java.util.Objects;

enum TopicType {
    STRING,
    ANY
}

public class Topic {
    TopicType type;
    String string;

     public static Topic any() {
         Topic t = new Topic();
         t.type = TopicType.ANY;
         return t;
     }

     public static Topic fromString(String string) {
         Topic t = new Topic();
         t.type = TopicType.STRING;
         t.string = string;
         return t;
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
}
