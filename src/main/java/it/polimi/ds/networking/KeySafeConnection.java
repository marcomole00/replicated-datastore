package it.polimi.ds.networking;

import it.polimi.ds.networking.messages.Message;
import it.polimi.ds.networking.messages.Presentation;
import it.polimi.ds.node.DataBase;
import it.polimi.ds.utils.SafeLogger;

import java.io.IOException;
import java.net.Socket;
import java.util.function.BiPredicate;

public class KeySafeConnection extends Connection{

    final DataBase db;

    private KeySafeConnection(Socket socket, SafeLogger logger, DataBase db) throws IOException {
        super(socket, logger);
        this.db = db;
    }

    public static KeySafeConnection fromSocket(Socket socket, SafeLogger logger, DataBase db) throws IOException {
        return new KeySafeConnection(socket, logger, db);
    }

    public static KeySafeConnection fromAddress(Address address, SafeLogger logger, DataBase db) throws IOException {
        return new KeySafeConnection(new Socket(address.getIp(), address.getPort()), logger, db);
    }

    @Override
    public void bindToMessage(MessageFilter filter, BiPredicate<Connection, Message> action) {
        BiPredicate<Connection, Message> newAction = (c,m)-> {
            if (m instanceof Presentation) {
                return action.test(c, m);
            }
            else {
                db.putIfNotPresent(m.getKey());
                synchronized (db.get(m.getKey())) {
                    return action.test(c, m);
                }
            }
        };
        super.bindToMessage(filter, newAction);
    }
}
