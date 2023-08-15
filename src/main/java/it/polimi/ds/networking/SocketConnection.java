package it.polimi.ds.networking;

import it.polimi.ds.utils.SafeLogger;

import java.io.IOException;
import java.net.Socket;
import java.util.logging.Logger;

public class SocketConnection extends Connection {
    /**
     * create a connection with no callback on new message
     * @param socket the socket this connection will use to send and receive messages
     * @param logger the connection debug info will be sent to logger
     */
    public SocketConnection(Socket socket, SafeLogger logger) throws IOException {
        super(socket, logger);
    }
}
