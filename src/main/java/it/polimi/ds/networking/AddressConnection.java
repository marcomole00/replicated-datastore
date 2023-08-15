package it.polimi.ds.networking;

import it.polimi.ds.utils.SafeLogger;

import java.io.IOException;
import java.net.Socket;
import java.util.logging.Logger;

public class AddressConnection extends Connection {
    /**
     * creates connection with socket from address and port
     * @param address the target ip to connect to
     * @param port the remote port to connect to
     * @param logger the connection debug info will be sent to logger
     */
    public AddressConnection(String address, int port, SafeLogger logger) throws IOException {
        super(new Socket(address, port), logger);
    }
}
