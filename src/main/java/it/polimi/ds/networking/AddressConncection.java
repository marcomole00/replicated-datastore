package it.polimi.ds.networking;

import java.io.IOException;
import java.net.Socket;
import java.util.logging.Logger;

public class AddressConncection extends Connection {
    /**
     * creates connection with socket from address and port
     * @param address the target ip to connect to
     * @param port the remote port to connect to
     * @param logger the connection debug info will be sent to logger
     */
    public AddressConncection(String address, int port, Logger logger) throws IOException {
        super(new Socket(address, port), logger);
    }
}
