package it.polimi.ds.node;

import it.polimi.ds.networking.Address;
import it.polimi.ds.networking.Connection;
import it.polimi.ds.networking.SocketConnection;
import it.polimi.ds.networking.Topology;
import it.polimi.ds.utils.SafeLogger;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.logging.Level;

public class SocketAccepter implements  Runnable{

    private ServerSocket serverSocket;
    private HashMap<Integer, Connection> peers;
    private Topology topology;

    private SafeLogger logger = SafeLogger.getLogger(this.getClass().getName());



    public SocketAccepter(ServerSocket serverSocket, HashMap<Integer, Connection> peers, Topology topology) {
        this.serverSocket = serverSocket;
        this.peers = peers;
        this.topology = topology;

    }

    @Override
    public void run() {

        while (true) {
           try {
              Socket socket =  serverSocket.accept();
              int id = topology.getId(socket.getInetAddress().getHostAddress());
                if (id == -1) {
                    logger.log(Level.WARNING ,"Received connection from unknown address " + socket.getInetAddress().getHostAddress());
                    continue;
                }

               Connection connection = new SocketConnection(socket, logger);
              peers.put(id, connection);
           } catch (Exception e) {
               e.printStackTrace();
           }
        }

    }
}
