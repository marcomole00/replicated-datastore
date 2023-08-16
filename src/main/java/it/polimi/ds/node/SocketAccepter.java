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

    private final ServerSocket serverSocket;
    private  final HashMap<Integer, Connection> peers;
    private final Topology topology;

    private final SafeLogger logger = SafeLogger.getLogger(this.getClass().getName());



    public SocketAccepter(ServerSocket serverSocket, HashMap<Integer, Connection> peers, Topology topology) {
        this.serverSocket = serverSocket;
        this.peers = peers;
        this.topology = topology;

    }

    @Override
    public void run() {

        while (peers.size() < topology.getNodes().size() - 1) {
           try {
              Socket socket = serverSocket.accept();
              int id = topology.getId(socket.getInetAddress().getHostAddress());
                if (id == -1) {
                    logger.log(Level.WARNING ,"Received connection from unknown address " + socket.getInetAddress().getHostAddress());
                    continue;
                }

                System.out.println("Received connection from " + id + " from port " + socket.getPort());

               Connection connection = new SocketConnection(socket, logger);
              synchronized ( peers) {
                  peers.put(id, connection);
              }
           } catch (Exception e) {
               e.printStackTrace();
           }
        }

        System.out.println("Socket accepter finished");

    }
}
