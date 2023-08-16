package it.polimi.ds.node;

import it.polimi.ds.networking.*;
import it.polimi.ds.networking.messages.Presentation;
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

    private final int myId;

    private  int accepted_connections = 0;


    public SocketAccepter(ServerSocket serverSocket, HashMap<Integer, Connection> peers, Topology topology, int id) {
        this.serverSocket = serverSocket;
        this.peers = peers;
        this.topology = topology;
        this.myId = id;

    }

    @Override
    public void run() {

        while (accepted_connections < myId) { // the id number is the number of incoming connection for that node
           System.out.println("peer size " + peers.size() + " topology size " + topology.getNodes().size());

            try {
              Socket socket = serverSocket.accept();
              int id = topology.getId(socket.getInetAddress().getHostAddress());
                if (id == -1) {
                    logger.log(Level.WARNING ,"Received connection from unknown address " + socket.getInetAddress().getHostAddress());
                    continue;
                }


               Connection connection = new SocketConnection(socket, logger);
                Presentation p =  (Presentation) connection.waitMessage(new MessageFilter("", Presentation.class));

                   synchronized (peers) {
                       peers.put(p.getId(), connection);
                       System.out.println("Received connection from " + p.getId() + " from port " + socket.getPort());
                       System.out.println("peers size" + peers.size());
                       accepted_connections++;
                   }


           } catch (Exception e) {
               e.printStackTrace();
           }
        }

        for (Connection c : peers.values()) {
            c.clearBindings("");
        }

        System.out.println("All peers are connected");



    }
}
