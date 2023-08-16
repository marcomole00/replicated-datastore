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


               Connection connection = new SocketConnection(socket, logger);
               connection.bindToMessage(new MessageFilter("", Presentation.class), (c, m) -> {
                   synchronized (peers) {
                       Presentation p = (Presentation) m;
                       peers.put(p.getId(), c);
                       System.out.println("Received connection from " + p.getId() + " from port " + socket.getPort());

                   }
                   return true;
               } );

               connection.clearBindings("");

           } catch (Exception e) {
               e.printStackTrace();
           }
        }

        System.out.println("Socket accepter finished");

    }
}
