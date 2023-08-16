package it.polimi.ds.node;

import it.polimi.ds.networking.*;
import it.polimi.ds.networking.messages.Presentation;
import it.polimi.ds.utils.SafeLogger;

import java.util.HashMap;
import java.util.logging.Level;

public class PeerConnector implements  Runnable{


    private final HashMap<Integer, Connection> peers;
    private final Topology topology;

    private final SafeLogger logger = SafeLogger.getLogger(this.getClass().getName());
    int myId;


    public PeerConnector(HashMap<Integer, Connection> peers, Topology topology, int id) {
            this.peers = peers;
            this.topology = topology;
            this.myId = id;
        }


        @Override
        public void run() {

            while (peers.size() < topology.getNodes().size() - 1) {
                try {
                    for (int i = myId+1; i < topology.getNodes().size(); i++) {
                        if (!peers.containsKey(i)) {
                            Address address = topology.getNodes().get(i);
                            Connection connection = new AddressConnection(address.getIp(), address.getPort(), logger);
                            connection.send(new Presentation(myId));
                            System.out.println(myId + " Connecting to " + i);
                            synchronized (peers) {
                                peers.put(i, connection);
                            }

                        }
                    }
                } catch (Exception e) {
                   // logger.log(Level.WARNING, "Error while connecting to " +);
                }
            }

            System.out.println("Peer Connector finished");



        }

}
