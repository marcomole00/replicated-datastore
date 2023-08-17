package it.polimi.ds.node;

import it.polimi.ds.networking.*;
import it.polimi.ds.networking.messages.*;
import it.polimi.ds.utils.SafeLogger;

import java.util.HashMap;
import java.util.logging.Level;

public class PeerConnector implements  Runnable{


    private final HashMap<Integer, Connection> peers;
    private final Topology topology;

    private final SafeLogger logger = SafeLogger.getLogger(this.getClass().getName());
    int myId;

    private int accepted_connections = 0;
    Node node;


    public PeerConnector(HashMap<Integer, Connection> peers, Topology topology, int id, Node node) {
            this.peers = peers;
            this.topology = topology;
            this.myId = id;
            this.node = node;
        }


        @Override
        public void run() {

            while (accepted_connections < topology.getNodes().size() - myId - 1) {
                try {
                    for (int i = myId+1; i < topology.getNodes().size(); i++) {
                        if (!peers.containsKey(i)) {
                            Address address = topology.getNodes().get(i);
                            Connection connection = new AddressConnection(address.getIp(), address.getPort(), logger);
                            accepted_connections++;
                            connection.send(new Presentation(myId));
                            connection.bindToMessage(new MessageFilter (Topic.any(), Read.class), node::onRead);
                            connection.bindToMessage(new MessageFilter (Topic.any(), ReadResponse.class), node::onReadResponse);
                            connection.bindToMessage(new MessageFilter (Topic.any(), ContactRequest.class), node::onContactRequest);


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
