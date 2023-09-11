package it.polimi.ds.node;

import it.polimi.ds.networking.Address;
import it.polimi.ds.networking.Connection;
import it.polimi.ds.networking.MessageFilter;
import it.polimi.ds.networking.Topic;
import it.polimi.ds.networking.messages.*;
import it.polimi.ds.utils.SafeLogger;
import it.polimi.ds.utils.Topology;

import java.util.concurrent.ConcurrentHashMap;

public class PeerConnector implements  Runnable{


    private final ConcurrentHashMap<Integer, Connection> peers;
    private final Topology topology;

    private final SafeLogger logger = SafeLogger.getLogger(this.getClass().getName() ,true);
    int myId;

    private int accepted_connections = 0;
    Node node;

    public PeerConnector(ConcurrentHashMap<Integer, Connection> peers, Topology topology, int id, Node node) {
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
                            Connection connection = Connection.fromAddress(address, logger, node.getLocks());
                            accepted_connections++;
                            connection.setId(i);
                            connection.send(new Presentation(myId));
                            connection.bind(new MessageFilter (Topic.any(), Read.class), node.decoratedCallback(node::onRead));
                            connection.bind(new MessageFilter (Topic.any(), ReadResponse.class), node.decoratedCallback(node::onReadResponse));
                            connection.bind(new MessageFilter (Topic.any(), Write.class), node.decoratedCallback(node::onWrite));
                            connection.bind(new MessageFilter (Topic.any(), ContactRequest.class), node.decoratedCallback(node::onContactRequest));


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
