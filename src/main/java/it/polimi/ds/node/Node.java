package it.polimi.ds.node;

import it.polimi.ds.networking.*;
import it.polimi.ds.networking.messages.*;
import it.polimi.ds.utils.Config;
import it.polimi.ds.utils.OperationLogger;
import it.polimi.ds.utils.SafeLogger;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.net.Socket;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.logging.Level;


public class Node {

    private final  HashMap<Integer, Connection> peers = new HashMap<>();

    SafeCounter contactCounter = new SafeCounter();

    SafeLogger logger = SafeLogger.getLogger(this.getClass().getName());

    OperationLogger operationLogger;

    Config config;

    AutoDiscoverSocket serverSocket;

    DataBase db = new DataBase();

    AbortedStack aborted = new AbortedStack();

    private final LockSet locks = new LockSet();

    public void run() throws Exception {
        //for each node in the topology create a connection
        //start the server socket
        config = new Config();
        serverSocket = new AutoDiscoverSocket(config);
        operationLogger = new OperationLogger(serverSocket.getMyId());
        PeerConnector peerConnector = new PeerConnector(peers, config.getTopology(), serverSocket.getMyId(), this);
        Thread connectorThread = new Thread(peerConnector);
        connectorThread.start();

        while(true) {
            try {
                Socket socket = serverSocket.accept();
                int id = config.getTopology().getId(socket.getInetAddress().getHostAddress());
                if (id == -1) {
                    logger.log(Level.WARNING ,"Received connection from unknown address " + socket.getInetAddress().getHostAddress());
                    continue;
                }
                Connection connection = Connection.fromSocket(socket, logger, locks);
                connection.bind(new MessageFilter(Topic.any(), Presentation.class), decoratedCallback(this::onPresentation));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public LockSet getLocks() {
        return locks;
    }

    boolean onPresentation(Connection connection, Message message) {
        Presentation p = (Presentation) message;

        if ( p.getId() < 0) {
            connection.clearBindings(Topic.any());
            connection.bind(new MessageFilter(Topic.any(), GetRequest.class), decoratedCallback(this::onGetRequest));
            connection.bind(new MessageFilter(Topic.any(), PutRequest.class), decoratedCallback(this::onPutRequest));
            connection.setId(-1);
        }
        else if (0 <= p.getId() && p.getId() < config.getNumberOfNodes()) {
            synchronized (peers) {
                peers.put(p.getId(), connection);
                System.out.println("Received connection from " + p.getId());
                connection.setId(p.getId());
                connection.clearBindings(Topic.any());
                connection.bind(new MessageFilter(Topic.any(), Read.class), decoratedCallback(this::onRead));
                connection.bind(new MessageFilter(Topic.any(), ReadResponse.class), decoratedCallback(this::onReadResponse));
                connection.bind(new MessageFilter(Topic.any(), ContactRequest.class), decoratedCallback(this::onContactRequest));
            }
        }
        return true;
    }

    void changeState(String key, State newState) {
        for (Connection c : peers.values()) {
            c.clearBindings(Topic.fromString(key));
        }
        logger.log(Level.INFO, "CHANGING STATE(" + key + ") from " +  db.get(key).getMetadata().state +  " to " + newState);
        for (Connection c : peers.values()) {
            logger.log(Level.INFO, c.getId() +" inbox:" + c.printQueue());
        }
        logger.log(Level.INFO, "Aborted stack: " + aborted);
        db.get(key).getMetadata().state = newState;
        db.get(key).getMetadata().ackCounter = 0;
        db.get(key).getMetadata().writeMaxVersion = -1;
        if(newState == State.Idle ){
            db.get(key).getMetadata().toWrite = null;
            db.get(key).getMetadata().coordinator = null;
            db.get(key).getMetadata().contactId = null;
        }
        if (newState == State.Idle && !aborted.isEmpty()) {
            Pair<Connection, PutRequest> p = aborted.pop();
            onPutRequest(p.getLeft(), p.getRight());
        }
        else {
            for (Connection c : peers.values()) {
                if (newState == State.Idle) {
                    c.bind(new MessageFilter(Topic.fromString(key), ContactRequest.class), decoratedCallback(this::onContactRequest));
                } else if (newState == State.Ready) {
                    c.bind(new MessageFilter(Topic.fromString(key), Abort.class), decoratedCallback(this::onAbort));
                    c.bind(new MessageFilter(Topic.fromString(key), ContactRequest.class), decoratedCallback(this::onContactRequest));
                    c.bind(new MessageFilter(Topic.fromString(key), Write.class), decoratedCallback(this::onWrite));
                } else if (newState == State.Waiting) {
                    c.bind(new MessageFilter(Topic.fromString(key), ContactRequest.class), decoratedCallback(this::onContactRequest));
                    c.bind(new MessageFilter(Topic.fromString(key), Nack.class), decoratedCallback(this::onNack));
                    c.bind(new MessageFilter(Topic.fromString(key), ContactResponse.class), decoratedCallback(this::onContactResponse));
                }
            }
        }
    }

    BiPredicate<Connection, Message> decoratedCallback(BiPredicate<Connection, Message> action) {
        return (c,m)-> {
            boolean res = action.test(c, m);
            for (Connection p : peers.values()) {
                p.updateQueue(Topic.fromString(m.getKey()));
            }
            return res;
        };
    }

    boolean onAbort(Connection c, Message msg) {
        logger.log(Level.INFO, "Elaborating on: " + msg + " from " + c.getId()
        + " my coordinator is " + db.get(msg.getKey()).getMetadata().coordinator);
       if (Objects.equals(c.getId(), db.get(msg.getKey()).getMetadata().coordinator))
           changeState(msg.getKey(), State.Idle);

        return true;
    }

    boolean onContactRequest(Connection c, Message msg) {
        db.putIfNotPresent(msg.getKey());
        ContactRequest contactRequest = (ContactRequest) msg;
        logger.log(Level.INFO, "Elaborating on: " + contactRequest + " from " + c.getId());
            Integer node = c.getId();
            if (node == -1) return false; // drop message
//            System.out.println("I received a contact request from " + node + " message was: " + contactRequest);
            Metadata metadata = db.get(msg.getKey()).getMetadata();
            if (db.get(msg.getKey()).getMetadata().state == State.Waiting) {
                if (node > serverSocket.getMyId()) {
                    changeState(msg.getKey(), State.Aborted);
                    for (int i = serverSocket.getMyId() + 1; i < serverSocket.getMyId() + config.getWriteQuorum(); i++) {
                        peers.get(i % config.getNumberOfNodes()).send(new Abort(msg.getKey(), metadata.contactId ));
                    }
                    aborted.push(new ImmutablePair<>(metadata.writeClient, new PutRequest(msg.getKey(), metadata.toWrite)));
                    metadata.coordinator = node;
                    metadata.contactId = contactRequest.getContactId();
                    if(serverSocket.getMyId() < node + config.getWriteQuorum() - config.getNumberOfNodes()) {
                        changeState(msg.getKey(), State.Ready);
                        c.send(new ContactResponse(msg.getKey(), db.get(msg.getKey()).getVersion(), metadata.contactId));
                    }
                    else {
                        changeState(msg.getKey(), State.Idle);
                    }
                    return true;
                } else {
                   logger.log(Level.INFO, "I'm the coordinator, ignoring contact request for now");
                    return false;
                }
            } else if (db.get(msg.getKey()).getMetadata().state == State.Ready) {
                if (node > metadata.coordinator) {
                    c.send(new Nack(msg.getKey(), metadata.coordinator, metadata.contactId));
                }
                else if (node.equals(metadata.coordinator)) {
                    metadata.contactId = contactRequest.getContactId();
                    c.send(new ContactResponse(msg.getKey(), db.get(msg.getKey()).getVersion(), metadata.contactId));
                    return true;

                }

                return false;
            } else if (db.get(msg.getKey()).getMetadata().state == State.Idle) {
                metadata.coordinator = node;
                changeState(msg.getKey(), State.Ready);
                metadata.contactId = contactRequest.getContactId();
                c.send(new ContactResponse(msg.getKey(), db.get(msg.getKey()).getVersion(), metadata.contactId));
                return true;
            }
            return false; // if state is Committed or Aborted don't consume the message
    }

    boolean onContactResponse(Connection ignored, Message msg) {
        ContactResponse contactResponse = (ContactResponse) msg;
        logger.log(Level.INFO, "Elaborating on: " + contactResponse);
        Metadata metadata = db.get(msg.getKey()).getMetadata();
        if (contactResponse.getContactId() != metadata.contactId) {
            logger.log(Level.INFO, "dropping contact response," +
                    " my contact id is " + metadata.contactId + " but I received " + contactResponse.getContactId());

            return true; // drop message
        }
        metadata.ackCounter++;
        if (contactResponse.getVersion() > metadata.writeMaxVersion)
            metadata.writeMaxVersion = contactResponse.getVersion();
        if (metadata.ackCounter == config.getWriteQuorum()-1) {
            Write write = new Write(msg.getKey(), metadata.toWrite, metadata.writeMaxVersion+1);
            PutResponse putResponse = new PutResponse(msg.getKey(), metadata.writeMaxVersion+1);
            changeState(contactResponse.getKey(), State.Committed);
            db.get(msg.getKey()).setValue(write.getValue());
            db.get(msg.getKey()).setVersion(write.getVersion());
            for(int i = serverSocket.getMyId()+1; i < serverSocket.getMyId() + config.getWriteQuorum(); i++) {
                peers.get(i % config.getNumberOfNodes()).send(write);
            }
            metadata.writeClient.send(putResponse);
            metadata.writeClient.stop();
            changeState(contactResponse.getKey(), State.Idle);
            operationLogger.log_put(write.getKey(), write.getValue(), write.getVersion());
        }
        return true;
    }

    boolean onGetRequest(Connection c, Message msg) {
        db.putIfNotPresent(msg.getKey());
        Metadata metadata = db.get(msg.getKey()).getMetadata();
        if (!metadata.reading) {
            metadata.reading = true;
            metadata.readMaxVersion = db.get(msg.getKey()).getVersion();
            metadata.latestValue = db.get(msg.getKey()).getValue();
            metadata.readClient = c;
            for(int i = serverSocket.getMyId()+1; i < serverSocket.getMyId() + config.getReadQuorum(); i++) {
                peers.get(i % config.getNumberOfNodes()).send(new Read(msg.getKey()));
            }
            return  true;
        }
        else {
            return false;
        }
    }

    boolean onNack(Connection ignored, Message msg) {
        Nack nack = (Nack) msg;
        logger.log(Level.INFO, "Elaborating on: " + nack);
        if (nack.getContactId() != db.get(msg.getKey()).getMetadata().contactId)
            return true; // drop message
        if (nack.getNodeID() >= serverSocket.myId + config.getWriteQuorum())
            peers.get(nack.getNodeID()).send(new ContactRequest(msg.getKey(), nack.getContactId()));
        return true;
    }

    boolean onPutRequest(Connection c, Message msg) {
        PutRequest putRequest = (PutRequest) msg;
        logger.log(Level.INFO, "Elaborating on: " + putRequest);
        db.putIfNotPresent(msg.getKey());
        Metadata metadata = db.get(msg.getKey()).getMetadata();
        if (metadata.state != State.Idle) {
            aborted.push(new ImmutablePair<>(c, putRequest));
            logger.log(Level.INFO, "Aborted: " + putRequest);
            return true;
        }
        changeState(msg.getKey(), State.Waiting);
        metadata.toWrite = putRequest.getValue();
        metadata.writeMaxVersion = db.get(msg.getKey()).getVersion();
        metadata.writeClient = c;
        metadata.contactId = contactCounter.getAndIncrement();
        for(int i = serverSocket.getMyId()+1; i < serverSocket.getMyId() + config.getWriteQuorum(); i++) {
            peers.get(i % config.getNumberOfNodes()).send(new ContactRequest(msg.getKey(), metadata.contactId));
        }
        return true;
    }

    boolean onRead(Connection c, Message msg) {
        logger.log(Level.INFO, "Elaborating on: " +  (Read) msg);
        db.putIfNotPresent(msg.getKey());
        Entry  entry = db.get(msg.getKey());
        String value = entry.getValue();
        int version = entry.getVersion();
        c.send(new ReadResponse(msg.getKey(), value, version));
        return true;
    }

    boolean onReadResponse(Connection ignored, Message msg) {
        ReadResponse readResponse = (ReadResponse) msg;
        logger.log(Level.INFO, "Elaborating on: " + readResponse);
        Metadata metadata = db.get(msg.getKey()).getMetadata();
        metadata.readCounter++;
        if (readResponse.getVersion() > metadata.readMaxVersion) {
            metadata.readMaxVersion = readResponse.getVersion();
            metadata.latestValue = readResponse.getValue();
        }
        if (metadata.readCounter == config.getReadQuorum()-1) {
            metadata.readClient.send(new GetResponse(msg.getKey(), metadata.latestValue, metadata.readMaxVersion));
            metadata.readClient.stop();
            metadata.readMaxVersion = -1;
            metadata.latestValue = null;
            metadata.readClient = null;
            metadata.readCounter = 0;
            metadata.reading = false;
        }
        return true;
    }

    boolean onWrite(Connection ignored, Message msg) {
        Write write = (Write) msg;
        logger.log(Level.INFO, "Elaborating on: " + write);
        db.get(write.getKey()).setValue(write.getValue());
        db.get(write.getKey()).setVersion(write.getVersion());

      // on Write dovrebbe essere accettata anche fuori da Ready
        // se viene accettata fuori da ready però lo stato non deve tornare ad idle

        changeState(write.getKey(), State.Idle);
        operationLogger.log_put(write.getKey(), write.getValue(), write.getVersion());
        return true;
    }
}
