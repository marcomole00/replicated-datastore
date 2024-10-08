package it.polimi.ds.node;

import it.polimi.ds.networking.Connection;
import it.polimi.ds.networking.LockSet;
import it.polimi.ds.networking.MessageFilter;
import it.polimi.ds.networking.Topic;
import it.polimi.ds.networking.messages.*;
import it.polimi.ds.utils.Config;
import it.polimi.ds.utils.OperationLogger;
import it.polimi.ds.utils.SafeLogger;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;


public class Node {

    private final ConcurrentHashMap<Integer, Connection> peers = new ConcurrentHashMap<>();

    private final List<Connection> clients = Collections.synchronizedList(new ArrayList<>());

    SafeCounter contactCounter = new SafeCounter();

    SafeLogger logger;

    OperationLogger operationLogger;

    Config config;

    AutoDiscoverSocket serverSocket;

    DataBase db = new DataBase();

    AbortedStack aborted = new AbortedStack();

    private final LockSet locks = new LockSet();

    public void run(boolean debug) throws Exception {
        //for each node in the topology create a connection
        //start the server socket
        logger = SafeLogger.getLogger(this.getClass().getName(), debug);
        config = new Config();
        serverSocket = new AutoDiscoverSocket(config);
        operationLogger = new OperationLogger(serverSocket.getMyId());
        PeerConnector peerConnector = new PeerConnector(peers, config.getTopology(), serverSocket.getMyId(), this,debug);
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
                Connection connection = Connection.fromSocket(socket, logger, locks, this::fullUpdate);
                connection.bind(new MessageFilter(Topic.any(), Presentation.class), this::onPresentation);
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
            connection.bind(new MessageFilter(Topic.any(), GetRequest.class), this::onGetRequest);
            connection.bind(new MessageFilter(Topic.any(), PutRequest.class), this::onPutRequest);
            connection.setId(-1);
            clients.add(connection);
        }
        else if (0 <= p.getId() && p.getId() < config.getNumberOfNodes()) {
            synchronized (peers) {
                peers.put(p.getId(), connection);
                System.out.println("Received connection from " + p.getId());
                connection.setId(p.getId());
                connection.clearBindings(Topic.any());
                connection.bind(new MessageFilter(Topic.any(), Read.class), this::onRead);
                connection.bind(new MessageFilter(Topic.any(), ReadResponse.class), this::onReadResponse);
                connection.bind(new MessageFilter(Topic.any(), Write.class), this::onWrite);
                connection.bind(new MessageFilter(Topic.any(), ContactRequest.class), this::onContactRequest);
            }
        }
        return true;
    }

    void changeState(String key, State newState) {
        for (Connection c : peers.values()) {
            c.clearBindings(Topic.fromString(key));
        }
        logger.log(Level.INFO, "CHANGING STATE(" + key + ") from " +  db.get(key).getMetadata().state +  " to " + newState);
        Metadata metadata = db.get(key).getMetadata();
        metadata.state = newState;
        metadata.ackCounter = 0;
        metadata.writeMaxVersion = -1;
        metadata.nacked.clear();
        if(newState == State.Idle ){
            metadata.toWrite = null;
            metadata.coordinator = null;
            metadata.contactId = null;
            metadata.extraContacts.clear();
        }
        if (newState == State.Idle && !aborted.isEmpty()) {
            Pair<Connection, PutRequest> p = aborted.pop();
            onPutRequest(p.getLeft(), p.getRight());
        }
        else {
            for (Connection c : peers.values()) {
                if (newState == State.Ready) {
                    c.bind(new MessageFilter(Topic.fromString(key), Abort.class), this::onAbort);
                } else if (newState == State.Waiting) {
                    c.bind(new MessageFilter(Topic.fromString(key), Nack.class), this::onNack);
                    c.bind(new MessageFilter(Topic.fromString(key), ContactResponse.class), this::onContactResponse);
                }
            }
        }
    }

    void fullUpdate(String key) {
        boolean change;
        do {
            change = false;
            for (Connection p : peers.values()) {
                change = change || p.waitUpdateQueue(Topic.fromString(key));
            }
            List<Connection> tmp = new ArrayList<>(clients);
            for (Connection c : tmp) {
                change = change || c.waitUpdateQueue(Topic.fromString(key));
            }
        } while (change);
    }

    boolean onAbort(Connection c, Message msg) {
        Abort abort = (Abort) msg;
        Metadata metadata = db.get(abort.getKey()).getMetadata();

        if (abort.getContactId() != metadata.contactId || !Objects.equals(c.getId(), metadata.coordinator))
            return false;
        changeState(msg.getKey(), State.Idle);
        return true;
    }

    boolean onContactRequest(Connection c, Message msg) {
        db.putIfNotPresent(msg.getKey());
        ContactRequest contactRequest = (ContactRequest) msg;
            Integer node = c.getId();
            if (node == -1) return false; // drop message
            Metadata metadata = db.get(msg.getKey()).getMetadata();
            if (db.get(msg.getKey()).getMetadata().state == State.Waiting) {
                if (node > serverSocket.getMyId()) {
                    logger.log(Level.INFO, "im aborting, my new coord  is " + node);
                    changeState(msg.getKey(), State.Aborted);
                    Abort abort = new Abort(msg.getKey(), metadata.contactId );
                    for (int i = serverSocket.getMyId() + 1; i < serverSocket.getMyId() + config.getWriteQuorum(); i++) {
                        peers.get(i % config.getNumberOfNodes()).send(abort);
                    }
                    metadata.extraContacts.forEach((i) -> peers.get(i).send(abort));
                    aborted.push(new ImmutablePair<>(metadata.writeClient, new PutRequest(msg.getKey(), metadata.toWrite)));
                    metadata.coordinator = node;
                    metadata.contactId = contactRequest.getContactId();
                    changeState(msg.getKey(), State.Ready);
                    c.send(new ContactResponse(msg.getKey(), db.get(msg.getKey()).getVersion(), contactRequest.getContactId()));
                    return true;
                } else {
                    return false;
                }
            } else if (metadata.state == State.Ready) {
                Pair<Integer, Integer> p = new ImmutablePair<>(node, contactRequest.getContactId());
                if (node > metadata.coordinator && !metadata.nacked.contains(p)) {
                    metadata.nacked.add(p);
                    c.send(new Nack(msg.getKey(), metadata.coordinator, contactRequest.getContactId()));
                }
                return false;
            } else if (db.get(msg.getKey()).getMetadata().state == State.Idle) {
                metadata.coordinator = node;
                changeState(msg.getKey(), State.Ready);
                metadata.contactId = contactRequest.getContactId();
                c.send(new ContactResponse(msg.getKey(), db.get(msg.getKey()).getVersion(), contactRequest.getContactId()));
                return true;
            }
            return false; // if state is Committed or Aborted don't consume the message
    }

    boolean onContactResponse(Connection c, Message msg) {
        ContactResponse contactResponse = (ContactResponse) msg;
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
            Write write = new Write(msg.getKey(), metadata.toWrite, metadata.writeMaxVersion+1, metadata.contactId);
            PutResponse putResponse = new PutResponse(msg.getKey(), metadata.writeMaxVersion+1, metadata.toWrite);
            changeState(contactResponse.getKey(), State.Committed);
            db.get(msg.getKey()).setValue(write.getValue());
            db.get(msg.getKey()).setVersion(write.getVersion());
            for(int i = serverSocket.getMyId()+1; i < serverSocket.getMyId() + config.getWriteQuorum(); i++) {
                peers.get(i % config.getNumberOfNodes()).send(write);
            }
            metadata.extraContacts.forEach((i) -> peers.get(i).send(write));
            metadata.writeClient.send(putResponse);
            metadata.writeClient.stop();
            clients.remove(metadata.writeClient);
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
        Metadata metadata = db.get(msg.getKey()).getMetadata();
        if (nack.getContactId() != metadata.contactId)
            return true; // drop message
        if ( (nack.getNodeID() >=(serverSocket.myId + config.getWriteQuorum()) - config.getNumberOfNodes()) && nack.getNodeID() < serverSocket.myId) {
            if (metadata.extraContacts.contains(nack.getNodeID())) // avoid repetitions
                return true; // drop message
            metadata.extraContacts.add(nack.getNodeID());
            peers.get(nack.getNodeID()).send(new ContactRequest(msg.getKey(), metadata.contactId));
            logger.log(Level.INFO, "New extraContacts: " + metadata.extraContacts);
        }
        return true;
    }

    boolean onPutRequest(Connection c, Message msg) {
        PutRequest putRequest = (PutRequest) msg;
        logger.log(Level.INFO, "Executing: " + putRequest + " from " + c.getId());
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
        logger.log(Level.INFO, "sending contact request because of"+ msg);
        for(int i = serverSocket.getMyId()+1; i < serverSocket.getMyId() + config.getWriteQuorum(); i++) {
            peers.get(i % config.getNumberOfNodes()).send(new ContactRequest(msg.getKey(), metadata.contactId));
        }
        return true;
    }

    boolean onRead(Connection c, Message msg) {
        db.putIfNotPresent(msg.getKey());
        Entry  entry = db.get(msg.getKey());
        String value = entry.getValue();
        int version = entry.getVersion();
        c.send(new ReadResponse(msg.getKey(), value, version));
        return true;
    }

    boolean onReadResponse(Connection ignored, Message msg) {
        ReadResponse readResponse = (ReadResponse) msg;
        Metadata metadata = db.get(msg.getKey()).getMetadata();
        metadata.readCounter++;
        if (readResponse.getVersion() > metadata.readMaxVersion) {
            metadata.readMaxVersion = readResponse.getVersion();
            metadata.latestValue = readResponse.getValue();
        }
        if (metadata.readCounter == config.getReadQuorum()-1) {
            metadata.readClient.send(new GetResponse(msg.getKey(), metadata.latestValue, metadata.readMaxVersion));
            metadata.readClient.stop();
            for(int i = serverSocket.getMyId()+1; i < serverSocket.getMyId() + config.getReadQuorum(); i++) {
                peers.get(i%config.getNumberOfNodes()).send(new Write(msg.getKey(), metadata.latestValue, metadata.readMaxVersion, -1));
            }
            metadata.readMaxVersion = -1;
            metadata.latestValue = null;
            metadata.readClient = null;
            metadata.readCounter = 0;
            metadata.reading = false;
        }
        return true;
    }

    boolean onWrite(Connection c, Message msg) {
        Write write = (Write) msg;
        Metadata metadata = db.get(write.getKey()).getMetadata();
        if (write.getContactId() == -1) {
            if (db.get(write.getKey()).getVersion() >= write.getVersion())
                return true; // drop message
        }
        else {
            if (write.getContactId() != metadata.contactId || !Objects.equals(c.getId(), metadata.coordinator))
                return false;
            changeState(write.getKey(), State.Idle);
        }
        db.get(write.getKey()).setValue(write.getValue());
        db.get(write.getKey()).setVersion(write.getVersion());
        operationLogger.log_put(write.getKey(), write.getValue(), write.getVersion());
        return true;
    }
}
