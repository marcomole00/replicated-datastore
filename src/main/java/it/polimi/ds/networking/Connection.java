package it.polimi.ds.networking;

import it.polimi.ds.networking.messages.Message;
import it.polimi.ds.utils.SafeLogger;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public abstract class Connection {
    private final Socket socket;
    protected final ObjectInputStream reader;
    protected final ObjectOutputStream writer;
    private final SafeLogger logger;

    private final Thread listenThread;

    private final Queue<Message> inbox = new LinkedList<>();

    private final Map<MessageFilter, BiPredicate<Connection, Message>> bindings = new HashMap<>();

    protected Connection(Socket socket, SafeLogger logger) throws IOException {
        this.socket = socket;
        writer = new ObjectOutputStream(socket.getOutputStream());
        reader = new ObjectInputStream(socket.getInputStream());
        this.logger = logger;
        listenThread = new Thread(this::listenMessages);
        listenThread.start();
    }

    /**
     * serialize the message and send it through the network, if the message is a move set sending order before
     * @param message the message to be serialized and sent
     */
    public void send(Message message) {
        try {
            synchronized (writer) {
                writer.writeObject(message);
            }
        } catch (IOException ignored) { /*ignored*/ }
    }

    /**
     * waits for a message that matches the filter
     * @param filter a list of class that compose the filter
     * @return the first received message or in queue to be processed that matches the filter
     */
    public Message waitMessage(MessageFilter filter) {
        Message m = pollFirstMatch(filter); // eventual message that was received before the call of waitMessage
        while (m == null) {  // if no compatible found wait for one

                try {
                    synchronized (this) {
                        wait();
                        m = pollFirstMatch(filter);
                    }
                } catch (InterruptedException e) {
                    logger.log(Level.WARNING, "Interrupted", e);
                    Thread.currentThread().interrupt();
                }
            }
        return m;
    }

    /**
     * remove the first matching message from messagesToProcess queue and returns it
     * @param filter the filter the message must match
     * @return the message removed from the queue or null if none is available
     */
    Message pollFirstMatch(MessageFilter filter) {
        synchronized (inbox) {
            for (Message m : inbox) {
                if (matchFilter(m, filter)) {
                    inbox.remove(m);
                    return m;
                }
            }
            return null;
        }
    }

    /**
     * checks if the message belongs to one of the classes in the filter
     * @param message the message to be checked
     * @param filter a list of class that the filter will match
     * @return if the message matches
     */
    static boolean matchFilter(Message message, MessageFilter filter) {
        for (Class<?> c: filter.getClasses()) {
            if(c.isInstance(message))
                return filter.getTopic().match(message.getKey());
        }
        return false;
    }

    public void bindToMessage(MessageFilter filter, BiPredicate<Connection, Message> action) {
        synchronized (bindings) {
            logger.log(Level.INFO,"Binding " + filter + " to " + action);
            bindings.put(filter, action);
        }
    }

    public void clearBindings(Topic topic) {
        synchronized (bindings) {
            bindings.keySet().stream().filter(e->topic.contains(e.getTopic())).toList().forEach(bindings.keySet()::remove);
        }
    }

    void listenMessages() {
        String toLog = "Listening for new messages from: " + socket.getInetAddress();
        logger.log(Level.INFO, toLog);
        while (isRunning()) {
            try {

                Message msg = (Message) reader.readObject();
                if(!matchBinding(msg)) {
                    synchronized (inbox) {
                        inbox.add(msg); //TODO: fifo channels
                        logger.log(Level.INFO, "Received message: " + msg);
                    }
                    synchronized (this) {
                        notifyAll();
                    }
                }

            } catch (IOException e) {
                toLog = "IOException when reading message: " + e.getMessage();
                logger.log(Level.SEVERE, toLog);
            } catch (ClassNotFoundException ignored) {}
        }
    }

    Boolean matchBinding(Message m) {
        synchronized (bindings) {
            for (Map.Entry<MessageFilter, BiPredicate<Connection, Message>> b : bindings.entrySet()) {
                if(matchFilter(m, b.getKey())) {
                    return b.getValue().test(this, m);
                }
            }
            return false;
        }
    }

    /**
     * checks if the connection is running by checking socket state
     * @return if the connection is still listening for messages
     */
    boolean isRunning() {
        return !socket.isClosed();
    }

    /**
     * stops the listen message loop by closing the socket that will raise an exception on readObject()
     */
    public void stop() {
        try {
            socket.close();
        } catch (IOException ignored) {}
    }

    /**
     * runs stop() and wait for the listen message thread to terminate
     */
    public void close() {
        try {
            stop();
            if (!Thread.currentThread().equals(listenThread))
                listenThread.join();
        } catch (InterruptedException e) {
            logger.log(Level.WARNING, "Interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

}
