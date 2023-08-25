package it.polimi.ds.utils;

import java.util.logging.Level;
import java.util.logging.Logger;

public class SafeLogger  {
    private final Logger innerLogger;
    private boolean debug;
    private SafeLogger(String name, boolean debug) {
        innerLogger = Logger.getLogger(name);
        this.debug = debug;
    }

    public static SafeLogger getLogger(String name, boolean debug) {
        return new SafeLogger(name, debug);
    }

    public synchronized void log(Level level, String message) {
        if (debug) innerLogger.log(level, message);
    }

    public synchronized void log(Level level, String message, Throwable thrown) {
       if (debug) innerLogger.log(level, message, thrown);
    }
}
