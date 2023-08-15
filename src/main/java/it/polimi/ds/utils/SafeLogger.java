package it.polimi.ds.utils;

import java.util.logging.Level;
import java.util.logging.Logger;

public class SafeLogger {
    private final Logger innerLogger;
    private SafeLogger(String name) {
        innerLogger = Logger.getLogger(name);
    }

    public static SafeLogger getLogger(String name) {
        return new SafeLogger(name);
    }

    public synchronized void log(Level level, String message) {
        innerLogger.log(level, message);
    }

    public synchronized void log(Level level, String message, Throwable thrown) {
        innerLogger.log(level, message, thrown);
    }
}
