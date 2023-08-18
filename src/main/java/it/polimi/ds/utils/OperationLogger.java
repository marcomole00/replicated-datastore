package it.polimi.ds.utils;

import java.io.*;

public class OperationLogger {


    private final int node_id;

    private final BufferedWriter writer;



    public OperationLogger(int node_id) throws IOException {
        this.node_id = node_id;
        writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(node_id +".log")));
        writer.write("--------");
        writer.newLine();
        writer.flush();

    }


   public  synchronized void log_put(String key, String value, int version) {
        try {
            writer.write( "put: " + key + "," + value + "," + version);
            writer.newLine();
            writer.flush();
        } catch (IOException ignored) {
            System.out.println(" operational logger not working");
        }

    }

}
