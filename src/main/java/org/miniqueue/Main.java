package org.miniqueue;

import org.miniqueue.server.MiniQueueServer;
import org.miniqueue.util.Config;

import java.io.IOException;

/**
 * Entry point to the application
 */
public class Main {
    public static void main(String[] args) {
        MiniQueueServer server = null;
        try {
            server = new MiniQueueServer(Config.SERVER_PORT);
            server.start();

            // Add a shutdown hook to close the server gracefully
            final MiniQueueServer finalServer = server;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    finalServer.close();
                } catch (Exception e) {
                    System.err.println("Error during server shutdown: " + e.getMessage());
                }
            }));

            System.out.println("MiniQueueServer started on port " + Config.SERVER_PORT);

        } catch (IOException e) {
            System.err.println("Failed to start MiniQueueServer: " + e.getMessage());
            if (server != null) {
                try {
                    server.close();
                } catch (Exception ex) {
                    System.err.println("Error closing server after startup failure: " + ex.getMessage());
                }
            }
            System.exit(1);
        } catch (Exception e) {
            System.err.println("An unexpected error occurred: " + e.getMessage());
            System.exit(1);
        }
    }
}