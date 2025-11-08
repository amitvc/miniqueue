package org.miniqueue.api;

import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.miniqueue.storage.LocalDiskEventStorage;

/**
 * Http handler for Admin api
 */
public class AdminHandler extends MiniQueueApi {

    private final LocalDiskEventStorage eventStorage;

    public AdminHandler(LocalDiskEventStorage eventStorage) {
        this.eventStorage = eventStorage;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "Method Not Allowed");
            return;
        }

        String path = exchange.getRequestURI().getPath();
        if ("/admin/partitions".equals(path)) {
            Map<Short, List<Integer>> allocatedPages = eventStorage.getAllocatedPages();
            String jsonResponse = convertMapToJson(allocatedPages);
            sendResponse(exchange, 200, jsonResponse);
        } else {
            sendResponse(exchange, 404, "Not Found");
        }
    }

    private String convertMapToJson(Map<Short, List<Integer>> map) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        boolean first = true;
        for (Map.Entry<Short, List<Integer>> entry : map.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            sb.append("\"").append(entry.getKey()).append("\":");
            sb.append("[");
            for (int i = 0; i < entry.getValue().size(); i++) {
                sb.append(entry.getValue().get(i));
                if (i < entry.getValue().size() - 1) {
                    sb.append(",");
                }
            }
            sb.append("]");
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }
}
