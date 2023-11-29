package de.dpa.example;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * This class is a proof-of-concept implementation how to pull the wireQ API and sends the data to an ActiveMQ server.
 * <b>Note</b>; Do not use this class in any production environment!
 */
public class Connector implements Callable<Integer> {
    private static final Logger log = LogManager.getLogger(Connector.class);
    /**
     * Default delay in seconds, if wireQ does not give a suggestion via "retry-after"-header
     */
    private static final int DEFAULT_DELAY = 30;
    private static final int THREAD_COUNT = 3;

    private final String wireQUrl = System.getenv("WIREQ_URL").replaceAll("/$", "");
    private final String activeMQ = System.getenv("ACTIVEMQ_URL");
    private final String activeMQ_User = "artemis";
    private final String activeMQ_Password = "artemis";

    private final OkHttpClient client = new OkHttpClient.Builder().build();

    /**
     * Starts {@code THREAD_COUNT} threads to receive data from wireQ and process them. If wireQ suggests to
     * "retry-after" a specific period of time, we follow that hint. (this is useful for situations when there are more
     * entries to get or after a 429 'too many requests' error)
     */
    public static void main(String[] args) {
        try (ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3)) {
            log.info("Starting schedule");
            var connectorMap = new HashMap<Integer, ScheduledFuture<Integer>>();
            while (true) { // infinite loop
                for (int i = 1; i <= THREAD_COUNT; i++) {
                    connectorMap.putIfAbsent(i, scheduler.schedule(new Connector(), DEFAULT_DELAY * i, TimeUnit.SECONDS));
                    connectorMap.computeIfPresent(i, (k, future) -> {
                        try {
                            if (future.isDone() || future.isCancelled()) {
                                return scheduler.schedule(new Connector(), future.get(), TimeUnit.SECONDS);
                            } else {
                                return future; // if it is not finished, do nothing
                            }
                        } catch (InterruptedException | ExecutionException | CancellationException e) {
                            log.error("One worker failed to process: " + e.getMessage(), e);
                            return scheduler.schedule(new Connector(), DEFAULT_DELAY, TimeUnit.SECONDS);
                        }
                    });
                }
                try {
                    // Sleep to reduce CPU load.
                    Thread.sleep(Duration.of(DEFAULT_DELAY / 2, ChronoUnit.SECONDS));
                } catch (InterruptedException ignored) {
                    // ignore exception
                }
            }
        }
    }

    /**
     * Receive entries from wireQ and immediately save them to an ActiveMQ broker. If wireQ suggests to retry-after a
     * specific period of time, this value is returned. (this is needed for the scheduler to delay the next execution.)
     *
     * @return The suggested time delay from wireQ in seconds if present. {@code DEFAULT_DELAY} otherwise.
     */
    @Override
    public Integer call() {
        Request request = new Request.Builder().url(wireQUrl + "/entries.json").get().build();
        log.info("Retrieving data...");
        int retryDelay = Connector.DEFAULT_DELAY;

        // Get entries from wireQ
        try (Response response = client.newCall(request).execute()) {
            String retryAfter = response.header("retry-after");
            if (retryAfter != null) {
                log.info("Recieved a retry-after value: " + retryAfter);
                retryDelay = Integer.parseInt(retryAfter);
            }
            if (!response.isSuccessful()) {
                log.error("Retrieving Data failed. Status Code: " + response.code());
                return retryDelay;
            }
            ResponseBody body = response.body();
            if (body == null) {
                log.warn("Response has no body.");
                return retryDelay;
            }

            JSONArray entries = new JSONObject(body.string()).getJSONArray("entries");
            if (entries.isEmpty()) {
                log.info("No Entries received.");
                return retryDelay;
            }

            // Open connection to ActiveMQ with standard admin credentials
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(activeMQ);
            try (Connection connection = connectionFactory.createConnection(activeMQ_User, activeMQ_Password)) {
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                // Defining the queue where the entries are stored
                Destination destination = session.createQueue("dpa.demo");
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);

                for (Object entry : entries) {
                    TextMessage message = session.createTextMessage(entry.toString());

                    JSONObject entryJson = new JSONObject(entry.toString());
                    // In case removing the entry from wireQ fails, setting this property ensures there are no duplicates
                    String entryId = entryJson.getString("entry_id");
                    message.setStringProperty("_AMQ_DUPL_ID", entryId);
                    log.info("Sent message: " + entryId);

                    // Sending the entry to ActiveMQ
                    producer.send(message);

                    // Removing the entry from wireQ
                    removeEntryFromWireQ(entryJson.getString("_wireq_receipt"));
                }
                session.close();
            }
        } catch (IOException e) {
            log.error("Could not retrieve data.", e);
        } catch (JMSException e) {
            log.error("Could not establish connection to message broker.", e);
        }
        return retryDelay;
    }

    /**
     * Sends a delete request to wireQ to remove the message {@code wireQReceipt} from the queue.
     *
     * @param wireQReceipt The receipt number of the message to remove from the queue.
     */
    private void removeEntryFromWireQ(String wireQReceipt) {
        Request delete = new Request.Builder().url(wireQUrl + "/entry/" + wireQReceipt).delete().build();
        client.newCall(delete).enqueue(new Callback() {
            @Override
            public void onFailure(@NotNull Call call, @NotNull IOException e) {
                log.warn("Could not remove entry from WireQ. Duplication may occur.", e);
            }

            @Override
            public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
                log.info("Deleted entry from WireQ");
            }
        });
    }
}
