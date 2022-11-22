package in.ashnehete.gigapaxos.mysql;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class MySQLAppClient extends ReconfigurableAppClientAsync<Request> {

    final static int CREATE_SERVICE_NAME_INDEX = 0;

    public MySQLAppClient() throws IOException {
        super();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int queryIdx = Integer.parseInt(args[0]);
        String[] queries = {
                "CREATE SERVICE NAME",
                "{\"database\": \"test\",\"query\": \"CREATE TABLE users (id INT, name varchar(64));\"}",
                "{\"database\": \"test\",\"query\": \"INSERT INTO users VALUES (1, 'Aashish');\"}",
                "{\"database\": \"test\",\"query\": \"INSERT INTO users VALUES (2, 'Alice')\"}",
                "{\"database\": \"test\",\"query\": \"UPDATE users SET name='Dumb' WHERE id=1\"}",
                "{\"database\": \"test\",\"query\": \"DELETE FROM users WHERE id=1\"}",
                "{\"database\": \"test\",\"query\": \"SELECT * FROM users;\"}"
        };

        System.out.println("Running query " + queries[queryIdx]);

        MySQLAppClient client = new MySQLAppClient();
        String name = "mysql";

        if (queryIdx == MySQLAppClient.CREATE_SERVICE_NAME_INDEX) {
            client.sendRequest(new CreateServiceName(name, "checkpoint/initial-state.sql"),
                    new RequestCallback() {
                        @Override
                        public void handleResponse(Request response) {
                            if (response instanceof CreateServiceName
                                    && !((CreateServiceName) response)
                                    .isFailed())
                                System.out.println(this
                                        + " created name " + name);
                            else
                                System.out.println(this
                                        + " failed to create name " + name);
                        }
                    });
        } else {
            client.sendRequest(
                    ReplicableClientRequest.wrap(new AppRequest(name,
                            queries[queryIdx],
                            AppRequest.PacketType.DEFAULT_APP_REQUEST,
                            false)),
                    new RequestCallback() {
                        @Override
                        public void handleResponse(Request response) {
                            System.out.println("Response: " + response);
                        }
                    }
            );
        }
        Thread.sleep(1000);
        client.close();
    }

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        try {
            if (stringified.equals(Request.NO_OP)) {
                return new AppRequest(null, 0, 0, Request.NO_OP,
                        AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
            }
            return new AppRequest(new JSONObject(stringified));
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        Set<IntegerPacketType> copy = new HashSet<IntegerPacketType>(Arrays.asList(AppRequest.PacketType.values()));
        return copy;
    }
}
