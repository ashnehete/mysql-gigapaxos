package in.ashnehete.gigapaxos.mysql;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.ClientMessenger;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.SSLMessenger;
import edu.umass.cs.reconfiguration.examples.AbstractReconfigurablePaxosApp;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.examples.noop.NoopApp;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import in.ashnehete.gigapaxos.mysql.util.ProcessResult;
import in.ashnehete.gigapaxos.mysql.util.ProcessRuntime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.stream.IntStream;

public class MySQLApp extends AbstractReconfigurablePaxosApp<String> implements Replicable, Reconfigurable, ClientMessenger {

    MySQLMetadata metadata = null;
    String myID = null;
    String serviceName = null;
    String gatewayIPAddress = null;

    List<String> env = new ArrayList<>();

    HashMap<String, MySQLMetadata> mysqlInstances = new HashMap<>();

    public MySQLApp(String[] args) {
        MySQLMetadata metadataAR0 = new MySQLMetadata("test", "127.0.0.1", 3306);
        MySQLMetadata metadataAR1 = new MySQLMetadata("test", "127.0.0.1", 3307);
        MySQLMetadata metadataAR2 = new MySQLMetadata("test", "127.0.0.1", 3308);
        this.mysqlInstances.put("AR0", metadataAR0);
        this.mysqlInstances.put("AR1", metadataAR1);
        this.mysqlInstances.put("AR2", metadataAR2);

        env.add("MYSQL_ALLOW_EMPTY_PASSWORD=true");
        env.add("MYSQL_DATABASE=test");

        gatewayIPAddress = "172.17.0.1";
        if (System.getProperty("gateway") != null)
            gatewayIPAddress = System.getProperty("gateway");
    }

    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
        this.log(request.toString());
        this.log("execute:" + this.myID);
        MySQLMetadata metadata = mysqlInstances.get(this.myID);

        if (request instanceof AppRequest) {
            if (((AppRequest) request).isStop())
                return true;
            String requestValue = ((AppRequest) request).getValue();

            try {
                JSONObject json = new JSONObject(requestValue);
                String query = json.getString("query");

                String connectionUrl = metadata.getConnectionUrl();
                Class.forName("com.mysql.cj.jdbc.Driver");
                Connection connect = DriverManager.getConnection(connectionUrl, "root", null);

                if (connect != null) {
                    Statement statement = connect.createStatement();
                    boolean executed = statement.execute(query);
                    if (executed) {
                        ResultSet rs = statement.getResultSet();
                        String response = this.getJsonFromResultSet(rs);
                        this.log("ResultSet: " + response);
                        ((AppRequest) request).setResponse(response);
                    } else {
                        ((AppRequest) request).setResponse("false");
                    }

                } else {
                    return false;
                }
            } catch (JSONException | ClassNotFoundException | SQLException e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    @Override
    public boolean execute(Request request) {
        return this.execute(request, false);
    }

    @Override
    public String checkpoint(String name) {
        this.log("Checkpoint:" + name);
        MySQLMetadata metadata = mysqlInstances.get(this.myID);
        String dest = MySQLMetadata.CHECKPOINT_DIR + this.myID + ".sql";
        List<String> command = this.getMySQLCheckpointCommand(metadata.getHost(), metadata.getPort(),
                metadata.getUser(), metadata.getPassword(), dest);
        run(command);
        return dest;
    }

    public boolean restore(String name, String state) {
        this.log("Restore: this.serviceName " + this.serviceName);
        this.log("Restore:" + name + "," + state);
        MySQLMetadata metadata = mysqlInstances.get(this.myID);

        String dockerName = "mysql-" + this.myID;

        if (name.equals(PaxosConfig.getDefaultServiceName())) {
            // Don't do any thing, this is the default app
            return true;
        }

        if (this.serviceName != null) {
            if (state == null) {
                // delete
                this.log("Restore: end of epoch");
//                this.serviceName = null;
                run(getStopCommand(dockerName));
            } else {
                // restore from non-empty state
                this.log("restore from non-empty state");
//                List<String> stopCommand = getStopCommand(dockerName);
//                run(stopCommand);
//                List<String> rmCommand = getRemoveCommand(dockerName);
//                run(rmCommand);
//
                run(getStartCommand(dockerName));
                try {
                    Thread.sleep(5000);
                    // restore mysqldump
                    List<String> restoreCommand = this.getMySQLCheckpointCommand(metadata.getHost(), metadata.getPort(),
                            metadata.getUser(), metadata.getPassword(), state);
                    run(restoreCommand);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } else {
            // does not exist, create from scratch
            this.log("Restore: creation");
            this.serviceName = name;
            run(getRunCommand(dockerName, 3306, metadata.getPort(), env, "mysql", null));
        }

        return true;
    }

    /**
     * Return a JSON string from ResultSet
     *
     * @param resultSet ResultSet object
     * @return
     */
    private String getJsonFromResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData md = resultSet.getMetaData();
        int numCols = md.getColumnCount();
        List<String> colNames = IntStream.range(0, numCols)
                .mapToObj(i -> {
                    try {
                        return md.getColumnName(i + 1);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        return "?";
                    }
                }).toList();

        JSONArray result = new JSONArray();
        while (resultSet.next()) {
            JSONObject row = new JSONObject();
            colNames.forEach(cn -> {
                try {
                    row.put(cn, resultSet.getObject(cn));
                } catch (JSONException | SQLException e) {
                    e.printStackTrace();
                }
            });
            result.put(row);
        }
        return result.toString();
    }

    /**
     * Needed only if app uses request types other than RequestPacket. Refer
     * {@link NoopApp} for a more detailed example.
     */
    @Override
    public Request getRequest(String stringified)
            throws RequestParseException {
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

    /**
     * Needed only if app uses request types other than RequestPacket. Refer
     * {@link NoopApp} for a more detailed example.
     */
    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        Set<IntegerPacketType> copy = new HashSet<IntegerPacketType>(Arrays.asList(AppRequest.PacketType.values()));
        return copy;
    }

    @Override
    public void setClientMessenger(SSLMessenger<?, JSONObject> messenger) {
        this.myID = messenger.getMyID().toString();
    }

    public void log(String line) {
        System.out.printf("[%s] %s\n", this.myID, line);
    }

    /* ========================= docker command ============================ */

    /**
     * Inspect a docker to get the information such as ip address
     */
    private List<String> getInspectCommand(String name) {
        List<String> command = new ArrayList<>();
        command.add("docker");
        command.add("inspect");
        command.add(name);
        return command;
    }

    private List<String> getBridgeInspectCommand() {
        List<String> command = new ArrayList<>();

        command.add("docker");
        command.add("network");
        command.add("inspect");
        // inspect the virtual bridge
        command.add("bridge");

        return command;
    }

    /**
     * Run command is the command to run a docker for the first time
     */
    // docker run --name xdn-demo-app -p 8080:3000 -e ADDR=172.17.0.1 -d oversky710/xdn-demo-app --ip 172.17.0.100
    private List<String> getRunCommand(String name, int port, int exportPort, List<String> env, String url, String vol) {
        return getRunCommand(name, port, exportPort, env, url, vol, 2.0, 8);
    }

    private List<String> getRunCommand(String name, int port, int exportPort,
                                       List<String> env, String url, String vol,
                                       double cpus, int memory) {
        List<String> command = new ArrayList<>();
        command.add("docker");
        command.add("run");

        // name is unique globally, otherwise it won't be created successfully
        command.add("--name");
        command.add(name);

        if (vol != null) {
            command.add("-v");
            command.add(vol + ":/tmp");
        }

        // FIXME: cpu and memory limit
        if (cpus > 0) {
            // command.add("--cpus=\"" + cpus + "\"");
            // command.add("--cpuset-cpus=0-3");
        }
        // command.add("-m");
        // command.add("8G");

        //FIXME: only works on cloud node
        if (port > 0) {
            command.add("-p");
            command.add(exportPort + ":" + port);
        }

        if (env != null) {
            for (String e : env) {
                command.add("-e");
                command.add(e);
            }
        }

        command.add("-e");
        command.add("HOST=" + gatewayIPAddress);

        command.add("-e");
        command.add("HOSTNAME=" + myID);

        command.add("-d");
        command.add(url);

        return command;
    }

    private List<String> getCheckpointCreateCommand(String name) {
        return getCheckpointCreateCommand(name, false);
    }

    // docker checkpoint create --leave-running=true --checkpoint-dir=/tmp/test name test
    private List<String> getCheckpointCreateCommand(String name, boolean leaveRunning) {
        List<String> command = new ArrayList<>();
        command.add("docker");
        command.add("checkpoint");
        command.add("create");
        if (leaveRunning)
            command.add("--leave-running=true");
        // command.add("--checkpoint-dir="+XDNConfig.checkpointDir+name);
        command.add(name);
        command.add(name);
        return command;
    }

    private List<String> getCheckpointRemoveCommand(String name) {
        List<String> command = new ArrayList<>();
        command.add("docker");
        command.add("checkpoint");
        command.add("rm");
        // command.add("--checkpoint-dir="+XDNConfig.checkpointDir+name);
        command.add(name);
        command.add(name);
        return command;
    }

    private List<String> getCheckpointListCommand(String name) {
        List<String> command = new ArrayList<>();
        command.add("docker");
        command.add("checkpoint");
        command.add("ls");
        // command.add("--checkpoint-dir="+XDNConfig.checkpointDir+name);
        command.add(name);

        return command;
    }

    List<String> getRemoveImageCommand(String name) {
        List<String> command = new ArrayList<>();
        command.add("docker");
        command.add("image");
        command.add("rm");
        // command.add("--checkpoint-dir="+XDNConfig.checkpointDir+name);
        command.add(name);
        return command;
    }

    /**
     * Start command is the command to start a docker when the previous docker has not been removed
     */
    // docker start --checkpoint=xdn-demo-app xdn-demo-app
    private List<String> getStartCommand(String name) {
        List<String> command = new ArrayList<>();
        command.add("docker");
        command.add("start");
        // command.add("--checkpoint-dir="+XDNConfig.checkpointDir+name);
        command.add(name);
        return command;
    }

    // docker restart xdn-demo-app
    private List<String> getRestartCommand(String name) {
        List<String> command = new ArrayList<>();
        command.add("docker");
        command.add("restart");
        // restart the docker immediately
        command.add("-t");
        command.add("0");
        command.add(name);
        return command;
    }

    // docker pull oversky710/xdn-demo-app
    private List<String> getPullCommand(String url) {
        List<String> command = new ArrayList<>();
        command.add("docker");
        command.add("pull");
        command.add(url);
        return command;
    }

    private List<String> getStopCommand(String name) {
        List<String> command = new ArrayList<>();
        command.add("docker");
        command.add("stop");
        command.add("-t");
        command.add("0"); // time to kill docker immediately
        command.add(name);
        return command;
    }

    private List<String> getRemoveCommand(String name) {
        List<String> command = new ArrayList<>();
        command.add("docker");
        command.add("rm");
        command.add(name);
        return command;
    }

    /* ================== End of docker command ==================== */

    private List<String> getMySQLDump(String host, int port, String username, String password) {
        List<String> command = new ArrayList<>();
        command.add("mysqldump");
        command.add("-u");
        command.add(username);
        if (password != null) {
            command.add("-p");
            command.add(password);
        }
        command.add("-h");
        command.add(host);
        command.add("-P");
        command.add(String.valueOf(port));
        return command;
    }

    private List<String> getMySQLCheckpointCommand(String host, int port, String username, String password, String dest) {
        List<String> command = getMySQLDump(host, port, username, password);
        command.add("--all-databases");
        command.add("--result-file=" + dest);
        return command;
    }

    private List<String> getMySQLRestoreCommand(String host, int port, String username, String password, String src) {
        List<String> command = getMySQLDump(host, port, username, password);
        command.add("<");
        command.add(src);
        return command;
    }

    private boolean run(List<String> command) {
        this.log("Command: " + command);
        ProcessResult result;
        try {
            result = ProcessRuntime.executeCommand(command);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        this.log("Command return value: " + result);
        return result.getRetCode() == 0;
    }
}
