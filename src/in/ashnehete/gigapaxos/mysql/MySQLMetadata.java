package in.ashnehete.gigapaxos.mysql;

public class MySQLMetadata {
    final static String CHECKPOINT_DIR = "checkpoint/";
    String database;
    String host;
    int port;
    String user;
    String password;

    public MySQLMetadata(String database, String host, int port, String user, String password) {
        this.database = database;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    public MySQLMetadata(String database, String host, int port) {
        this(database, host, port, "root", null);
    }

    public String getConnectionUrl() {
        return "jdbc:mysql://" + this.host + "/" + this.database + "?useSSL=false";
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
