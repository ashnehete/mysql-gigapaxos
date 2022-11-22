package in.ashnehete.gigapaxos.mysql;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableAppInfo;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationPolicyTest;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

/**
 * This policy simply move a service between two servers.
 * For example, there are two replicas, AR0 and AR1, the service is first created on both of them with replica_all policy
 * Then the service will move as follows upon every request you send:
 * AR0 -> AR1 -> AR0 -> AR1 -> ...
 */
public class TutorialDemandProfile extends AbstractDemandProfile {
    protected enum Keys {
        NAME, STATS, RATE, NREQS, NTOTREQS, SRC
    };

    /**
     * The minimum number of requests after which a demand report will be sent
     * to reconfigurators.
     */
    protected static int minRequestsBeforeDemandReport = 3;
    /**
     * The minimum amount of time (ms) that must elapse since the previous
     * reconfiguration before the next reconfiguration can happen.
     */
    protected static long minReconfigurationInterval = 5000;
    /**
     * The minimum number of requests between two successive reconfigurations.
     */
    protected static long minRequestsBeforeReconfiguration = minRequestsBeforeDemandReport;

    protected double interArrivalTime = 0.0;
    protected long lastRequestTime = 0;
    protected int numRequests = 0;
    protected int numTotalRequests = 0;
    protected String srcIpAddr = "";

    // Needed only at reconfigurators, so we don't need to serialize this.
    protected TutorialDemandProfile lastReconfiguredProfile = null;

    /**
     * The string argument {@code name} is the service name for which this
     * demand profile is being maintained.
     *
     * @param name
     */
    public TutorialDemandProfile(String name) {
        super(name);
    }

    /**
     * Deep copy constructor. This constructor should create a copy of the
     * supplied DemandProfile argument {@code dp} such that the newly
     * constructed DemandProfile instance dpCopy != dp but dpCopy.equals(dp).
     *
     * @param dp
     */
    public TutorialDemandProfile(TutorialDemandProfile dp) {
        super(dp.name);
        this.interArrivalTime = dp.interArrivalTime;
        this.lastRequestTime = dp.lastRequestTime;
        this.numRequests = dp.numRequests;
        this.numTotalRequests = dp.numTotalRequests;
        this.srcIpAddr = dp.srcIpAddr;
    }

    /**
     * All {@link AbstractDemandProfile} instances must be constructible from a
     * JSONObject.
     *
     * @param json
     * @throws JSONException
     */
    public TutorialDemandProfile(JSONObject json) throws JSONException {
        super(json.getString(Keys.NAME.toString()));
        this.interArrivalTime = 1.0 / json.getDouble(Keys.RATE.toString());
        this.numRequests = json.getInt(Keys.NREQS.toString());
        this.numTotalRequests = json.getInt(Keys.NTOTREQS.toString());
        this.srcIpAddr = json.getString(Keys.SRC.toString());
    }

    /**
     *
     * @param name
     * @return All {@link AbstractDemandProfile} instances must support a single
     *         String argument constructor that is the underlying service name.
     */
    public static TutorialDemandProfile createDemandProfile(String name) {
        return new TutorialDemandProfile(name);
    }

    /**
     * This method is used to inform the reconfiguration policy that
     * {@code request} received from a client at IP address {@code sender}. The
     * parameter {@code nodeConfig} provides the list of all active replica
     * locations. The reconfiguration policy may use this information to
     * assimilate a demand distribution and use that to determine whether and
     * how to reconfigure the current set of replicas.
     *
     * The simplistic example below ignores the {@code sender} information that
     * in general is needed to determine the geo-distribution of demand.
     */
    @Override
    public boolean shouldReportDemandStats(Request request, InetAddress sender,
                                           ReconfigurableAppInfo nodeConfig) {
        System.out.println(">>>> About to send report for request "+request+", sender:"+sender);

        // incorporate request
        if (!request.getServiceName().equals(this.name))
            return false;
        this.numRequests++;
        this.numTotalRequests++;
        long iaTime = 0;
        if (lastRequestTime > 0) {
            iaTime = System.currentTimeMillis() - this.lastRequestTime;
            this.interArrivalTime = Util
                    .movingAverage(iaTime, interArrivalTime);
        } else
            lastRequestTime = System.currentTimeMillis(); // initialization

        // determine whether to send demand report
        if (DISABLE_RECONFIGURATION)
            return false;
        if (getNumRequests() >= minRequestsBeforeDemandReport){
            if (sender.toString() != this.srcIpAddr) {
                this.srcIpAddr = sender.toString();
                System.out.println(">>>>>>>>>>>> Let's report to reconfigurator! Request "
                        +request+" FROM a remote sender:"+sender);
                return true;
            }
        }
        return false;

    }

    /**
     * @return Request rate for the service name.
     */
    public double getRequestRate() {
        return this.interArrivalTime > 0 ? 1.0 / this.interArrivalTime
                : 1.0 / (1000 * 1000 * 1000);
    }

    /**
     * @return Number of requests for this service name since the most recent
     *         demand report was sent to reconfigurators.
     */
    public double getNumRequests() {
        return this.numRequests;
    }

    /**
     * @return Total number of requests for this service name.
     */
    public double getNumTotalRequests() {
        return this.numTotalRequests;
    }

    private String getSrcIpAddr() {
        return this.srcIpAddr;
    }

    private static final boolean DISABLE_RECONFIGURATION = Config
            .getGlobalBoolean(RC.DISABLE_RECONFIGURATION);

    @Override
    public JSONObject getDemandStats() {
        JSONObject json = new JSONObject();
        try {
            json.put(Keys.NAME.toString(), this.name);
            json.put(Keys.RATE.toString(), getRequestRate());
            json.put(Keys.NREQS.toString(), getNumRequests());
            json.put(Keys.NTOTREQS.toString(), getNumTotalRequests());
            json.put(Keys.SRC.toString(), getSrcIpAddr());
        } catch (JSONException je) {
            je.printStackTrace();
        }
        System.out.println("Prepare a demand profile:"+json.toString());
        return json;
    }

    @Override
    public void combine(AbstractDemandProfile dp) {
        TutorialDemandProfile update = (TutorialDemandProfile) dp;

        this.lastRequestTime = Math.max(this.lastRequestTime,
                update.lastRequestTime);
        this.interArrivalTime = Util.movingAverage(update.interArrivalTime,
                this.interArrivalTime, update.getNumRequests());
        this.numRequests += update.numRequests;
        this.numTotalRequests += update.numTotalRequests;
        this.srcIpAddr = update.srcIpAddr;

        System.out.println(">>>>>>>>> reconfigurator knows a new sender from "+update.srcIpAddr );
    }

    @Override
    public Set<String> reconfigure(Set<String> curActives,
                                   ReconfigurableAppInfo nodeConfig) {
        System.out.println(">>>>>>>>>> Consider to reconfigure the service, current actives:"+curActives);
        System.out.println(">>>>>>>>>> All actives:"+nodeConfig.getAllActiveReplicas().keySet()
                +" corresponding to IP addresses "+nodeConfig.getAllActiveReplicas().values());

        if (this.lastReconfiguredProfile != null) {
            if (System.currentTimeMillis()
                    - this.lastReconfiguredProfile.lastRequestTime < minReconfigurationInterval)
                return null;
            if (this.numTotalRequests
                    - this.lastReconfiguredProfile.numTotalRequests < minRequestsBeforeReconfiguration)
                return null;
        }

        String curNode = curActives.iterator().next();
        Set<String> retval = new HashSet<String>();
        for (String nodeID : nodeConfig.getAllActiveReplicas().keySet()){
            if(!nodeID.equals(curNode))
                retval.add(nodeID);
        }

        System.out.println(">>>>>>>>>> To configure the service to the set of actives:"+retval+"\n");
        return retval;
    }

    @Override
    public void justReconfigured() {
        // deep copy
        this.lastReconfiguredProfile = new TutorialDemandProfile(this);
        // this.clone();
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        ReconfigurationPolicyTest.testPolicyImplementation(TutorialDemandProfile.class);
    }
}
