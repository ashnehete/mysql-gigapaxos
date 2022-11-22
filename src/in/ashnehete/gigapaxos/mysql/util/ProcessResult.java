package in.ashnehete.gigapaxos.mysql.util;

public class ProcessResult {

    final private String result;
    final private int ret;

    public ProcessResult(String result, int ret) {
        this.result = result;
        this.ret = ret;
    }

    public ProcessResult(String result) {
        this(result, 0);
    }

    public String getResult() {
        return this.result;
    }

    public int getRetCode() {
        return this.ret;
    }

    @Override
    public String toString() {
        return "return code:"+ret+"\nresult:"+result;
    }
}
