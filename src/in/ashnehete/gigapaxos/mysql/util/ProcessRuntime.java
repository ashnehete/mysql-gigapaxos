package in.ashnehete.gigapaxos.mysql.util;

import edu.umass.cs.gigapaxos.PaxosConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class ProcessRuntime {

    private static Logger log = PaxosConfig.getLogger();

    public static ProcessResult executeCommand(List<String> command) throws IOException, InterruptedException {

        StringBuilder result
                = new StringBuilder();

        ProcessBuilder builder = new ProcessBuilder(command);

        //builder.redirectError(ProcessBuilder.Redirect.INHERIT);
        //builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        //builder.redirectInput(Redirect.INHERIT);

        Process process = builder.start();

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream()))) {

            String line;

            while ((line = reader.readLine()) != null) {
                log.fine(line);
                // System.out.println(line);
                result.append(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        int ret = process.waitFor();

        return new ProcessResult(result.toString(), ret);
    }


    public static void main(String[] args) throws IOException, InterruptedException {

        final String name = "3ffc7fe7f53a";
        List<String> command = new ArrayList<String>();
        command.add("docker");
        command.add("checkpoint");
        command.add("ls");
        command.add(name);

        executeCommand( command );
    }
}
