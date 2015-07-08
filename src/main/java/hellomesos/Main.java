package hellomesos;

import com.google.protobuf.ByteString;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;

public class Main {

    private static void usage() {
        final String name = Main.class.getName();
        System.err.println("Usage: " + name +
                " <master-ip>:<master-port> <message> <nr-of-messages>");
    }

    public static void main(String[] args) {

        if (args.length != 3) {
            usage();
            System.exit(1);
        }

        final int frameworkFailoverTimeout = 30;

        final Protos.FrameworkInfo.Builder frameworkBuilder =
                Protos.FrameworkInfo.newBuilder()
                .setName("HelloMesos")
                .setUser("")
                .setFailoverTimeout(frameworkFailoverTimeout);

        final String message = args[1];
        final int messageCount = Integer.parseInt(args[2]);

        final Scheduler helloScheduler = new HelloScheduler(message, messageCount);

        MesosSchedulerDriver driver;
        if (System.getenv("MESOS_AUTHENTICATE") != null) {
            System.out.println("Enabling authentication for the framework");

            final String defaultPrincipal = System.getenv("DEFAULT_PRINCIPAL");
            if (defaultPrincipal == null) {
                System.err.println(
                        "Expecting authentication principal in the environment");
                System.exit(1);
            }

            final String defaultSecret = System.getenv("DEFAULT_SECRET");
            if (defaultSecret == null) {
                System.err.println(
                        "Expecting authentication secret in the environment");
                System.exit(1);
            }

            Protos.Credential credential = Protos.Credential.newBuilder()
                    .setPrincipal(defaultPrincipal)
                    .setSecret(ByteString.copyFrom(defaultSecret.getBytes()))
                    .build();

            frameworkBuilder.setPrincipal(defaultPrincipal);

            driver = new MesosSchedulerDriver(helloScheduler,
                    frameworkBuilder.build(), args[0], credential);
        } else {
            frameworkBuilder.setPrincipal("test-framework-java");
            driver = new MesosSchedulerDriver(helloScheduler,
                    frameworkBuilder.build(), args[0]);
        }

        final int status = driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1;

        // Ensure that the driver process terminates.
        driver.stop();

        System.exit(status);
    }
}
