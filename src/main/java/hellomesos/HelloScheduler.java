package hellomesos;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public class HelloScheduler implements Scheduler {

    private static final Logger logger = LoggerFactory.getLogger(HelloScheduler.class);

    private final String message;
    private final int desiredHellos;
    private final List<String> pendingHellos = new ArrayList<>();
    private final List<String> runningHellos = new ArrayList<>();
    private final AtomicInteger taskIDGenerator = new AtomicInteger();
    private Protos.FrameworkID frameworkID;
    private int completedHellos = 0;

    public HelloScheduler(final String message, final int desiredHellos) {
        this.message = message;
        this.desiredHellos = desiredHellos;
    }

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId,
                           Protos.MasterInfo masterInfo) {
        logger.info("registered() master={}:{}, framework={}",
                masterInfo.getIp(), masterInfo.getPort(), frameworkId);
        this.frameworkID = frameworkId;
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
        logger.info("reregistered()");
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        logger.info("resourceOffers() with {} offers", offers.size());

        for (final Protos.Offer offer : offers) {
            final List<Protos.TaskInfo> tasks = new ArrayList<>();
            if (completedHellos < desiredHellos) {
                final Protos.TaskID taskID = Protos.TaskID.newBuilder()
                        .setValue(Integer.toString(taskIDGenerator.incrementAndGet()))
                        .build();

                logger.info("Launching task {}", taskID.getValue());
                pendingHellos.add(taskID.getValue());

                final Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
                        .setName("task " + taskID.getValue())
                        .setTaskId(taskID)
                        .setSlaveId(offer.getSlaveId())
                        .addResources(
                                Protos.Resource.newBuilder()
                                        .setName("cpus")
                                        .setType(Protos.Value.Type.SCALAR)
                                        .setScalar(
                                                Protos.Value.Scalar.newBuilder()
                                                        .setValue(1)))
                        .addResources(
                                Protos.Resource.newBuilder()
                                .setName("mem")
                                .setType(Protos.Value.Type.SCALAR)
                                .setScalar(
                                        Protos.Value.Scalar.newBuilder().setValue(128)))
                        .setCommand(Protos.CommandInfo.newBuilder()
                                .setValue("/bin/echo " + this.message))
                        .build();

                tasks.add(task);
            }
            final Protos.Filters filters = Protos.Filters.newBuilder()
                    .setRefuseSeconds(1).build();
            driver.launchTasks(Collections.singleton(offer.getId()), tasks, filters);
        }
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
        logger.info("offerRescinded()");
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        final String taskID = status.getTaskId().getValue();

        logger.info("statusUpdate() task {} is in state {}",
                taskID, status.getState());

        switch (status.getState()) {
            case TASK_RUNNING:
                pendingHellos.remove(taskID);
                runningHellos.add(taskID);
                break;
            case TASK_FAILED:
            case TASK_FINISHED:
                pendingHellos.remove(taskID);
                runningHellos.remove(taskID);
                completedHellos += 1;
                break;
        }

        logger.info("Number of instances: pending={}, running={}",
                pendingHellos.size(), runningHellos.size());
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver,
                                 Protos.ExecutorID executorId,
                                 Protos.SlaveID slaveId, byte[] data) {
        logger.info("frameworkMessage()");
    }

    @Override
    public void disconnected(SchedulerDriver driver) {
        logger.info("disconnected()");
    }

    @Override
    public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
        logger.info("slaveLost()");
    }

    @Override
    public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId,
                             Protos.SlaveID slaveId, int status) {
        logger.info("executorLost()");
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
        logger.info("error {}", message);
    }
}
