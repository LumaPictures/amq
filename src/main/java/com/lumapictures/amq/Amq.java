package com.lumapictures.amq;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.Set;
import java.util.TreeSet;


/**
 * General-purpose ActiveMQ management/monitoring utility
 */
public class Amq {
    private static final Logger log = Logger.getLogger(Amq.class);

    public static void main(String[] args) throws Exception {
        // Parse arguments
        ArgumentParser parser = ArgumentParsers.newArgumentParser("amq")
                .defaultHelp(true)
                .description("Monitors and manages ActiveMQ queues");

        parser.addArgument("command")
                .choices("purge", "monitor", "list")
                .help("Command to run");
        parser.addArgument("-b", "--broker-name")
                .dest("brokerName")
                .type(String.class)
                .help("Broker name");
        parser.addArgument("-q", "--queue-name")
                .dest("queueName")
                .type(String.class)
                .help("Queue name");
        parser.addArgument("-jp", "--jmx-port")
                .dest("jmxPort")
                .type(Integer.class)
                .setDefault(1099)
                .help("JMX port");
        parser.addArgument("-jh", "--jmx-host")
                .dest("jmxHost")
                .type(String.class)
                .setDefault("localhost")
                .help("JMX host");

        Namespace config;
        try {
            config = parser.parseArgs(args);
        }
        catch (ArgumentParserException ex) {
            parser.handleError(ex);
            return;
        }

        // Configure logger
        BasicConfigurator.configure();

        JMXServiceURL url = new JMXServiceURL(
                String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi",
                        config.getString("jmxHost"),
                        config.getInt("jmxPort")));

        JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
        MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

        ObjectName mbeanName = new ObjectName(String.format(
                "org.apache.activemq:type=Broker,brokerName=%s,destinationType=Queue,destinationName=%s",
                config.getString("brokerName"),
                config.getString("queueName")));

        // MBean representing queue
        QueueViewMBean mbeanProxy = JMX.newMBeanProxy(mbsc, mbeanName, QueueViewMBean.class, true);

        String command = config.getString("command");
        if (command.equals("purge")) {
            purgeQueue(mbeanProxy);
        }
        else if (command.equals("monitor")) {
            monitorQueue(mbeanProxy, config);
        }
        else if (command.equals("list")) {
            throw new UnsupportedOperationException();
//            QueryExp query = Query.and(
//                Query.and(
//                        Query.eq(Query.attr("type"), Query.value("Broker")),
//                        Query.eq(Query.attr("destinationType"), Query.value("Queue"))
//                ),
//                Query.eq(Query.attr("brokerName"), Query.value(config.getString("brokerName")))
//            );
//
//            Set<ObjectName> names = new TreeSet<ObjectName>(mbsc.queryNames(null, query));
//            for (ObjectName name : names) {
//                name.getKeyProperty("destinationName");
//                log.info(name.toString());
//            }
        }
        else {
            log.error("Invalid command: " + command);
        }
    }

    /**
     * Monitors the queue
     */
    public static void monitorQueue(QueueViewMBean mbeanProxy, Namespace config) {
        Long lastQueueSize = null;
        while (true) {
            long queueSize = mbeanProxy.getQueueSize();
            if (lastQueueSize == null || lastQueueSize != queueSize) {
                log.info(String.format("Queue size: %d", queueSize));
                lastQueueSize = queueSize;
            }
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException ex) {
                log.warn("Monitor interrupted!");
                break;
            }
        }
    }

    /**
     * Purges the queue contents
     */
    public static void purgeQueue(QueueViewMBean mbeanProxy) {
        long queueSize = mbeanProxy.getQueueSize();
        try {
            mbeanProxy.purge();
            log.info(String.format("Purged %d items from queue", queueSize));
        }
        catch (Exception ex) {
            log.error("Unable to purge queue: " + ex.getMessage(), ex);
        }
    }
}
