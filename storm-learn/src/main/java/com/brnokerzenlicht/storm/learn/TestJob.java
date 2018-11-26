package com.brnokerzenlicht.storm.learn;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

public class TestJob {

    public static void main(String[] args) {

        try {
            PrintStream out = new PrintStream(new FileOutputStream("logs/storm.txt"));
            System.setErr(out);
            System.err.println(Thread.currentThread().getName() + " begin");
            TopologyBuilder builder = new TopologyBuilder();

            // 设置spout
            builder.setSpout("spoutName", new TestSpout(), 1);

            // 设置windowBolt
            builder.setBolt("windowBolt", new TestWindowBolt().withWindow(BaseWindowedBolt.Duration.of(2000),
                    BaseWindowedBolt.Duration.of(1000)), 1).localOrShuffleGrouping("spoutName");

            // 设置bolt
            builder.setBolt("bolt", new TestBolt(), 1).localOrShuffleGrouping("windowBolt");
            Config config = new Config();
            if (args != null && args.length == 1) {
                // 集群运行
                StormSubmitter.submitTopology(args[0], null, builder.createTopology());
            }
            else {
                LocalCluster cluster = new LocalCluster();
                config.put(Config.TOPOLOGY_DEBUG, true);
                config.put(Config.TOPOLOGY_ACKER_EXECUTORS, 1);
                cluster.submitTopology("local", config, builder.createTopology());
                TimeUnit.SECONDS.sleep(5);
                cluster.shutdown();
                System.err.println(Thread.currentThread().getName() + " end");
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
