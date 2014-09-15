package nl.tno.timeseries.testapp;

import nl.tno.timeseries.batchers.NumberOfParticlesBatcher;
import nl.tno.timeseries.channels.ChannelGrouperBolt;
import nl.tno.timeseries.channels.ChannelSpout;
import nl.tno.timeseries.channels.MultipleOperationChannelBolt;
import nl.tno.timeseries.channels.SingleOperationChannelBolt;
import nl.tno.timeseries.timer.TimerChannelSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TestRunner {
	private static final String topologyName = "timeserieslib-tester";
	private final long sleeptime = 50000000;

	public void run() {
		LocalCluster localCluster = new LocalCluster();
		TopologyBuilder builder = new TopologyBuilder();
		Config config = new Config();
		// om zookeeper in te stellen browse naar:
		// http://134.221.210.122:8080/exhibitor/v1/ui/index.html
		config.put("config.zookeeper.connectionstring", "134.221.210.122:2181");
		config.put("config.zookeeper.topologyname", "test");

		// multipleOperationTopolgyTest(builder, config);
		// timerTopolgyTest(builder, config);
		// batchTopolgyTest(builder, config);
		// timedBatchTopolgyTest(builder, config);
		// groupedTopolgyTest(builder, config);
		// basicConfigTopolgyTest(builder, config);
		singleOperationTopolgyTest(builder, config);

		localCluster.submitTopology(topologyName, config,
				builder.createTopology());
		System.out.println("Topology " + topologyName + " submitted");

		boolean running = true;
		while (running) {
			try {
				Thread.sleep(sleeptime);
				running = false;
			} catch (InterruptedException e) {
			}
		}

		System.out.println("Shutdown");
		localCluster.killTopology(topologyName);
		localCluster.shutdown();
		System.exit(0);

	}

	private void multipleOperationTopolgyTest(TopologyBuilder builder,
			Config config) {
		System.out.println("Multiple operation topology test");
		builder.setSpout("input", new ChannelSpout(config, new MyFetcher()));
		builder.setBolt("src",
				new MultipleOperationChannelBolt(config, MyOperation.class), 1)
				.shuffleGrouping("input");
	}

	private void timerTopolgyTest(TopologyBuilder builder, Config config) {
		System.out.println("timer topology test");
		builder.setSpout("input", new TimerChannelSpout(config,
				new MyFetcher(), true, 1000L));
		builder.setBolt(
				"src",
				new MultipleOperationChannelBolt(config, MyTimedOperation.class),
				1).shuffleGrouping("input");
	}

	private void batchTopolgyTest(TopologyBuilder builder, Config config) {
		System.out.println("Batch topology test");
		builder.setSpout("input", new ChannelSpout(config, new MyFetcher()));
		builder.setBolt(
				"src",
				new MultipleOperationChannelBolt(config,
						NumberOfParticlesBatcher.class, MyBatchOperation.class),
				1).shuffleGrouping("input");
	}

	private void timedBatchTopolgyTest(TopologyBuilder builder, Config config) {
		System.out.println("timer batch topology test");
		builder.setSpout("input", new TimerChannelSpout(config,
				new MyFetcher(), true, 1000L));
		builder.setBolt(
				"src",
				new MultipleOperationChannelBolt(config,
						NumberOfParticlesBatcher.class,
						MyTimedBatchOperation.class), 1).shuffleGrouping(
				"input");
	}

	private void groupedTopolgyTest(TopologyBuilder builder, Config config) {
		System.out.println("Grouped topology test");
		builder.setSpout("input",
				new ChannelSpout(config, new MyGroupFetcher()));
		builder.setBolt("grouper",
				new ChannelGrouperBolt(config, new MyChannelGrouper()), 1)
				.shuffleGrouping("input");
		builder.setBolt("src",
				new MultipleOperationChannelBolt(config, MyOperation.class), 1)
				.shuffleGrouping("grouper");
	}

	private void gracefullShutdownTopolgyTest(TopologyBuilder builder,
			Config config) {
		System.out.println("GracefullShutdown topology test");
		builder.setSpout("input", new ChannelSpout(config,
				new MyGracefullShutdownFetcher()));
		builder.setBolt(
				"src",
				new MultipleOperationChannelBolt(config,
						MyGracefullShutdownOperation.class), 1)
				.shuffleGrouping("input");
	}

	private void basicConfigTopolgyTest(TopologyBuilder builder, Config config) {
		System.out.println("Basic config topology test");
		builder.setSpout("input", new ChannelSpout(config,
				new MyConfigFetcher()));
		builder.setBolt(
				"src",
				new MultipleOperationChannelBolt(config,
						MyConfigOperation.class), 1).shuffleGrouping("input");
	}

	private void singleOperationTopolgyTest(TopologyBuilder builder,
			Config config) {
		System.out.println("Single operation topology test");
		builder.setSpout("input", new ChannelSpout(config, new MyFetcher()));
		builder.setBolt("src",
				new SingleOperationChannelBolt(config, MyOperation.class), 1)
				.shuffleGrouping("input");
	}

	public static void main(String[] args) throws Exception {
		TestRunner testRunner = new TestRunner();
		testRunner.run();
	}
}
