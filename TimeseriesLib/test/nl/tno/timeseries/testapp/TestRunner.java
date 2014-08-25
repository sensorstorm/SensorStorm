package nl.tno.timeseries.testapp;

import nl.tno.timeseries.channels.ChannelBolt;
import nl.tno.timeseries.channels.ChannelSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TestRunner {
	private static final String topologyName = "timeserieslib-tester";
	private final long sleeptime = 5000;

	private void basicTopolgyTest(TopologyBuilder builder, Config config) {
		System.out.println("Basic topoly test");
		builder.setSpout("input", new ChannelSpout(config, new MyFetcher()));
		builder.setBolt("src", new ChannelBolt(config, MyOperation.class), 1)
				.shuffleGrouping("input");

	}

	public void run() {
		LocalCluster localCluster = new LocalCluster();
		Config config = new Config();
		TopologyBuilder builder = new TopologyBuilder();

		/*
		 * builder.setSpout("input", new TimerChannelSpout(new MyFetcher(),
		 * true, 1L)); builder.setBolt("src", new
		 * ChannelBolt(MyOperation.class), 1).shuffleGrouping("input");
		 * builder.setBolt("src", new ChannelBolt(MyOperation.class),
		 * 1).shuffleGrouping("input"); builder.setBolt("src", new
		 * ChannelBolt(NumberOfParticlesBatcher.class, MyBatchOperation.class),
		 * 1).shuffleGrouping("input"); builder.setBolt("src", new
		 * ChannelBolt(NumberOfParticlesBatcher.class, MyBatchOperation.class),
		 * 1).shuffleGrouping("input");
		 */
		/*
		 * builder.setSpout("input", new ChannelSpout(config, new
		 * MyGroupFetcher())); // builder.setSpout("input", new
		 * TimerChannelSpout(config, // new MyGroupFetcher(), true, 1L));
		 * builder.setBolt("grouper", new ChannelGrouperBolt(config, new
		 * MyChannelGrouper()), 1) .shuffleGrouping("input");
		 * builder.setBolt("src", new ChannelBolt(config, MyOperation.class), 1)
		 * .shuffleGrouping("grouper");
		 */

		basicTopolgyTest(builder, config);

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

	public static void main(String[] args) throws Exception {
		TestRunner testRunner = new TestRunner();
		testRunner.run();
	}
}
