package nl.tno.timeseries.testapp;

import nl.tno.timeseries.batchers.NumberOfParticlesBatcher;
import nl.tno.timeseries.stormcomponents.ChannelBolt;
import nl.tno.timeseries.stormcomponents.ChannelGrouperBolt;
import nl.tno.timeseries.timer.TimerChannelSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TestRunner {	
	private static final String topologyName = "timeserieslib-tester";
	private long sleeptime = 5000;

	public void run() {
		LocalCluster localCluster = new LocalCluster();
		Config config = new Config();
		TopologyBuilder builder = new TopologyBuilder();

/*
		builder.setSpout("input", new TimerChannelSpout(new MyFetcher(), true, 1L));
		builder.setBolt("src", new ChannelBolt(MyOperation.class), 1).shuffleGrouping("input");
		builder.setBolt("src", new ChannelBolt(MyOperation.class), 1).shuffleGrouping("input");
		builder.setBolt("src", new ChannelBolt(NumberOfParticlesBatcher.class, MyBatchOperation.class), 1).shuffleGrouping("input");
		builder.setBolt("src", new ChannelBolt(NumberOfParticlesBatcher.class, MyBatchOperation.class), 1).shuffleGrouping("input");
*/
		builder.setSpout("input", new TimerChannelSpout(new MyGroupFetcher(), true, 1L));
		builder.setBolt("grouper", new ChannelGrouperBolt(new MyChannelGrouper()), 1).shuffleGrouping("input");
		builder.setBolt("src", new ChannelBolt(NumberOfParticlesBatcher.class, MyBatchOperation.class), 1).shuffleGrouping("grouper");
//		builder.setBolt("src", new ChannelBolt(MyOperation.class), 1).shuffleGrouping("input");

		
		localCluster.submitTopology(topologyName, config, builder.createTopology());
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
	
	
	public static void main(String[] args) throws Exception{
		TestRunner testRunner = new TestRunner();
		testRunner.run();
	}
}