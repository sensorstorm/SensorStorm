package nl.tno.sensorstorm.example;

import nl.tno.sensorstorm.stormcomponents.SensorStormBolt;
import nl.tno.sensorstorm.stormcomponents.SensorStormSpout;
import nl.tno.sensorstorm.stormcomponents.groupers.SensorStormFieldGrouping;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class Main {

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_DEBUG, false);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new SensorStormSpout(conf,
				new BlockFetcher(), true, 5000), 1);
		builder.setBolt(
				"bolt",
				new SensorStormBolt(conf, 1000, WindowBatcher.class,
						AverageOperation.class, "sensorId"), 3).customGrouping(
				"spout", new SensorStormFieldGrouping("sensorId"));

		if ((args != null) && (args.length > 0)) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
					builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}

}
