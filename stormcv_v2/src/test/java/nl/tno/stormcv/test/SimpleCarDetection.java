package nl.tno.stormcv.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.BasicConfigurator;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.fetcher.VideoStreamFetcher;
import nl.tno.stormcv.operation.CascadeClassifierOperation;
import nl.tno.stormcv.operation.OpenCVOperation;
import nl.tno.timeseries.channels.ChannelSpout;
import nl.tno.timeseries.channels.SingleOperationChannelBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class SimpleCarDetection {

	public static void main(String[] args) {
		//BasicConfigurator.configure(); // used to enable log4j
		StormCVConfig conf = new StormCVConfig();
		
		conf.setNumWorkers(6); // typically set to the number of this topology requires
		//conf.setMaxSpoutPending(0); // maximum un-acked/un-failed tuples per spout (spout blocks if this number is reached)
		//conf.put(StormCVConfig., value)
		conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true); // True if Storm should timeout messages or not.
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS , 60); // The maximum amount of time given to the topology to fully process a message emitted by a spout (default = 30)
		conf.put(ChannelSpout.TOPOLOGY_FAULT_TOLERANT, false); // TTL (seconds) for all elements in all caches throughout the topology 
		conf.put(ChannelSpout.TOPOLOGY_TUPLECACHE_MAX_SIZE, 500); // maximum number of elements in the caches within the topology
		conf.put(OpenCVOperation.LIBNAME_CONFIG_KEY, "mac64_opencv_java248.dylib");
		List<String> locations = new ArrayList<String>();
		locations.add("rtsp://streaming3.webcam.nl:1935/n224/n224.stream");

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new ChannelSpout(conf,new VideoStreamFetcher(locations).frameSkip(13).sleep(450)), 1);
		
		builder.setBolt("detection", new SingleOperationChannelBolt(conf, new CascadeClassifierOperation("car", "A12_35_5_Re_A58_37_1_Li_84videos_sun.xml").minSize(5, 5).maxSize(75, 75).outputFrame(true)), 2)
			.shuffleGrouping("spout");
		
		try {
			// run in local mode
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("streamer", conf, builder.createTopology());
			Utils.sleep(20*60*1000);
			cluster.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
