package test;

import nl.tno.sensortimeseries.model.Measurement;
import nl.tno.sensortimeseries.model.MeasurementTimerTick;
import nl.tno.sensortimeseries.operation.SensorAlgorithmOperation;
import nl.tno.sensortimeseries.serializer.MeasurementSerializer;
import nl.tno.sensortimeseries.utils.TSSensorConfig;
import nl.tno.timeseries.bolt.SingleInputBolt;
import nl.tno.timeseries.spout.StreamableSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class SampleRateTopologyRunner {
	private static final String topologyName = "bigdavids-wp1-sensortest";
	private long sleeptime = 5000;

	
	
	public static void main(String[] args) throws Exception{
		SampleRateTopologyRunner sampleRateTopologyRunner = new SampleRateTopologyRunner();
		sampleRateTopologyRunner.run();
//		sampleRateTopologyRunner.test();
	}

	
//	private void test() {
//		Object v = new Double(1.0);
//		Object valueObject = v;
//		Class<?>  valueClass = valueObject.getClass();
//		
//		if (valueClass.isInstance(Double.class)) {
//			System.out.println("instance1");
//		}
//		if (Double.class.isInstance(valueClass)) {
//			System.out.println("instance2");
//		}
//		if (valueClass.isAssignableFrom(Double.class)) {
//			System.out.println("assignable1");
//		}
//		if (Double.class.isAssignableFrom(valueClass)) {
//			System.out.println("assignable2");
//		}
//	}
	
	private void makeSampleRateChangeabilityTest(TopologyBuilder builder, Config config) {
		System.out.println("makeSampleRateChangeabilityTest");

		sleeptime = 700000;
		config.put(TSSensorConfig.BASESENSOR_SPOUT_USE_MEASUREMENT_TIME, true);
		config.put(TSSensorConfig.BASESENSOR_SPOUT_MAIN_TIMERTICK_FREQ, 100000);
		config.put(SRCConfig.LIFEDIJK_FILENAME, "data\\export_1_sensor.csv");
		
		config.put(SRCConfig.SAMPLERATE_CHANGEABILITY_DMAX, 36010000);
		config.put(SRCConfig.SAMPLERATE_CHANGEABILITY_DMIN, 3601000);
		config.put(SRCConfig.SAMPLERATE_CHANGEABILITY_ALPHA, 1.1);


		builder.setSpout("input", new StreamableSpout(new LiveDijkFileFetcher()), 1);

		builder.setBolt("src", new SingleInputBolt(new SensorAlgorithmOperation(SampleRateChangeabilityAlgorithm.class)), 1)
		.shuffleGrouping("input");
	}

	
	private void run() {
		LocalCluster localCluster = new LocalCluster();

		SRCConfig config = new SRCConfig();
		config.setNumWorkers(4);
		config.setDebug(false);
//		config.setMaxSpoutPending(8); // maximum un-acked/un-failed tuples per spout (spout blocks if this number is reached)
//		config.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS , 120); // time after which un acked/failed tupes are failed automatically
//		config.put(SRCConfig.STORMCV_CACHES_MAX_SIZE, 100); // limits caches used throughout the topology to 100 elements (this protects against memory overflow)
//		config.put(SRCConfig.STORMCV_CACHES_TIMEOUT_SEC, 120); // TTL of elements in throughout the topology caches. If an element in the cache reaches its TTL it will be failed

		config.registerSerialization(Measurement.class, MeasurementSerializer.class);
		config.registerSerialization(MeasurementTimerTick.class, MeasurementSerializer.class);

		TopologyBuilder builder = new TopologyBuilder();
		makeSampleRateChangeabilityTest(builder, config);

		
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

}
