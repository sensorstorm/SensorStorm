package nl.tno.timeseries.testapp;

//import nl.tno.sensorstorm.operations.OperationException;
//import nl.tno.sensorstorm.stormcomponents.SensorStormBolt;
//import nl.tno.sensorstorm.stormcomponents.SensorStormSpout;
//import nl.tno.sensorstorm.stormcomponents.groupers.SensorStormFieldGrouping;
//import nl.tno.sensorstorm.stormcomponents.groupers.SensorStormShuffleGrouping;
//import nl.tno.storm.configuration.impl.ZookeeperStormConfigurationFactory;
//import backtype.storm.Config;
//import backtype.storm.LocalCluster;
//import backtype.storm.topology.TopologyBuilder;
//
//public class TestRunner {
//	private static final String topologyName = "timeserieslib-tester";
//	private final long sleeptime = 500000;
//
//	private LocalCluster localCluster;
//	private TopologyBuilder builder;
//	private Config config;
//
//	private void run() {
//
//		try {
//			prepare();
//
//			// testSingleOperationTopology();
//			// testFieldGrouperOperationTopology();
//			// testTimerTicksFieldGrouperOperationTopology();
//			// testTimerTicksSingleOperationTopology();
//			// testSensorStormFieldGroupingTopology();
//			// testSensorStormShuffleGroupingTopology();
//			testSingleOperationWithConfigTopology();
//
//			submitAndWait();
//			tearDown();
//		} catch (OperationException e) {
//			System.out.println("Can not create topolgy: " + e.getMessage());
//			System.exit(0);
//		}
//	}
//
//	public void testTimerTicksFieldGrouperOperationTopology()
//			throws OperationException {
//		System.out.println("timer fieldgrouper operation topology test");
//		builder.setSpout("input", new SensorStormSpout(config,
//				new MyFetcher(4), true, 500L));
//		builder.setBolt("src",
//				new SensorStormBolt(config, MyTimedOperation.class, "myId"), 1)
//				.shuffleGrouping("input");
//	}
//
//	public void testTimerTicksSingleOperationTopology()
//			throws OperationException {
//		System.out.println("timer single operation topology test");
//		builder.setSpout("input", new SensorStormSpout(config,
//				new MyFetcher(4), true, 500L));
//		builder.setBolt("src",
//				new SensorStormBolt(config, MyTimedOperation.class, null), 1)
//				.shuffleGrouping("input");
//	}
//
//	public void testFieldGrouperOperationTopology() throws OperationException {
//		System.out.println("Field grouper operation topology test");
//		builder.setSpout("input",
//				new SensorStormSpout(config, new MyFetcher(4)));
//		builder.setBolt("src",
//				new SensorStormBolt(config, MyOperation.class, "myId"), 2)
//				.shuffleGrouping("input");
//	}
//
//	public void testSingleOperationTopology() throws OperationException {
//		System.out.println("Single operation topology test");
//		builder.setSpout("input",
//				new SensorStormSpout(config, new MyFetcher(4)));
//		builder.setBolt("src",
//				new SensorStormBolt(config, MyOperation.class, null), 1)
//				.shuffleGrouping("input");
//	}
//
//	public void testSensorStormFieldGroupingTopology()
//			throws OperationException {
//		System.out.println("timer topology test");
//		builder.setSpout("input", new SensorStormSpout(config,
//				new MyFetcher(4), true, 500L));
//		builder.setBolt("src",
//				new SensorStormBolt(config, MyTimedOperation.class, "myId"), 2)
//				.customGrouping("input", new SensorStormFieldGrouping("myId"));
//	}
//
//	public void testSensorStormShuffleGroupingTopology()
//			throws OperationException {
//		System.out.println("timer topology test");
//		builder.setSpout("input", new SensorStormSpout(config,
//				new MyFetcher(4), true, 500L));
//		builder.setBolt("src",
//				new SensorStormBolt(config, MyTimedOperation.class, "myId"), 3)
//				.customGrouping("input", new SensorStormShuffleGrouping());
//	}
//
//	public void testSingleOperationWithConfigTopology()
//			throws OperationException {
//		System.out.println("Single operation with config topology test");
//		ZookeeperStormConfigurationFactory
//				.setExternalStormConfiguration(new MyExternalStormConfiguration());
//
//		builder.setSpout("input",
//				new SensorStormSpout(config, new MyFetcher(4)));
//		builder.setBolt("src",
//				new SensorStormBolt(config, MyConfigOperation.class, null), 1)
//				.shuffleGrouping("input");
//	}
//
//	public void prepare() {
//		localCluster = new LocalCluster();
//		builder = new TopologyBuilder();
//		config = new Config();
//		// om zookeeper in te stellen browse naar:
//		// http://134.221.210.122:8080/exhibitor/v1/ui/index.html
//		// config.put("config.zookeeper.connectionstring",
//		// "134.221.210.122:2181");
//		config.put("config.zookeeper.connectionstring",
//				"storm-zookeeper.sensorlab.tno.nl");
//		config.put("config.zookeeper.topologyname", "test");
//		// config.put(Config.TOPOLOGY_DEBUG, "true");
//	}
//
//	private void submitAndWait() {
//		localCluster.submitTopology(topologyName, config,
//				builder.createTopology());
//		System.out.println("Topology " + topologyName + " submitted");
//
//		boolean running = true;
//		while (running) {
//			try {
//				Thread.sleep(sleeptime);
//				running = false;
//			} catch (InterruptedException e) {
//			}
//		}
//	}
//
//	private void tearDown() {
//		System.out.println("Shutdown");
//		localCluster.killTopology(topologyName);
//		localCluster.shutdown();
//		System.exit(0);
//	}
//
//	/*
//	 * private void multipleOperationTopolgyTest(TopologyBuilder builder, Config
//	 * config) throws OperationException {
//	 * System.out.println("Multiple operation topology test");
//	 * builder.setSpout("input", new SensorStormSpout(config, new MyFetcher()));
//	 * builder.setBolt("src", new SensorStormBolt(config, MyOperation.class),
//	 * 1).shuffleGrouping("input"); }
//	 * 
//	 * 
//	 * private void batchTopolgyTest(TopologyBuilder builder, Config config)
//	 * throws OperationException { System.out.println("Batch topology test");
//	 * builder.setSpout("input", new SensorStormSpout(config, new MyFetcher()));
//	 * builder.setBolt( "src", new SensorStormBolt(config,
//	 * NumberOfParticlesBatcher.class, MyBatchOperation.class),
//	 * 1).shuffleGrouping("input"); }
//	 * 
//	 * private void timedBatchTopolgyTest(TopologyBuilder builder, Config
//	 * config) throws OperationException {
//	 * System.out.println("timer batch topology test");
//	 * builder.setSpout("input", new TimerSensorStormSpout(config, new
//	 * MyFetcher(), true, 1000L)); builder.setBolt( "src", new
//	 * SensorStormBolt(config, NumberOfParticlesBatcher.class,
//	 * MyTimedBatchOperation.class), 1).shuffleGrouping( "input"); }
//	 * 
//	 * private void groupedTopolgyTest(TopologyBuilder builder, Config config)
//	 * throws OperationException { System.out.println("Grouped topology test");
//	 * builder.setSpout("input", new SensorStormSpout(config, new
//	 * MyGroupFetcher())); builder.setBolt("grouper", new
//	 * ChannelGrouperBolt(config, new MyChannelGrouper()), 1)
//	 * .shuffleGrouping("input"); builder.setBolt("src", new
//	 * SensorStormBolt(config, MyOperation.class),
//	 * 1).shuffleGrouping("grouper"); }
//	 * 
//	 * private void gracefullShutdownTopolgyTest(TopologyBuilder builder, Config
//	 * config) throws OperationException {
//	 * System.out.println("GracefullShutdown topology test");
//	 * builder.setSpout("input", new SensorStormSpout(config, new
//	 * MyGracefullShutdownFetcher())); builder.setBolt( "src", new
//	 * SensorStormBolt(config, MyGracefullShutdownOperation.class),
//	 * 1).shuffleGrouping("input"); }
//	 * 
//	 * private void basicConfigTopolgyTest(TopologyBuilder builder, Config
//	 * config) throws OperationException {
//	 * System.out.println("Basic config topology test");
//	 * builder.setSpout("input", new SensorStormSpout(config, new
//	 * MyConfigFetcher())); builder.setBolt("src", new SensorStormBolt(config,
//	 * MyConfigOperation.class), 1) .shuffleGrouping("input"); }
//	 */
//
//	// public static void main(String[] args) throws Exception {
//	// TestRunner testRunner = new TestRunner();
//	// testRunner.run();
//	// }
// }
