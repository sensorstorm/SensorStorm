package nl.tno.timeseries.channels;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.storm.configuration.api.StormConfigurationException;
import nl.tno.storm.configuration.api.ZookeeperStormConfigurationAPI;
import nl.tno.storm.configuration.impl.ZookeeperStormConfigurationFactory;
import nl.tno.timeseries.annotation.MetaParticleHandlerDecleration;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.config.EmptyStormConfiguration;
import nl.tno.timeseries.interfaces.BatchOperation;
import nl.tno.timeseries.interfaces.Batcher;
import nl.tno.timeseries.interfaces.BatcherException;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.DataParticleBatch;
import nl.tno.timeseries.interfaces.FaultTolerant;
import nl.tno.timeseries.interfaces.MetaParticle;
import nl.tno.timeseries.interfaces.Operation;
import nl.tno.timeseries.interfaces.OperationException;
import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.interfaces.SingleOperation;
import nl.tno.timeseries.mapper.ParticleMapper;
import nl.tno.timeseries.particles.EmitParticleInterface;
import nl.tno.timeseries.particles.MetaParticleHandler;
import nl.tno.timeseries.particles.MetaParticleUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class SingleOperationChannelBolt extends BaseRichBolt implements
		EmitParticleInterface, FaultTolerant {

	private static final long serialVersionUID = -7628008145368347247L;
	private final static String EMPTY_CHANNELID = "";
	private final static long EMPTY_STARTTIMESTAMP = 0;

	protected Logger logger = LoggerFactory
			.getLogger(SingleOperationChannelBolt.class);
	protected ZookeeperStormConfigurationAPI zookeeperStormConfiguration;
	protected OutputCollector collector;
	protected String boltName;
	protected int nrOfOutputFields;
	protected Fields metaParticleFields;
	protected ParticleCache cache;
	private final Operation operation;
	private Batcher batcher;
	protected boolean ackFailAndAnchor = false;
	private List<MetaParticleHandler> metaParticleHandlers;

	/**
	 * Construct a {@link SingleOperationChannelBolt} with a {@link Batcher}
	 * 
	 * @param conf
	 *            Storm configuration map
	 * @param batcherClass
	 *            {@link Class} of the {@link Batcher} implementation
	 * @param batchOperationClass
	 *            {@link Class} of the {@link BatchOperation} implementation
	 */
	public SingleOperationChannelBolt(Config config, Batcher batcher,
			BatchOperation batchOperation) {
		// Set fields
		this.operation = batchOperation;
		this.batcher = batcher;
		this.metaParticleFields = MetaParticleUtil
				.registerMetaParticleFieldsWithOperationClass(config,
						batchOperation.getClass());
	}

	/**
	 * Construct a {@link SingleOperationChannelBolt} without a {@link Batcher}
	 * 
	 * @param conf
	 *            Storm configuration map
	 * @param operationClass
	 *            {@link Class} of the {@link Operation} implementation
	 */
	public SingleOperationChannelBolt(Config config, SingleOperation operation) {
		// Set fields
		this.operation = operation;
		this.metaParticleFields = MetaParticleUtil
				.registerMetaParticleFieldsWithOperationClass(config,
						operation.getClass());
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormNativeConfig,
			TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.boltName = context.getThisComponentId();

		try {
			zookeeperStormConfiguration = ZookeeperStormConfigurationFactory
					.getInstance().getStormConfiguration(stormNativeConfig);
		} catch (StormConfigurationException e) {
			logger.error("Can not connect to zookeeper for get Storm configuration. Reason: "
					+ e.getMessage());
			zookeeperStormConfiguration = new EmptyStormConfiguration();
		}

		try {
			if (batcher != null) {
				batcher.init(EMPTY_CHANNELID, EMPTY_STARTTIMESTAMP,
						stormNativeConfig, zookeeperStormConfiguration);
			}
			if (operation != null) {
				operation.init(EMPTY_CHANNELID, EMPTY_STARTTIMESTAMP,
						stormNativeConfig, zookeeperStormConfiguration);
				createMetaParticleHandlers(operation);
			}
		} catch (InstantiationException | IllegalAccessException
				| OperationException | BatcherException e) {
			logger.error("Unable to instantiate batcher and/or operation due to: "
					+ e.getMessage());
		}

		ackFailAndAnchor = (stormNativeConfig
				.containsKey(ChannelSpout.TOPOLOGY_FAULT_TOLERANT) && (boolean) stormNativeConfig
				.get(ChannelSpout.TOPOLOGY_FAULT_TOLERANT))
				|| (stormNativeConfig.get(Config.TOPOLOGY_MAX_SPOUT_PENDING) != null && (long) stormNativeConfig
						.get(Config.TOPOLOGY_MAX_SPOUT_PENDING) > 0);

		logger.info("Acking, Failing and Anchoring enabled: "
				+ ackFailAndAnchor);

		// initiate tuple cache
		if (ackFailAndAnchor) {
			int timeout = ((Long) stormNativeConfig
					.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)).intValue();
			int maxSize = ((Long) stormNativeConfig
					.get(ChannelSpout.TOPOLOGY_TUPLECACHE_MAX_SIZE)).intValue();
			cache = new ParticleCache(maxSize, timeout, this);
		}

	}

	@Override
	public void execute(Tuple tuple) {
		Particle inputParticle = ParticleMapper.tupleToParticle(tuple);
		List<? extends Particle> outputParticles;
		if (inputParticle != null) {
			// handle metadata particle
			if (inputParticle instanceof MetaParticle) {
				outputParticles = handleMetaParticle((MetaParticle) inputParticle);
				emitParticles(tuple, outputParticles);
				this.ack(tuple);
			} else if (inputParticle instanceof DataParticle) {
				// perform batchoperation
				if (batcher != null) {
					try {
						cache.put((DataParticle) inputParticle, tuple);
						List<DataParticleBatch> batchedParticles = batcher
								.batch(cache, (DataParticle) inputParticle);
						// are there one or more batches to be sent?
						if (batchedParticles != null) {
							List<DataParticle> batchResult = new ArrayList<DataParticle>();
							for (DataParticleBatch batchedParticle : batchedParticles) {
								batchResult.addAll(((BatchOperation) operation)
										.execute(batchedParticle));
							}
							outputParticles = batchResult;
							emitParticles(tuple, outputParticles);
						}
					} catch (OperationException | BatcherException oe) {
						this.fail(tuple);
						logger.error(
								"Unable to execute BatchOperation due to: "
										+ oe.getMessage(), oe);
					}

				} else {
					try {
						outputParticles = ((SingleOperation) operation)
								.execute((DataParticle) inputParticle);
						emitParticles(tuple, outputParticles);
						this.ack(tuple);
					} catch (OperationException e) {
						this.fail(tuple);
						logger.error(
								"Unable to execute SingleInputOperation due to: "
										+ e.getMessage(), e);
					}
				}
			}
		}
	}

	/**
	 * Create all metaHandlers as specified in the annotations
	 * 
	 * @param operation
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	private void createMetaParticleHandlers(Operation operation)
			throws InstantiationException, IllegalAccessException {
		// walk through all specified metaHandlers in the annotation
		OperationDeclaration operationDeclaration = operation.getClass()
				.getAnnotation(OperationDeclaration.class);
		for (Class<? extends MetaParticleHandler> mph : operationDeclaration
				.metaParticleHandlers()) {
			MetaParticleHandler newInstance = mph.newInstance();
			newInstance.init(operation);
			metaParticleHandlers.add(newInstance);
		}
	}

	/**
	 * pass this metaParticle to all metaHandlers.
	 * 
	 * @param metaParticle
	 */
	private List<Particle> handleMetaParticle(MetaParticle metaParticle) {
		List<Particle> result = null;
		for (MetaParticleHandler mph : metaParticleHandlers) {
			MetaParticleHandlerDecleration mphd = mph.getClass().getAnnotation(
					MetaParticleHandlerDecleration.class);
			if (metaParticle.getClass().isAssignableFrom(mphd.metaParticle())) {
				result = mph.handleMetaParticle(metaParticle);
			}
		}
		return result;
	}

	/**
	 * Given an Operation class, figure out all the types of particles (both
	 * DataParticles and MetaParticles) produced by this Operation
	 * 
	 * @param operationClass
	 *            Class of the Operation
	 * @return List of Particle classes
	 */
	public static List<Class<? extends Particle>> getOutputParticles(
			Class<? extends Operation> operationClass) {
		List<Class<? extends Particle>> result = new ArrayList<>();
		OperationDeclaration od = operationClass
				.getAnnotation(OperationDeclaration.class);
		for (Class<? extends DataParticle> cl : od.outputs()) {
			result.add(cl);
		}
		for (Class<? extends MetaParticleHandler> cl : od
				.metaParticleHandlers()) {
			MetaParticleHandlerDecleration mphd = cl
					.getAnnotation(MetaParticleHandlerDecleration.class);
			result.add(mphd.metaParticle());
		}
		return result;
	}

	/**
	 * Given an Operation class, figure out all the types of DataParticles
	 * produced by this Operation
	 * 
	 * @param operationClass
	 *            Class of the Operation
	 * @return List of DataParticle classes
	 */
	public static List<Class<? extends DataParticle>> getOutputDataParticles(
			Class<? extends Operation> operationClass) {
		List<Class<? extends DataParticle>> result = new ArrayList<>();
		OperationDeclaration od = operationClass
				.getAnnotation(OperationDeclaration.class);
		for (Class<? extends DataParticle> cl : od.outputs()) {
			result.add(cl);
		}
		return result;
	}

	/**
	 * Given an Operation class, figure out all the types of MetaParticles
	 * produced by this Operation
	 * 
	 * @param operationClass
	 *            Class of the Operation
	 * @return List of MetaParticle classes
	 */
	public static List<Class<? extends MetaParticle>> getOutputMetaParticles(
			Class<? extends Operation> operationClass) {
		List<Class<? extends MetaParticle>> result = new ArrayList<>();
		OperationDeclaration od = operationClass
				.getAnnotation(OperationDeclaration.class);
		for (Class<? extends MetaParticleHandler> cl : od
				.metaParticleHandlers()) {
			MetaParticleHandlerDecleration mphd = cl
					.getAnnotation(MetaParticleHandlerDecleration.class);
			result.add(mphd.metaParticle());
		}
		return result;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// merge all output particle fields for DataParticles
		Fields fields = null;
		List<Class<? extends DataParticle>> outputParticles = ChannelManager
				.getOutputDataParticles(operation.getClass());
		for (Class<? extends DataParticle> outputParticleClass : outputParticles) {
			if (fields == null) {
				fields = ParticleMapper.getFields(outputParticleClass);
			} else {
				fields = ParticleMapper.mergeFields(fields,
						ParticleMapper.getFields(outputParticleClass));
			}
		}

		// Add fields for MetaParticles
		if (fields == null) {
			fields = this.metaParticleFields;
		} else {
			fields = ParticleMapper
					.mergeFields(fields, this.metaParticleFields);
		}

		nrOfOutputFields = fields.size();
		declarer.declare(fields);
	}

	@Override
	public void emitParticle(Tuple anchor, Particle particle) {
		collector.emit(anchor,
				ParticleMapper.particleToValues(particle, nrOfOutputFields));
	}

	@Override
	public void ack(Tuple tuple) {
		if (ackFailAndAnchor)
			collector.ack(tuple);
	}

	@Override
	public void fail(Tuple tuple) {
		if (ackFailAndAnchor)
			collector.fail(tuple);
	}

	public void emitParticles(Tuple anchor, List<? extends Particle> particles) {
		for (Particle particle : particles) {
			if (ackFailAndAnchor)
				this.emitParticle(anchor, particle);
			else
				this.emitParticle(particle);
		}
	}

	@Override
	public void emitParticle(Particle particle) {
		collector.emit(ParticleMapper.particleToValues(particle,
				nrOfOutputFields));
	}

}
