package nl.tno.timeseries.channels;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.timeseries.annotation.MetaParticleHandlerDecleration;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.config.ConfigKeys;
import nl.tno.timeseries.interfaces.BatchOperation;
import nl.tno.timeseries.interfaces.Batcher;
import nl.tno.timeseries.interfaces.BatcherException;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.DataParticleBatch;
import nl.tno.timeseries.interfaces.MetaParticle;
import nl.tno.timeseries.interfaces.Operation;
import nl.tno.timeseries.interfaces.OperationException;
import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.interfaces.SingleOperation;
import nl.tno.timeseries.mapper.ParticleMapper;
import nl.tno.timeseries.particles.MetaParticleHandler;
import nl.tno.timeseries.particles.MetaParticleUtil;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * StormNativeConfig params used: ChannelSpout.TOPOLOGY_FAULT_TOLERANT
 * Config.TOPOLOGY_MAX_SPOUT_PENDING Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS
 * ChannelSpout.TOPOLOGY_TUPLECACHE_MAX_SIZE
 * 
 * @author waaijbdvd
 * 
 */
public class SingleOperationChannelBolt extends AbstractOperationChannelBolt {
	private static final long serialVersionUID = -7628008145368347247L;
	private final static String EMPTY_CHANNELID = "";

	protected int nrOfOutputFields;
	protected Fields metaParticleFields;
	protected ParticleCache particleCache;
	private final Operation operation;
	private final Batcher batcher;
	private List<MetaParticleHandler> metaParticleHandlers;
	protected boolean batcherPreparedForFirstParticle = false;
	protected boolean operationPreparedForFirstParticle = false;

	/**
	 * Construct a {@link SingleOperationChannelBolt} with a {@link Batcher}
	 * 
	 * @param conf
	 *            Storm configuration map
	 * @param batcherClass
	 *            {@link Class} of the {@link Batcher} implementation
	 * @param batchOperationClass
	 *            {@link Class} of the {@link BatchOperation} implementation
	 * @throws OperationException
	 */
	public SingleOperationChannelBolt(Config config, Batcher batcher,
			BatchOperation batchOperation) throws OperationException {
		if (batchOperation == null) {
			throw new OperationException("batchOperation may not be null");
		}
		if (batcher == null) {
			throw new OperationException("batcher may not be null");
		}

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
	 * @throws OperationException
	 */
	public SingleOperationChannelBolt(Config config, SingleOperation operation)
			throws OperationException {
		if (operation == null) {
			throw new OperationException("batchOperation may not be null");
		}

		// Set fields
		this.operation = operation;
		this.batcher = null;
		this.metaParticleFields = MetaParticleUtil
				.registerMetaParticleFieldsWithOperationClass(config,
						operation.getClass());
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormNativeConfig,
			TopologyContext context, OutputCollector collector) {
		super.prepare(stormNativeConfig, context, collector);

		// initiate particle cache
		int timeout = ConfigKeys.getStormNativeIntegerConfigValue(
				stormNativeConfig, Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS,
				ConfigKeys.TOPOLOGY_MESSAGE_TIMEOUT_SECS_DEFAULT);
		int maxSize = ConfigKeys.getStormNativeIntegerConfigValue(
				stormNativeConfig, ConfigKeys.SPOUT_TUPLECACHE_MAX_SIZE,
				ConfigKeys.SPOUT_TUPLECACHE_MAX_SIZE_DEFAULT);
		particleCache = new ParticleCache(maxSize, timeout, this);

		// initialze operation and optionally the batcher
		try {
			if (batcher != null) {
				batcher.init(EMPTY_CHANNELID, particleCache, stormNativeConfig,
						zookeeperStormConfiguration);
			}
			operation.init(EMPTY_CHANNELID, stormNativeConfig,
					zookeeperStormConfiguration);
			createMetaParticleHandlers(operation);
		} catch (InstantiationException | IllegalAccessException
				| OperationException | BatcherException e) {
			logger.error("Unable to instantiate batcher and/or operation due to: "
					+ e.getMessage());
		}

	}

	/**
	 * Handle the new incoming tuple
	 */
	@Override
	public void execute(Tuple tuple) {
		Particle inputParticle = ParticleMapper.tupleToParticle(tuple);

		// is there a particle to process?
		if (inputParticle != null) {

			// **** handle metadata particle
			if (inputParticle instanceof MetaParticle) {
				List<Particle> outputParticles = handleMetaParticle((MetaParticle) inputParticle);
				emitParticles(tuple, outputParticles);
				this.ack(tuple);
			}

			// **** handle data particle
			else if (inputParticle instanceof DataParticle) {
				DataParticle dataParticle = (DataParticle) inputParticle;
				handleDataParticle(tuple, dataParticle);
			}
		}
	}

	/**
	 * Handle an incoming dataParticle
	 * 
	 * @param tuple
	 * @param dataParticle
	 */
	protected void handleDataParticle(Tuple tuple, DataParticle dataParticle) {
		// prepare the operation if it is the first time
		if (!operationPreparedForFirstParticle) {
			try {
				operation.prepareForFirstParticle(dataParticle.getTimestamp());
				operationPreparedForFirstParticle = true;
			} catch (OperationException e) {
				this.fail(tuple);
				logger.error(
						"Unable to prepare the SingleInputOperation for the first particle due to: "
								+ e.getMessage(), e);
			}
		}

		// Is there a batcher to call before the operation?
		if (batcher != null) {
			try {
				// prepare the batcher if it is the first time
				if (!batcherPreparedForFirstParticle) {
					batcher.prepareForFirstParticle(dataParticle.getTimestamp());
					batcherPreparedForFirstParticle = true;
				}

				// add data particle to the cache
				particleCache.put(dataParticle, tuple);

				// call the batcher to see if there is some batch ready
				// for the operation
				List<DataParticleBatch> batchedParticles = batcher
						.batch(dataParticle);

				// are there one or more batches to process?
				if (batchedParticles != null) {
					List<DataParticle> resultOfAllBatches = new ArrayList<DataParticle>();
					for (DataParticleBatch batchedParticle : batchedParticles) {
						resultOfAllBatches.addAll(((BatchOperation) operation)
								.execute(batchedParticle));
					}
					emitParticles(tuple, resultOfAllBatches);
				}
			} catch (OperationException | BatcherException oe) {
				this.fail(tuple);
				logger.error(
						"Unable to execute BatchOperation due to: "
								+ oe.getMessage(), oe);
			}
		}

		// no batcher, only the operation
		else {
			try {
				List<DataParticle> outputDataParticles = ((SingleOperation) operation)
						.execute(dataParticle);
				emitParticles(tuple, outputDataParticles);
				this.ack(tuple);
			} catch (OperationException e) {
				this.fail(tuple);
				logger.error("Unable to execute SingleInputOperation due to: "
						+ e.getMessage(), e);
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
	protected void createMetaParticleHandlers(Operation operation)
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
	protected List<Particle> handleMetaParticle(MetaParticle metaParticle) {
		List<Particle> result = new ArrayList<Particle>();
		for (MetaParticleHandler mph : metaParticleHandlers) {
			MetaParticleHandlerDecleration mphd = mph.getClass().getAnnotation(
					MetaParticleHandlerDecleration.class);
			if (metaParticle.getClass().isAssignableFrom(mphd.metaParticle())) {
				result.addAll(mph.handleMetaParticle(metaParticle));
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

	/**
	 * Emit an anchored particle, called from the batcher
	 */
	@Override
	public void emitParticle(Tuple anchor, Particle particle) {
		if (particle != null) {
			collector
					.emit(anchor, ParticleMapper.particleToValues(particle,
							nrOfOutputFields));
		}
	}

	/**
	 * Emit a non-anchored particle, called from the batcher
	 */
	@Override
	public void emitParticle(Particle particle) {
		collector.emit(ParticleMapper.particleToValues(particle,
				nrOfOutputFields));
	}

}
