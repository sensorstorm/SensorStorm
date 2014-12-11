package nl.tno.timeseries.operations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.storm.configuration.api.StormConfigurationException;
import nl.tno.storm.configuration.impl.ZookeeperStormConfigurationFactory;
import nl.tno.timeseries.batchers.Batcher;
import nl.tno.timeseries.config.EmptyStormConfiguration;
import nl.tno.timeseries.mapper.ParticleMapper;
import nl.tno.timeseries.particles.DataParticle;
import nl.tno.timeseries.particles.MetaParticle;
import nl.tno.timeseries.particles.MetaParticleUtil;
import nl.tno.timeseries.particles.Particle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class SensorStormBolt extends BaseRichBolt {
	private static final long serialVersionUID = -5109656134961759532L;
	private static final long SYNC_BUFFFER_SIZE_MS = 1000;
	protected Logger logger = LoggerFactory.getLogger(this.getClass());

	protected Class<? extends Operation> operationClass;
	protected int nrOfOutputFields;
	protected Class<? extends Batcher> batcherClass;
	protected Map<String, OperationManager> operationManagers;
	protected Fields metaParticleFields;
	protected String fieldGrouperId;

	protected OutputCollector collector;
	protected String boltName;
	protected ExternalStormConfiguration zookeeperStormConfiguration;
	protected @SuppressWarnings("rawtypes") Map stormNativeConfig;
	private SyncBuffer syncBuffer;
	private String originId;

	/**
	 * Construct a {@link SensorStormBolt} with a {@link Batcher}
	 * 
	 * @param conf
	 *            Storm configuration map
	 * @param batcherClass
	 *            {@link Class} of the {@link Batcher} implementation
	 * @param batchOperationClass
	 *            {@link Class} of the {@link BatchOperation} implementation
	 * @param fieldGrouperId
	 *            The field name on which this bolt the operations should
	 *            instantiated. To specify an single operation, one instance of
	 *            the operation class for all particles, the fieldGrouper must
	 *            be null.
	 * @throws OperationException
	 */
	public SensorStormBolt(Config config,
			Class<? extends Batcher> batcherClass,
			Class<? extends BatchOperation> batchOperationClass,
			String fieldGrouperId) throws OperationException {
		if (batcherClass == null) {
			throw new OperationException("batcherClass may not be null");
		}
		if (batchOperationClass == null) {
			throw new OperationException("batchOperationClass may not be null");
		}

		sensorStormBolt(config, batcherClass, batchOperationClass,
				fieldGrouperId);
	}

	/**
	 * Construct a {@link SensorStormBolt} without a {@link Batcher}
	 * 
	 * @param conf
	 *            Storm configuration map
	 * @param singleOperationClass
	 *            {@link Class} of the {@link Operation} implementation
	 * @param fieldGrouperId
	 *            The field name on which this bolt the operations should
	 *            instantiated. To specify an single operation, one instance of
	 *            the operation class for all particles, the fieldGrouper must
	 *            be null.
	 * @throws OperationException
	 */
	public SensorStormBolt(Config config,
			Class<? extends SingleOperation> singleOperationClass,
			String fieldGrouperId) throws OperationException {
		if (singleOperationClass == null) {
			throw new OperationException("operationClass may not be null");
		}

		sensorStormBolt(config, null, singleOperationClass, fieldGrouperId);
	}

	/**
	 * General logic for the constructors of the singleOperation and the
	 * batchOperation.
	 * 
	 * @param config
	 * @param batcherClass
	 * @param operationClass
	 * @param fieldGrouperId
	 */
	private void sensorStormBolt(Config config,
			Class<? extends Batcher> batcherClass,
			Class<? extends Operation> operationClass, String fieldGrouperId) {

		this.operationClass = operationClass;
		this.batcherClass = null;
		this.fieldGrouperId = fieldGrouperId;
		this.operationManagers = new HashMap<String, OperationManager>();
		this.metaParticleFields = MetaParticleUtil
				.registerMetaParticleFieldsWithOperationClass(config,
						operationClass);
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormNativeConfig,
			TopologyContext context, OutputCollector collector) {
		this.stormNativeConfig = stormNativeConfig;
		this.collector = collector;
		this.boltName = context.getThisComponentId();
		this.originId = operationClass.getName() + "."
				+ context.getThisTaskIndex();

		// connect to the zoopkeeper configuration
		try {
			zookeeperStormConfiguration = ZookeeperStormConfigurationFactory
					.getInstance().getStormConfiguration(stormNativeConfig);
		} catch (StormConfigurationException e) {
			logger.error("Can not connect to zookeeper for get Storm configuration. Reason: "
					+ e.getMessage());
			// create empty config to avoid errors
			zookeeperStormConfiguration = new EmptyStormConfiguration();
		}

		String msg = "SensorStormBolt instance created for";
		if (fieldGrouperId == null) {
			msg = msg + " a single_instance operation class \""
					+ operationClass.getName() + "\"";
		} else {
			msg = msg + " a fieldGrouping operation class \""
					+ operationClass.getName() + "\" grouped on tuple field \""
					+ fieldGrouperId + "\"";
		}
		if (batcherClass != null) {
			msg = msg + ", with a batcher class \"" + batcherClass.getName()
					+ "\"";
		} else {
			msg = msg + ", with no batcher.";
		}
		logger.info(msg);
		this.syncBuffer = new FlushingSyncBuffer(SYNC_BUFFFER_SIZE_MS);
	}

	/**
	 * Handle the new incoming tuple
	 */
	@Override
	public void execute(Tuple originalTuple) {
		// Map the Tuple to a Particle
		// FYI: ParticleMapper will log an error if it is not able to map
		Particle inputParticle = ParticleMapper.tupleToParticle(originalTuple);

		if (inputParticle != null) {
			// Push the particle through the SyncBuffer
			List<Particle> particlesToProcess = syncBuffer
					.pushParticle(inputParticle);
			// Process the particles from the buffer (if any)
			for (Particle particle : particlesToProcess) {
				if (particle instanceof MetaParticle) {
					List<Particle> outputParticles = processMetaParticle(
							originalTuple, (MetaParticle) particle);
					// Emit new particles (if any)
					emitParticles(originalTuple, outputParticles);
					// Pass through the current MetaParticle
					emitParticle(originalTuple, particle);
				} else if (particle instanceof DataParticle) {
					List<Particle> outputParticles = processDataParticle(
							originalTuple, (DataParticle) particle);
					emitParticles(originalTuple, outputParticles);
				} else {
					// This is not a MetaParticle and not a DataParticle
					logger.error("Unknown particle type, not a MetaParticle or a DataParticle, but a "
							+ particle.getClass().getName());
				}
			}
		}
		// Always acknowledge tuples
		collector.ack(originalTuple);
	}

	/**
	 * Process a single DataParticle. This method sends the DataParticle to
	 * appropriate {@link OperationManager}.
	 * 
	 * @param originalTuple
	 *            The unmapped tuple
	 * @param inputParticle
	 *            originalTuple mapped to a DataParticle
	 * @return List of output particles
	 */
	private List<Particle> processDataParticle(Tuple originalTuple,
			DataParticle inputParticle) {
		// deliver dataParticle to the correct operationManager

		// get an operation manager based on the value of the
		// fieldGrouperId field in the tuple, as specified in the
		// constructor
		OperationManager operationManager;
		if (fieldGrouperId == null) {
			// single instance operation mode
			operationManager = getOperationManager(null);
		} else {
			// try to select an operationManager from the value of the
			// fieldGrouperId field
			String fieldGrouperValue = originalTuple
					.getStringByField(fieldGrouperId);
			if (fieldGrouperValue != null) { // fieldGrouperId exists
				operationManager = getOperationManager(fieldGrouperValue);
			} else {
				operationManager = null;
				logger.error("Specified fieldGrouperId "
						+ fieldGrouperId
						+ " does not exists in particle "
						+ inputParticle
						+ ". Therefore can not route it to a specific operation.");
			}
		}

		if (operationManager == null) {
			return null;
		} else {
			return operationManager.processDataParticle(inputParticle);
		}
	}

	/**
	 * Process a single MetaParticle. This method sends the MetaParticle to all
	 * {@link OperationManager}s.
	 * 
	 * @param originalTuple
	 *            The unmapped tuple
	 * @param inputParticle
	 *            originalTuple mapped to a MetaParticle
	 * @return List of output particles
	 */
	private List<Particle> processMetaParticle(Tuple originalTuple,
			MetaParticle inputParticle) {
		// broadcast metaParticle to all operationManagers
		List<Particle> outputParticles = new ArrayList<Particle>();
		Collection<OperationManager> allOperationManagers = operationManagers
				.values();
		for (OperationManager operationManager : allOperationManagers) {
			List<Particle> particles = operationManager
					.processMetaParticle(inputParticle);
			if (particles != null) {
				outputParticles.addAll(particles);
			}
		}
		return outputParticles;
	}

	/**
	 * Returns the operationManager related to the fieldGrouper, or instantiate
	 * one if it was not present.
	 * 
	 * @param fieldGrouperValue
	 * @return Returns an operationManager for the fieldGrouper. Returns null in
	 *         case of an exception, this will be logged.
	 */
	@SuppressWarnings("unchecked")
	private OperationManager getOperationManager(String fieldGrouperValue) {
		OperationManager operationManager = operationManagers
				.get(fieldGrouperValue);

		try {
			// no operation manager present yet for the fieldGrouper
			if (operationManager == null) {
				// is it a single operation?
				if (SingleOperation.class.isAssignableFrom(operationClass)) {
					operationManager = new OperationManager(fieldGrouperValue,
							(Class<? extends SingleOperation>) operationClass,
							stormNativeConfig, zookeeperStormConfiguration);
				}

				// is it a batch operation?
				else if (BatchOperation.class.isAssignableFrom(operationClass)) {
					operationManager = new OperationManager(fieldGrouperValue,
							batcherClass,
							(Class<? extends BatchOperation>) operationClass,
							stormNativeConfig, zookeeperStormConfiguration);
				} else {
					// Apparently a new constructor is added to create a new
					// type of
					// operation
					logger.error("Internal error, unknown operation class "
							+ operationClass.getName());
				}

				// register the new operation manager for this
				// fieldGrouperValue
				operationManagers.put(fieldGrouperValue, operationManager);
			}
		} catch (InstantiationException | IllegalAccessException e) {
			logger.error("For fieldGrouper "
					+ fieldGrouperId
					+ "("
					+ fieldGrouperValue
					+ ") : can not create an operationManager for the operation ("
					+ operationClass.getName() + ") msg=" + e);
			operationManager = null;
		}

		return operationManager;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// merge all output particle fields for DataParticles
		Fields fields = null;
		List<Class<? extends DataParticle>> outputParticles = OperationManager
				.getOutputDataParticles(operationClass);
		for (Class<? extends DataParticle> outputParticleClass : outputParticles) {
			fields = ParticleMapper.mergeFields(fields,
					ParticleMapper.getFields(outputParticleClass));
		}

		// Add fields for MetaParticles
		fields = ParticleMapper.mergeFields(fields, this.metaParticleFields);

		nrOfOutputFields = fields.size();
		declarer.declare(fields);
	}

	/**
	 * Emit a particle, anchored to the anchor tuple
	 * 
	 * @param anchor
	 * @param particle
	 */
	public void emitParticle(Tuple anchor, Particle particle) {
		if (particle != null) {
			if (particle instanceof MetaParticle) {
				((MetaParticle) particle).setOriginId(this.originId);
			}
			collector
					.emit(anchor, ParticleMapper.particleToValues(particle,
							nrOfOutputFields));
		}
	}

	/**
	 * Emit a list of particles, if the list is not null or empty. Each particle
	 * will be anchored
	 * 
	 * @param anchor
	 * @param particles
	 */
	public void emitParticles(Tuple anchor, List<? extends Particle> particles) {
		if (particles != null) {
			for (Particle particle : particles) {
				this.emitParticle(anchor, particle);
			}
		}
	}

}
