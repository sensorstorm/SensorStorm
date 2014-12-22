package nl.tno.sensorstorm.stormcomponents;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.tno.sensorstorm.batchers.Batcher;
import nl.tno.sensorstorm.config.EmptyStormConfiguration;
import nl.tno.sensorstorm.mapper.ParticleMapper;
import nl.tno.sensorstorm.operations.FlushingSyncBuffer;
import nl.tno.sensorstorm.operations.Operation;
import nl.tno.sensorstorm.operations.OperationException;
import nl.tno.sensorstorm.operations.OperationManager;
import nl.tno.sensorstorm.operations.ParticleBatchOperation;
import nl.tno.sensorstorm.operations.SingleParticleOperation;
import nl.tno.sensorstorm.operations.SyncBuffer;
import nl.tno.sensorstorm.particles.DataParticle;
import nl.tno.sensorstorm.particles.MetaParticle;
import nl.tno.sensorstorm.particles.MetaParticleUtil;
import nl.tno.sensorstorm.particles.Particle;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.storm.configuration.api.StormConfigurationException;
import nl.tno.storm.configuration.impl.ZookeeperStormConfigurationFactory;

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

	// ///////////// //
	// Static fields //
	// ///////////// //
	private static final long serialVersionUID = -5109656134961759532L;
	private static Logger logger = LoggerFactory.getLogger(BaseRichBolt.class);

	// ///////////////////////// //
	// Fields set in constructor //
	// ///////////////////////// //
	protected long syncBufferSize;
	protected Class<? extends Batcher> batcherClass;
	protected Class<? extends Operation> operationClass;
	protected String fieldGrouperId;
	protected Fields metaParticleFields;
	protected Map<String, OperationManager> operationManagers;

	// ///////////////////// //
	// Fields set in prepare //
	// ///////////////////// //
	protected @SuppressWarnings("rawtypes") Map stormNativeConfig;
	protected OutputCollector collector;
	protected String originId;
	protected ExternalStormConfiguration zookeeperStormConfiguration;
	protected SyncBuffer syncBuffer;
	protected HashMap<Particle, String> fieldGrouperValues;

	// //////////// //
	// Other fields //
	// //////////// //
	protected int nrOfOutputFields;

	/**
	 * Construct a {@link SensorStormBolt} with a {@link Batcher}
	 * 
	 * @param conf
	 *            Storm configuration map
	 * @param syncBufferSize
	 *            size of the SyncBuffer in ms
	 * @param batcherClass
	 *            {@link Class} of the {@link Batcher} implementation
	 * @param batchOperationClass
	 *            {@link Class} of the {@link ParticleBatchOperation}
	 *            implementation
	 * @param fieldGrouperId
	 *            The field name on which this bolt the operations should
	 *            instantiated. To specify an single operation, one instance of
	 *            the operation class for all particles, the fieldGrouper must
	 *            be null.
	 * @throws OperationException
	 */
	public SensorStormBolt(Config config, long syncBufferSize,
			Class<? extends Batcher> batcherClass,
			Class<? extends ParticleBatchOperation> batchOperationClass,
			String fieldGrouperId) throws OperationException {
		if (batcherClass == null) {
			throw new OperationException("batcherClass may not be null");
		}
		if (batchOperationClass == null) {
			throw new OperationException("batchOperationClass may not be null");
		}

		sensorStormBolt(config, syncBufferSize, batcherClass,
				batchOperationClass, fieldGrouperId);
	}

	/**
	 * Construct a {@link SensorStormBolt} without a {@link Batcher}
	 * 
	 * @param conf
	 *            Storm configuration map
	 * @param syncBufferSize
	 *            size of the SyncBuffer in ms
	 * @param singleOperationClass
	 *            {@link Class} of the {@link Operation} implementation
	 * @param fieldGrouperId
	 *            The field name on which this bolt the operations should
	 *            instantiated. To specify an single operation, one instance of
	 *            the operation class for all particles, the fieldGrouper must
	 *            be null.
	 * @throws OperationException
	 */
	public SensorStormBolt(Config config, long syncBufferSize,
			Class<? extends SingleParticleOperation> singleOperationClass,
			String fieldGrouperId) throws OperationException {
		if (singleOperationClass == null) {
			throw new OperationException("operationClass may not be null");
		}

		sensorStormBolt(config, syncBufferSize, null, singleOperationClass,
				fieldGrouperId);
	}

	/**
	 * Construct a {@link SensorStormBolt} with a {@link Batcher} and a default
	 * SyncBuffer size of 1000 ms
	 * 
	 * @param conf
	 *            Storm configuration map
	 * @param batcherClass
	 *            {@link Class} of the {@link Batcher} implementation
	 * @param batchOperationClass
	 *            {@link Class} of the {@link ParticleBatchOperation}
	 *            implementation
	 * @param fieldGrouperId
	 *            The field name on which this bolt the operations should
	 *            instantiated. To specify an single operation, one instance of
	 *            the operation class for all particles, the fieldGrouper must
	 *            be null.
	 * @throws OperationException
	 */
	public SensorStormBolt(Config config,
			Class<? extends Batcher> batcherClass,
			Class<? extends ParticleBatchOperation> batchOperationClass,
			String fieldGrouperId) throws OperationException {
		if (batcherClass == null) {
			throw new OperationException("batcherClass may not be null");
		}
		if (batchOperationClass == null) {
			throw new OperationException("batchOperationClass may not be null");
		}

		sensorStormBolt(config, 1000, batcherClass, batchOperationClass,
				fieldGrouperId);
	}

	/**
	 * Construct a {@link SensorStormBolt} without a {@link Batcher} and a
	 * default SyncBuffer size of 1000 ms
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
			Class<? extends SingleParticleOperation> singleOperationClass,
			String fieldGrouperId) throws OperationException {
		if (singleOperationClass == null) {
			throw new OperationException("operationClass may not be null");
		}

		sensorStormBolt(config, 1000, null, singleOperationClass,
				fieldGrouperId);
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
	private void sensorStormBolt(Config config, long syncBufferSize,
			Class<? extends Batcher> batcherClass,
			Class<? extends Operation> operationClass, String fieldGrouperId) {
		// Set fields
		this.syncBufferSize = syncBufferSize;
		this.batcherClass = batcherClass;
		this.operationClass = operationClass;
		this.fieldGrouperId = fieldGrouperId;

		// Initialize data structures
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
		this.syncBuffer = new FlushingSyncBuffer(this.syncBufferSize);
		this.fieldGrouperValues = new HashMap<Particle, String>();
	}

	/**
	 * Handle the new incoming tuple
	 */
	@Override
	public void execute(Tuple originalTuple) {
		// Map the Tuple to a Particle
		// FYI: ParticleMapper will log an error if it is not able to map
		Particle inputParticle = ParticleMapper.tupleToParticle(originalTuple);

		if (fieldGrouperId != null) {
			fieldGrouperValues.put(inputParticle,
					originalTuple.getStringByField(fieldGrouperId));
		}

		if (inputParticle != null) {
			// Push the particle through the SyncBuffer
			List<Particle> particlesToProcess = syncBuffer
					.pushParticle(inputParticle);
			// Process the particles from the buffer (if any)
			for (Particle particle : particlesToProcess) {
				if (particle instanceof MetaParticle) {
					List<Particle> outputParticles = processMetaParticle((MetaParticle) particle);
					// Emit new particles (if any)
					emitParticles(originalTuple, outputParticles);
					// Pass through the current MetaParticle
					emitParticle(originalTuple, particle);
				} else if (particle instanceof DataParticle) {
					List<Particle> outputParticles = processDataParticle((DataParticle) particle);
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
	 * @param inputParticle
	 *            originalTuple mapped to a DataParticle
	 * @return List of output particles
	 */
	private List<Particle> processDataParticle(DataParticle inputParticle) {
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
			String fieldGrouperValue = fieldGrouperValues.get(inputParticle);

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
	 * @param inputParticle
	 *            originalTuple mapped to a MetaParticle
	 * @return List of output particles
	 */
	private List<Particle> processMetaParticle(MetaParticle inputParticle) {
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
				if (SingleParticleOperation.class
						.isAssignableFrom(operationClass)) {
					operationManager = new OperationManager(
							fieldGrouperValue,
							(Class<? extends SingleParticleOperation>) operationClass,
							stormNativeConfig, zookeeperStormConfiguration);
				}

				// is it a batch operation?
				else if (ParticleBatchOperation.class
						.isAssignableFrom(operationClass)) {
					operationManager = new OperationManager(
							fieldGrouperValue,
							batcherClass,
							(Class<? extends ParticleBatchOperation>) operationClass,
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
			fieldGrouperValues.remove(particle);
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
