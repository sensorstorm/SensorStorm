package nl.tno.sensorstorm.storm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.tno.sensorstorm.api.annotation.OperationDeclaration;
import nl.tno.sensorstorm.api.particles.DataParticle;
import nl.tno.sensorstorm.api.particles.MetaParticle;
import nl.tno.sensorstorm.api.particles.Particle;
import nl.tno.sensorstorm.api.processing.Batcher;
import nl.tno.sensorstorm.api.processing.Operation;
import nl.tno.sensorstorm.api.processing.ParticleBatchOperation;
import nl.tno.sensorstorm.api.processing.SingleParticleOperation;
import nl.tno.sensorstorm.config.EmptyStormConfiguration;
import nl.tno.sensorstorm.impl.FlushingSyncBuffer;
import nl.tno.sensorstorm.impl.MetaParticleUtil;
import nl.tno.sensorstorm.impl.OperationManager;
import nl.tno.sensorstorm.impl.SyncBuffer;
import nl.tno.sensorstorm.particlemapper.ParticleMapper;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.storm.configuration.api.StormConfigurationException;
import nl.tno.storm.configuration.impl.ZookeeperStormConfigurationFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * This is a generic Bolt for the SensorStorm library. There are several
 * configurations of this bolt possible. There are different constructors
 * different configurations. On top a {@link SensorStormBolt} you can run an
 * {@link Operation} and optionally a {@link Batcher}.
 * 
 * You can use this Bolt without a {@link Batcher}. In that case you can run a
 * {@link SingleParticleOperation} on top of this Bolt.
 * 
 * You can also put a {@link Batcher} on this bolt. In that case you have to run
 * a {@link ParticleBatchOperation} on top of this Bolt.
 * 
 * Additionally, you can choose to have one instance of the {@link Operation}
 * (and possibly the {@link Batcher}), or you can have a separate
 * {@link Operation} (and possible {@link Batcher}) for each value of a field in
 * the {@link Tuple} (e.g. the sensor id). If you do this, you usually want to
 * use a {@link SensorStormFieldGrouping} with the same field name on the Spout
 * or Bolt before this Bolt.
 */
public class SensorStormBolt extends BaseRichBolt {

	// ///////////// //
	// Static fields //
	// ///////////// //
	private static final long serialVersionUID = -5109656134961759532L;
	private static final Logger logger = LoggerFactory
			.getLogger(SensorStormBolt.class);
	private static final int TIME_BUCKET_SIZE_IN_SECS = 10;
	private static final int DEFAULT_SYNC_BUFFER_SIZE = 1000;

	// ///////////////////////// //
	// Fields set in constructor //
	// ///////////////////////// //
	protected long syncBufferSize;
	protected Class<? extends Batcher> batcherClass;
	protected Class<? extends Operation> operationClass;
	protected String fieldGrouperId;
	protected Fields metaParticleFields;

	// ///////////////////// //
	// Fields set in prepare //
	// ///////////////////// //
	@SuppressWarnings("rawtypes")
	protected transient Map stormNativeConfig;
	protected transient OutputCollector collector;
	protected transient String originId;
	protected transient ExternalStormConfiguration zookeeperStormConfiguration;
	protected transient SyncBuffer syncBuffer;
	protected transient Map<String, OperationManager> operationManagers;
	protected transient Map<Particle, String> fieldGrouperValues;
	protected transient CountMetric bufferRejectMetric;

	// //////////// //
	// Other fields //
	// //////////// //
	protected int nrOfOutputFields;

	/**
	 * Construct a {@link SensorStormBolt} with a {@link Batcher}.
	 * 
	 * @param config
	 *            Storm configuration map
	 * @param syncBufferSize
	 *            size of the SyncBuffer in milliseconds
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
	 * @throws NullPointerException
	 *             When batchOperationClass or batchOperationClass is null
	 */
	public SensorStormBolt(Config config, long syncBufferSize,
			Class<? extends Batcher> batcherClass,
			Class<? extends ParticleBatchOperation> batchOperationClass,
			String fieldGrouperId) {
		if (batcherClass == null) {
			throw new NullPointerException("batcherClass");
		}
		if (batchOperationClass == null) {
			throw new NullPointerException("batchOperationClass");
		}

		sensorStormBolt(config, syncBufferSize, batcherClass,
				batchOperationClass, fieldGrouperId);
	}

	/**
	 * Construct a {@link SensorStormBolt} without a {@link Batcher}.
	 * 
	 * @param config
	 *            Storm configuration map
	 * @param syncBufferSize
	 *            size of the SyncBuffer in milliseconds
	 * @param singleOperationClass
	 *            {@link Class} of the {@link Operation} implementation
	 * @param fieldGrouperId
	 *            The field name on which this bolt the operations should
	 *            instantiated. To specify an single operation, one instance of
	 *            the operation class for all particles, the fieldGrouper must
	 *            be null.
	 * @throws NullPointerException
	 *             When singleOperationClass is null
	 */
	public SensorStormBolt(Config config, long syncBufferSize,
			Class<? extends SingleParticleOperation> singleOperationClass,
			String fieldGrouperId) {
		if (singleOperationClass == null) {
			throw new NullPointerException("singleOperationClass");
		}

		sensorStormBolt(config, syncBufferSize, null, singleOperationClass,
				fieldGrouperId);
	}

	/**
	 * Construct a {@link SensorStormBolt} with a {@link Batcher} and a default
	 * SyncBuffer size of 1000 milliseconds.
	 * 
	 * @param config
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
	 * @throws NullPointerException
	 *             When batcherClass or batchOperationClass is null
	 */
	public SensorStormBolt(Config config,
			Class<? extends Batcher> batcherClass,
			Class<? extends ParticleBatchOperation> batchOperationClass,
			String fieldGrouperId) {
		if (batcherClass == null) {
			throw new NullPointerException("batcherClass");
		}
		if (batchOperationClass == null) {
			throw new NullPointerException("batchOperationClass");
		}

		sensorStormBolt(config, DEFAULT_SYNC_BUFFER_SIZE, batcherClass,
				batchOperationClass, fieldGrouperId);
	}

	/**
	 * Construct a {@link SensorStormBolt} without a {@link Batcher} and a
	 * default SyncBuffer size of 1000 milliseconds.
	 * 
	 * @param config
	 *            Storm configuration map
	 * @param singleOperationClass
	 *            {@link Class} of the {@link Operation} implementation
	 * @param fieldGrouperId
	 *            The field name on which this bolt the operations should
	 *            instantiated. To specify an single operation, one instance of
	 *            the operation class for all particles, the fieldGrouper must
	 *            be null.
	 * @throws NullPointerException
	 *             When singleOperationClass is null
	 */
	public SensorStormBolt(Config config,
			Class<? extends SingleParticleOperation> singleOperationClass,
			String fieldGrouperId) {
		if (singleOperationClass == null) {
			throw new NullPointerException("singleOperationClass");
		}

		sensorStormBolt(config, DEFAULT_SYNC_BUFFER_SIZE, null,
				singleOperationClass, fieldGrouperId);
	}

	/**
	 * General logic for the constructors of the singleOperation and the
	 * batchOperation.
	 * 
	 * @param config
	 *            Storm configuration map
	 * @param syncBufferSize
	 *            Size of the SyncBuffer in milliseconds
	 * @param batcherClass
	 *            Class of the {@link Batcher}
	 * @param operationClass
	 *            Class of the {@link Operation}
	 * @param fieldGrouperId
	 *            Field name to group on
	 */
	private void sensorStormBolt(Config config, long syncBufferSize,
			Class<? extends Batcher> batcherClass,
			Class<? extends Operation> operationClass, String fieldGrouperId) {
		// Set fields
		this.syncBufferSize = syncBufferSize;
		this.batcherClass = batcherClass;
		this.operationClass = operationClass;
		this.fieldGrouperId = fieldGrouperId;

		// Check annotations
		if (!operationClass.isAnnotationPresent(OperationDeclaration.class)) {
			throw new IllegalArgumentException("The operation "
					+ operationClass.getName()
					+ " does not have an OperationDecleration");
		}

		// Initialize data structures
		metaParticleFields = MetaParticleUtil
				.registerMetaParticleFieldsFromOperationClass(config,
						operationClass);
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormNativeConfig,
			TopologyContext context, OutputCollector collector) {
		this.stormNativeConfig = stormNativeConfig;
		this.collector = collector;
		originId = operationClass.getName() + "." + context.getThisTaskIndex();

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
		syncBuffer = new FlushingSyncBuffer(syncBufferSize);
		operationManagers = new HashMap<String, OperationManager>();
		fieldGrouperValues = new HashMap<Particle, String>();
		bufferRejectMetric = new CountMetric();
		context.registerMetric("syncbuffer_rejects", bufferRejectMetric,
				TIME_BUCKET_SIZE_IN_SECS);
	}

	/**
	 * Handle the new incoming tuple.
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
			try {
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
			} catch (IllegalArgumentException e) {
				bufferRejectMetric.incr();
				logger.warn("Particle with timestamp "
						+ inputParticle.getTimestamp()
						+ " was rejected from SyncBuffer of bolt " + originId);
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
	 *            Name of the field to group on (null if this bolt has a
	 *            {@link SingleParticleOperation})
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
				if (SingleParticleOperation.class
						.isAssignableFrom(operationClass)) {
					// Single Operation
					operationManager = new OperationManager(
							fieldGrouperValue,
							(Class<? extends SingleParticleOperation>) operationClass,
							stormNativeConfig, zookeeperStormConfiguration);
				} else if (ParticleBatchOperation.class
						.isAssignableFrom(operationClass)) {
					// Batch Operation
					operationManager = new OperationManager(
							fieldGrouperValue,
							batcherClass,
							(Class<? extends ParticleBatchOperation>) operationClass,
							stormNativeConfig, zookeeperStormConfiguration);
				} else {
					// Apparently a new constructor is added to create a new
					// type of operation
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
		fields = ParticleMapper.mergeFields(fields, metaParticleFields);

		nrOfOutputFields = fields.size();
		declarer.declare(fields);
	}

	/**
	 * Emit a particle, anchored to the anchor tuple.
	 * 
	 * @param anchor
	 *            Tuple to anchor on
	 * @param particle
	 *            Particle to emit
	 */
	public void emitParticle(Tuple anchor, Particle particle) {
		if (particle != null) {
			fieldGrouperValues.remove(particle);
			if (particle instanceof MetaParticle) {
				((MetaParticle) particle).setOriginId(originId);
			}
			collector
					.emit(anchor, ParticleMapper.particleToValues(particle,
							nrOfOutputFields));
		}
	}

	/**
	 * Emit a list of particles, if the list is not null or empty. Each particle
	 * will be anchored.
	 * 
	 * @param anchor
	 *            Tuple to anchor on
	 * @param particles
	 *            Particles to emit
	 */
	public void emitParticles(Tuple anchor, List<? extends Particle> particles) {
		if (particles != null) {
			for (Particle particle : particles) {
				emitParticle(anchor, particle);
			}
		}
	}

}
