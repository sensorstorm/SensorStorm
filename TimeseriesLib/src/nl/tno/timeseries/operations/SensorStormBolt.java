package nl.tno.timeseries.operations;

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
	protected @SuppressWarnings("rawtypes")
	Map stormNativeConfig;

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
	 * general logic for the constructors of the singleOperation and the
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
	}

	/**
	 * Handle the new incoming tuple
	 */
	@Override
	public void execute(Tuple tuple) {
		Particle inputParticle = ParticleMapper.tupleToParticle(tuple);

		// is there a particle to process?
		if (inputParticle != null) {
			if (inputParticle instanceof MetaParticle) {
				// broadcast metaParticle to all operationManagers
				Collection<OperationManager> allOperationManagers = operationManagers
						.values();
				for (OperationManager operationManager : allOperationManagers) {
					operationManager
							.processMetaParticle((MetaParticle) inputParticle);
				}
			} else if (inputParticle instanceof DataParticle) {
				// deliver dataParticle to the correct operationManager

				// get an operation manager based on the value of the
				// fieldGrouperId field in the tuple, as specified in the
				// constructor
				OperationManager operationManager;
				if (fieldGrouperId == null) { // single instance operation mode
					operationManager = getOperationManager(null);
				} else { // try to select an operationManager from the value of
							// the fieldGrouperId field
					String fieldGrouperValue = tuple
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

				if (operationManager != null) {
					// process the particle and emit the output
					List<Particle> outputParticles = operationManager
							.processDataParticle((DataParticle) inputParticle);
					emitParticles(tuple, outputParticles);
					collector.ack(tuple);
				}

			} else {
				logger.error("Unknown particle type, not a MetaParticle or a DataParticle, but a "
						+ inputParticle.getClass().getName());
			}

		}
	}

	/**
	 * Returns the operationManager related to the fieldGrouper, or instantiate
	 * one if it was not present.
	 * 
	 * @fieldGrouperValue
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
	 * Emit a particle, anchored to the anchor tuple
	 * 
	 * @param anchor
	 * @param particle
	 */
	public void emitParticle(Tuple anchor, Particle particle) {
		if (particle != null) {
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
