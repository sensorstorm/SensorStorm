package nl.tno.timeseries.channels;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.storm.configuration.api.StormConfiguration;
import nl.tno.storm.configuration.api.StormConfigurationException;
import nl.tno.storm.configuration.impl.ZookeeperStormConfigurationFactory;
import nl.tno.timeseries.annotation.MetaParticleHandlerDecleration;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.config.EmptyStormConfiguration;
import nl.tno.timeseries.interfaces.BatchOperation;
import nl.tno.timeseries.interfaces.Batcher;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.DataParticleBatch;
import nl.tno.timeseries.interfaces.MetaParticle;
import nl.tno.timeseries.interfaces.Operation;
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
		EmitParticleInterface {
	private static final long serialVersionUID = -7628008145368347247L;
	private final static String EMPTY_CHANNELID = "";
	private final static long EMPTY_STARTTIMESTAMP = 0;

	protected Logger logger = LoggerFactory
			.getLogger(SingleOperationChannelBolt.class);
	protected StormConfiguration stormConfiguration;
	protected OutputCollector collector;
	protected String boltName;
	protected int nrOfOutputFields;
	protected Fields metaParticleFields;
	private final Operation operation;
	private Batcher batcher;
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
			stormConfiguration = ZookeeperStormConfigurationFactory
					.getInstance().getStormConfiguration(stormNativeConfig);
		} catch (StormConfigurationException e) {
			logger.error("Can not connect to zookeeper for get Storm configuration. Reason: "
					+ e.getMessage());
			stormConfiguration = new EmptyStormConfiguration();
		}

		try {
			if (batcher != null) {
				batcher.init(EMPTY_CHANNELID, EMPTY_STARTTIMESTAMP,
						stormConfiguration);
			}
			if (operation != null) {
				operation.init(EMPTY_CHANNELID, EMPTY_STARTTIMESTAMP,
						stormConfiguration);
				createMetaParticleHandlers(operation);
			}
		} catch (InstantiationException | IllegalAccessException e) {
			logger.error("Unable to instantiate batcher and/or operation due to: "
					+ e.getMessage());
		}

	}

	@Override
	public void execute(Tuple tuple) {
		Particle inputParticle = ParticleMapper.tupleToParticle(tuple);
		if (inputParticle != null) {
			List<Particle> outputParticles = processParticle(inputParticle);
			if (outputParticles != null) {
				for (Particle outputParticle : outputParticles) {
					emitParticle(outputParticle);
				}
			}
		}
	}

	/**
	 * Process a particle (either MetaParticle or DataParticle)
	 * 
	 * @param particle
	 *            Particle to be processed. If the particle == null, null is
	 *            returned
	 * @return returns a list with one MetaParticle (to be sent further upto the
	 *         topology), zero or more DataParticles or null in case of an
	 *         error. These particles should be emitted by the bolt.
	 */
	public List<Particle> processParticle(Particle particle) {
		if (particle == null)
			return null;

		// parse particles
		List<Particle> result = new ArrayList<Particle>();

		if (particle instanceof MetaParticle) { // metaParticle
			List<Particle> outputParticles = handleMetaParticle((MetaParticle) particle);
			// add metaParticle to output list in order to be resent further in
			// the topology
			result.add(particle);
			// add optional output particles
			if (outputParticles != null) {
				result.addAll(outputParticles);
			}
		} else if (particle instanceof DataParticle) { // dataParticle
			List<DataParticle> outputDataParticles = null;
			if (batcher != null) { // batch dataParticle and give it to
									// batcherOperation
				List<DataParticleBatch> batchedParticles = batcher
						.batch((DataParticle) particle);
				// are there one or more batches to be sent?
				if (batchedParticles != null) {
					for (DataParticleBatch batchedParticle : batchedParticles) {
						outputDataParticles = ((BatchOperation) operation)
								.execute(batchedParticle);
					}
				}
			} else { // single operation
				outputDataParticles = ((SingleOperation) operation)
						.execute((DataParticle) particle);
			}

			if (outputDataParticles != null) {
				result.addAll(outputDataParticles);
			}
		} else {
			logger.warn("unknown particle type ("
					+ particle.getClass().getName() + ") to process");
			return null;
		}

		return result;
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
	public void emitParticle(Particle particle) {
		collector.emit(ParticleMapper.particleToValues(particle,
				nrOfOutputFields));
	}

}
