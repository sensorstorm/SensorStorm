package nl.tno.timeseries.channels;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.tno.timeseries.interfaces.BatchOperation;
import nl.tno.timeseries.interfaces.Batcher;
import nl.tno.timeseries.interfaces.ChannelGrouper;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.Operation;
import nl.tno.timeseries.interfaces.OperationException;
import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.interfaces.SingleOperation;
import nl.tno.timeseries.mapper.ParticleMapper;
import nl.tno.timeseries.particles.MetaParticleUtil;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class MultipleOperationChannelBolt extends AbstractOperationChannelBolt {

	private static final long serialVersionUID = -5109656134961759532L;

	protected Class<? extends Operation> operationClass;
	protected int nrOfOutputFields;
	protected Class<? extends Batcher> batcherClass;
	protected Map<String, ChannelManager> channelManagers;
	protected Fields metaParticleFields;

	/**
	 * Construct a {@link MultipleOperationChannelBolt} with a {@link Batcher}
	 * 
	 * @param conf
	 *            Storm configuration map
	 * @param batcherClass
	 *            {@link Class} of the {@link Batcher} implementation
	 * @param batchOperationClass
	 *            {@link Class} of the {@link BatchOperation} implementation
	 * @throws OperationException
	 */
	public MultipleOperationChannelBolt(Config config,
			Class<? extends Batcher> batcherClass,
			Class<? extends BatchOperation> batchOperationClass)
			throws OperationException {
		if (batcherClass == null) {
			throw new OperationException("batcherClass may not be null");
		}
		if (batchOperationClass == null) {
			throw new OperationException("batchOperationClass may not be null");
		}

		// Set fields
		this.operationClass = batchOperationClass;
		this.batcherClass = batcherClass;
		this.channelManagers = new HashMap<String, ChannelManager>();
		this.metaParticleFields = MetaParticleUtil
				.registerMetaParticleFieldsWithOperationClass(config,
						operationClass);
	}

	/**
	 * Construct a {@link MultipleOperationChannelBolt} without a
	 * {@link Batcher}
	 * 
	 * @param conf
	 *            Storm configuration map
	 * @param operationClass
	 *            {@link Class} of the {@link Operation} implementation
	 * @throws OperationException
	 */
	public MultipleOperationChannelBolt(Config config,
			Class<? extends SingleOperation> operationClass)
			throws OperationException {
		if (operationClass == null) {
			throw new OperationException("operationClass may not be null");
		}

		// Set fields
		this.operationClass = operationClass;
		this.batcherClass = null;
		this.channelManagers = new HashMap<String, ChannelManager>();
		this.metaParticleFields = MetaParticleUtil
				.registerMetaParticleFieldsWithOperationClass(config,
						operationClass);
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormNativeConfig,
			TopologyContext context, OutputCollector collector) {
		super.prepare(stormNativeConfig, context, collector);
	}

	/**
	 * Handle the new incoming tuple
	 */
	@Override
	public void execute(Tuple tuple) {
		Particle inputParticle = ParticleMapper.tupleToParticle(tuple);

		// is there a particle to process?
		if (inputParticle != null) {
			String selectChannelManagerId = inputParticle.getChannelId();

			// determine if there was a channelGrouper in front of this bolt, if
			// so use the channelGroupId as selector for the channelManager
			String channelGroupId = null;
			try {
				channelGroupId = tuple
						.getStringByField(ChannelGrouper.GROUPED_PARTICLE_FIELD);
			} catch (IllegalArgumentException e) {
				channelGroupId = null;
			}
			if (channelGroupId != null) {
				selectChannelManagerId = channelGroupId;
			}

			// get a channel manager based on the channelId or channelGroupd
			ChannelManager channelManager = getChannelManager(selectChannelManagerId);

			// process the particle and emit the output
			List<Particle> outputParticles = channelManager
					.processParticle(inputParticle);
			emitParticles(tuple, outputParticles);
		}
	}

	/**
	 * Returns the channelmanager related to the channelId, or instantiate one
	 * if it was not present.
	 * 
	 * @param channelId
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private ChannelManager getChannelManager(String channelId) {
		ChannelManager channelManager = channelManagers.get(channelId);

		// no channel manager present yet for the channelId
		if (channelManager == null) {
			// is it a single operation
			if (SingleOperation.class.isAssignableFrom(operationClass)) {
				channelManager = new ChannelManager(channelId,
						(Class<? extends SingleOperation>) operationClass,
						stormNativeConfig, zookeeperStormConfiguration, this);
			}

			// is it a batch operation
			else if (BatchOperation.class.isAssignableFrom(operationClass)) {
				channelManager = new ChannelManager(channelId, batcherClass,
						(Class<? extends BatchOperation>) operationClass,
						stormNativeConfig, zookeeperStormConfiguration, this);
			} else {
				// Apparently a new constructor is added to create a new type of
				// operation
				logger.error("Internal error, unknown operation class "
						+ operationClass.getName());
			}

			// register the new channel manager for this channelId
			channelManagers.put(channelId, channelManager);
		}

		return channelManager;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// merge all output particle fields for DataParticles
		Fields fields = null;
		List<Class<? extends DataParticle>> outputParticles = ChannelManager
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

	@Override
	public void emitParticle(Tuple anchor, Particle particle) {
		if (particle != null) {
			collector
					.emit(anchor, ParticleMapper.particleToValues(particle,
							nrOfOutputFields));
		}
	}

	@Override
	public void emitParticle(Particle particle) {
		if (particle != null) {
			collector.emit(ParticleMapper.particleToValues(particle,
					nrOfOutputFields));
		}
	}

}
