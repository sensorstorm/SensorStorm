package nl.tno.timeseries.stormcomponents;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.tno.timeseries.batchers.EmptyBatcher;
import nl.tno.timeseries.interfaces.BatchOperation;
import nl.tno.timeseries.interfaces.Batcher;
import nl.tno.timeseries.interfaces.ChannelGrouper;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.EmitParticleInterface;
import nl.tno.timeseries.interfaces.Operation;
import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.interfaces.SingleOperation;
import nl.tno.timeseries.mapper.ParticleMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ChannelBolt extends BaseRichBolt implements EmitParticleInterface {

	private static final long serialVersionUID = -5109656134961759532L;

	protected Logger logger = LoggerFactory.getLogger(ChannelBolt.class);
	protected @SuppressWarnings("rawtypes")
	Map stormConfig;
	protected OutputCollector collector;
	protected String boltName;
	protected Class<? extends Operation> operationClass;
	protected int nrOfOutputFields;
	protected Class<? extends Batcher> batcherClass;
	protected Map<String, ChannelManager> channelManagers;
	protected Fields metaParticleFields;

	/**
	 * Construct a {@link ChannelBolt} with a {@link Batcher}
	 * 
	 * @param conf
	 *            Storm configuration map
	 * @param batcherClass
	 *            {@link Class} of the {@link Batcher} implementation
	 * @param batchOperationClass
	 *            {@link Class} of the {@link BatchOperation} implementation
	 */
	public ChannelBolt(Config conf, Class<? extends Batcher> batcherClass,
			Class<? extends BatchOperation> batchOperationClass) {
		// Set fields
		this.operationClass = batchOperationClass;
		this.batcherClass = batcherClass;
		this.channelManagers = new HashMap<String, ChannelManager>();
		this.metaParticleFields = MetaParticleUtil.setMetaParticleFields(conf, operationClass);
	}

	/**
	 * Construct a {@link ChannelBolt} without a {@link Batcher}
	 * 
	 * @param conf
	 *            Storm configuration map
	 * @param operationClass
	 *            {@link Class} of the {@link Operation} implementation
	 */
	public ChannelBolt(Config conf, Class<? extends Operation> operationClass) {
		// Set fields
		this.operationClass = operationClass;
		this.batcherClass = EmptyBatcher.class;
		this.channelManagers = new HashMap<String, ChannelManager>();
		this.metaParticleFields = MetaParticleUtil.setMetaParticleFields(conf, operationClass);
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.stormConfig = conf;
		this.collector = collector;
		this.boltName = context.getThisComponentId();
	}

	@Override
	public void execute(Tuple tuple) {
		Particle inputParticle = ParticleMapper.tupleToParticle(tuple);
		if (inputParticle != null) {
			String selectChannelManagerId = inputParticle.getChannelId();
			// determine if there was a channelGrouper in front of this bolt, if
			// so use the channelGroup as grouper.
			String channelGroupId;
			try {
				channelGroupId = tuple.getStringByField(ChannelGrouper.GROUPED_PARTICLE_FIELD);
			} catch (IllegalArgumentException e) {
				channelGroupId = null;
			}
			if (channelGroupId != null) {
				selectChannelManagerId = channelGroupId;
			}

			ChannelManager channelManager = getChannelManager(selectChannelManagerId);
			List<Particle> outputParticles = channelManager.processParticle(inputParticle);
			if (outputParticles != null) {
				for (Particle outputParticle : outputParticles) {
					emitParticle(outputParticle);
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private ChannelManager getChannelManager(String channelId) {
		ChannelManager channelManager = channelManagers.get(channelId);
		if (channelManager == null) {
			if (SingleOperation.class.isAssignableFrom(operationClass)) {
				channelManager = new ChannelManager(channelId, (Class<? extends SingleOperation>) operationClass,
						stormConfig, this);
			} else if (BatchOperation.class.isAssignableFrom(operationClass)) {
				channelManager = new ChannelManager(channelId, batcherClass,
						(Class<? extends BatchOperation>) operationClass, stormConfig, this);
			} else {
				logger.error("Unknown operation " + operationClass.getName());
			}
			channelManagers.put(channelId, channelManager);
		}
		return channelManager;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// merge all output particle fields for DataParticles
		Fields fields = null;
		List<Class<? extends DataParticle>> outputParticles = ChannelManager.getOutputDataParticles(operationClass);
		for (Class<? extends DataParticle> outputParticleClass : outputParticles) {
			if (fields == null) {
				fields = ParticleMapper.getFields(outputParticleClass);
			} else {
				fields = ParticleMapper.mergeFields(fields, ParticleMapper.getFields(outputParticleClass));
			}
		}

		// Add fields for MetaParticles
		if (fields == null) {
			fields = this.metaParticleFields;
		} else {
			fields = ParticleMapper.mergeFields(fields, this.metaParticleFields);
		}

		nrOfOutputFields = fields.size();
		declarer.declare(fields);
	}

	@Override
	public void emitParticle(Particle particle) {
		collector.emit(ParticleMapper.particleToValues(particle, nrOfOutputFields));
	}

}
