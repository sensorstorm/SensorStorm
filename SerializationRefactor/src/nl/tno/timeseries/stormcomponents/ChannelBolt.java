package nl.tno.timeseries.stormcomponents;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.MetaParticle;
import nl.tno.timeseries.interfaces.MetaParticleHandler;
import nl.tno.timeseries.interfaces.Operation;
import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.mapper.ParticleMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ChannelBolt extends BaseRichBolt {
	private static final long serialVersionUID = -5109656134961759532L;

	protected Logger logger = LoggerFactory.getLogger(ChannelBolt.class);
	protected @SuppressWarnings("rawtypes")
	Map stormConfig;
	protected OutputCollector collector;
	protected String boltName;
	protected Class<? extends Operation> operationClass;
	protected Class<? extends Particle> outputParticleClass;
	private Map<String, Operation> operations;
	private Map<Class<? extends MetaParticle>, MetaParticleHandler> metaProcessors;
	protected int nrOfOutputFields;

	public ChannelBolt(Class<? extends Operation> operationClass, Class<? extends Particle> outputParticleClass) {
		this.operationClass = operationClass;
		this.outputParticleClass = outputParticleClass;

		operations = new HashMap<String, Operation>();
		metaProcessors = new HashMap<Class<? extends MetaParticle>, MetaParticleHandler>();
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
			List<Particle> outputParticles = null;

			outputParticles = processParticle(inputParticle);

			if (outputParticles != null) {
				for (Particle outputParticle : outputParticles) {
					collector.emit(ParticleMapper.particleToValues(outputParticle, nrOfOutputFields));
				}
			}
		}
	}

	protected void addMetaProcessor(Class<? extends MetaParticle> metaParticle, MetaParticleHandler metaProcessor) {
		metaProcessors.put(metaParticle, metaProcessor);
	}

	protected List<Particle> processParticle(Particle inputParticle) {
		Operation operation = getOperation(inputParticle);

		// process input particle and return result particles
		List<Particle> result = null;
		if (inputParticle instanceof MetaParticle) {
			List<Particle> metaParticleProcessorResult = null;
			result = new ArrayList<>();
			MetaParticleHandler metaParticleProcessor = metaProcessors.get(inputParticle.getClass());
			if (metaParticleProcessor != null) {
				metaParticleProcessorResult = metaParticleProcessor.execute((MetaParticle) inputParticle);
			}
			// pass metaParticle on further in the topology, make sure the metaParticle is in front of the list
			result.add(inputParticle);
			result.addAll(metaParticleProcessorResult);
		} else if (inputParticle instanceof DataParticle) {
			result = operation.execute((DataParticle) inputParticle);
		}
		return result;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields fields = ParticleMapper.getFields(outputParticleClass);

		for (Class<? extends MetaParticle> metaParticleClass : metaProcessors.keySet()) {
			fields = ParticleMapper.mergeFields(fields, ParticleMapper.getFields(metaParticleClass));
		}

		nrOfOutputFields = fields.size();
		declarer.declare(fields);
	}

	protected Operation getOperation(Particle inputParticle) {
		String channelId = inputParticle.getChannelId();
		Operation operation = operations.get(channelId);
		if (operation == null) { // first time this operations is used for this
									// channelId
			try {
				operation = operationClass.newInstance();
				initOperation(operation, inputParticle);
				operations.put(channelId, operation);
			} catch (InstantiationException | IllegalAccessException e) {
				logger.error("Can not instantiate Operation class " + operationClass.getName());
				operation = null;
			}
		}
		return operation;
	}

	/**
	 * This method initializes the operation. It can be overriden if additional
	 * initialization is needed.
	 * 
	 * @param operation
	 * @param inputParticle
	 */
	protected void initOperation(Operation operation, Particle inputParticle) {
		operation.init(inputParticle.getChannelId(), inputParticle.getSequenceNr(), stormConfig);
	}

}
