package nl.tno.timeseries.stormcomponents;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.MetaParticle;
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
	protected @SuppressWarnings("rawtypes") Map stormConfig;
	protected OutputCollector collector;
	protected String boltName;
	protected Class<? extends Operation> operationClass;
	protected Class<? extends Particle> outputParticleClass;
	private Map<String, OperationContext> operationInstances;


	public ChannelBolt(Class<? extends Operation> operationClass, Class<? extends Particle> outputParticleClass) {
		this.operationClass = operationClass;
		this.outputParticleClass = outputParticleClass;
		
		operationInstances = new HashMap<String, OperationContext>();
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes")Map conf, TopologyContext context, OutputCollector collector) {
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
					collector.emit(ParticleMapper.particleToValues(outputParticle));
				}
			}
		}
	}
	
	
	protected List<Particle> processParticle(Particle inputParticle) {
		String channelId = inputParticle.getChannelId();
		
		// get context corresponding to the channel 
		// create new operation context if it does not exists and add it to the list
		OperationContext operationContext = operationInstances.get(channelId);
		Operation operation;
		if (operationContext == null) {
			operationContext = new OperationContext(operationClass, stormConfig);
			operationInstances.put(channelId, operationContext);
			
			// init operation
			operation = operationContext.getOperation();
			operation.init(channelId, inputParticle.getSequenceNr(), operationContext);
		} else {
			operation = operationContext.getOperation();
		}

		// process input particle and return result particles
		List<Particle> result;
		if (inputParticle instanceof MetaParticle) {
			result = operationContext.processMetaParticle((MetaParticle)inputParticle);
		} else {
			List<DataParticle> inputParticles = new ArrayList<DataParticle>();
			inputParticles.add((DataParticle)inputParticle);
			result = operation.execute(inputParticles);
		}
		return result;
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields fields = ParticleMapper.getFields(outputParticleClass);
		//@TODO merge fields with MetaParticle fields
		declarer.declare(fields);
	}

}
