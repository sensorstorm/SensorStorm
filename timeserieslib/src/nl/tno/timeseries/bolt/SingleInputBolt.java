package nl.tno.timeseries.bolt;

import java.util.List;
import java.util.Map;

import nl.tno.timeseries.model.Streamable;
import nl.tno.timeseries.operation.SingleInputOperation;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;

/**
 * A basic StreamableBolt implementation that works with single items received (hence maintains no
 * history of items received). The bolt will ask the provided {@link SingleInputOperation} implementation to
 * work on the input it received and produce zero or more results which will be emitted by the bolt. 
 * If an operation throws an exception the input will be failed in all other situations the input will be acked.
 * 
 * @author Corne Versloot
 *
 */
public class SingleInputBolt extends StreamableBolt {
	
	private static final long serialVersionUID = 8954087163234223475L;

	private SingleInputOperation<? extends Streamable> operation;

	/**
	 * Constructs a SingleInputOperation 
	 * @param operation the operation to be performed
	 */
	public SingleInputBolt(SingleInputOperation<? extends Streamable> operation){
		this.operation = operation;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	void prepare(Map stormConf, TopologyContext context) {
		try {
			operation.prepare(stormConf, context);
		} catch (Exception e) {
			logger.error("Unale to prepare Operation ", e);
		}		
	}
	

//	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(operation.getSerializer().getFields());
	}

//	@Override
	List<? extends Streamable> execute(Streamable input) throws Exception{
		List<? extends Streamable> result = operation.execute(input);
		// copy metadata from input to output if configured to do so
		for(Streamable s : result){
			for(String key : input.getMetadata().keySet()){
				if(!s.getMetadata().containsKey(key)){
					s.getMetadata().put(key, input.getMetadata().get(key));
				}
			}
		}
		return result;
	}

}
