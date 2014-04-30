package nl.tno.timeseries.bolt;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.tno.timeseries.model.Streamable;
import nl.tno.timeseries.serializer.StreamableSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import clojure.lang.PersistentArrayMap;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * A BaseRichBolt implementation that supports the use of Streamable objects (i.e. subclasses of nl.tno.stormcv.model.Streamable).
 * StreamableBolt supports fault tolerance if it is configured to do so and supports the serialization of model objects.  
 * 
 * @author corne versloot
 *
 */
public abstract class StreamableBolt extends BaseRichBolt{

	private static final long serialVersionUID = -5421951488628303992L;
	
	protected Logger logger = LoggerFactory.getLogger(StreamableBolt.class);
	protected HashMap<String, StreamableSerializer<? extends Streamable>> serializers = new HashMap<String, StreamableSerializer<? extends Streamable>>();
	protected OutputCollector collector;
	protected String boltName;
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.boltName = context.getThisComponentId();
		
		try{
			PersistentArrayMap map = (PersistentArrayMap)conf.get(Config.TOPOLOGY_KRYO_REGISTER);
			for(Object className : map.keySet()){
				serializers.put((String)className, (StreamableSerializer<? extends Streamable>)Class.forName((String)map.get(className)).newInstance());
			}
		}catch(Exception e){
			logger.error("Unable to prepare StreamableBolt due to ",e);
		}
		this.prepare(conf, context);
	}
	
	@SuppressWarnings({ "rawtypes" })
	@Override
	public void execute(Tuple input) {
		try{
			Streamable cvt = deserialize(input);
			List<? extends Streamable> results = execute(cvt);
			for(Streamable output : results){
				StreamableSerializer serializer = serializers.get(output.getClass().getName());
				if(serializers.containsKey(output.getClass().getName())){
					collector.emit(input, serializer.toTuple(output));
				}else{
					// TODO: what else?
				}
			}
			collector.ack(input);
		}catch(Exception e){
			logger.warn("Unable to process input", e);
			collector.fail(input);
		}
	}
	
	/**
	 * Deserializes a Tuple into a Streamable type
	 * @param tuple
	 * @return
	 * @throws IOException 
	 */
	protected Streamable deserialize(Tuple tuple) throws IOException{
		String typeName = tuple.getStringByField(StreamableSerializer.TYPE);
		return serializers.get(typeName).fromTuple(tuple);
	}
	
	/**
	 * Subclasses must implement this method which is responsible for analysis of 
	 * received Streamable objects. A single input object may result in zero or more
	 * resulting objects which will be serialized and emitted by this Bolt.
	 * 
	 * @param input
	 * @return
	 */
	abstract List<? extends Streamable> execute(Streamable input) throws Exception;
	
	@SuppressWarnings("rawtypes")
	abstract void prepare(Map stormConf, TopologyContext context);
}
