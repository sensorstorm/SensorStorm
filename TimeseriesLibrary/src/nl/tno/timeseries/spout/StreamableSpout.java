package nl.tno.timeseries.spout;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import nl.tno.timeseries.TSConfig;
import nl.tno.timeseries.fetcher.Fetcher;
import nl.tno.timeseries.model.Streamable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Values;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Basic spout implementation that includes fault tolerance (if activated). Subclasses of do need not to worry about
 * fault tolerance. It should be noted that running the spout in fault tolerant mode will use more memory because emitted
 * tuples are cashed for a limited amount of time (which is configurable).   
 *  
 * @author corne versloot
 *
 * @param <OutputType> the objects emitted by the spout
 */
public class StreamableSpout implements IRichSpout{
	
	private static final long serialVersionUID = 2828206148753936815L;
	
	private Logger logger = LoggerFactory.getLogger(StreamableSpout.class);
	private Cache<Object, Object> tupleCache; // a cache holding emitted tuples so they can be replayed on failure
	protected SpoutOutputCollector collector;
	private boolean faultTolerant = false;
	private Fetcher fetcher;
	
	public StreamableSpout(Fetcher fetcher){
		this.fetcher = fetcher;
	}
	
	/**
	 * Indicates if this Spout must cache tuples it has emitted so they can be replayed on failure.
	 * This setting does not effect anchoring of tuples (which is always done to support TOPOLOGY_MAX_SPOUT_PENDING configuration) 
	 * @param faultTolerant
	 * @return
	 */
	public StreamableSpout setFaultTolerant(boolean faultTolerant){
		this.faultTolerant = faultTolerant;
		return this;
	}
	
	/**
	 * Configures the spout by fetching optional parameters from the provided configuration. If faultTolerant is true the open
	 * function will also construct the cache to hold the emitted tuples.
	 * Configuration options are:
	 * <ul>
	 * <li>stormcv.faulttolerant --> boolean: indicates if the spout must operate in fault tolerant mode (i.e. replay tuples after failure)</li>
	 * <li>stormcv.tuplecache.timeout --> long: timeout (seconds) for tuples in the cache </li>
	 * <li>stormcv.tuplecache.maxsize --> int: maximum number of tuples in the cache (used to avoid memory overload)</li>
	 * </ul>
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,	SpoutOutputCollector collector) {
		this.collector = collector;
		if(conf.containsKey(TSConfig.STORMCV_SPOUT_FAULTTOLERANT)){
			faultTolerant = (Boolean) conf.get(TSConfig.STORMCV_SPOUT_FAULTTOLERANT);
		}
		if(faultTolerant){
			long timeout = conf.get(TSConfig.STORMCV_CACHES_TIMEOUT_SEC) == null ? 30 : (Long)conf.get(TSConfig.STORMCV_CACHES_TIMEOUT_SEC);
			int maxSize = conf.get(TSConfig.STORMCV_CACHES_MAX_SIZE) == null ? 500 : ((Long)conf.get(TSConfig.STORMCV_CACHES_MAX_SIZE)).intValue();
			tupleCache = CacheBuilder.newBuilder()
					.maximumSize(maxSize)
					.expireAfterAccess(timeout, TimeUnit.SECONDS)
					.build();
		}

		// pass configuration to subclasses
		try {
			fetcher.prepare(conf, context);
		} catch (Exception e) {
			logger.warn("Unable to configure spout due to ", e);
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(fetcher.getSerializer().getFields());
	}
	
	@Override
	public void nextTuple(){
		List<Streamable> streamables = fetcher.fetchData();
		if (streamables != null) {
			for (Streamable streamable : streamables) {
				if(streamable != null) try {
					//System.out.println("SPOUT: "+streamable);
					Values values = fetcher.getSerializer().toTuple(streamable);
						String id = streamable.getStreamId()+"_"+streamable.getSequenceNr();
						if(faultTolerant && tupleCache != null) tupleCache.put(id, values);
						collector.emit(values, id);
				} catch (IOException e) {
					logger.warn("Unable to fetch next frame from queue due to: "+e.getMessage());
				}
			}
		}
	}
	
	@Override
	public void close() {
		if(faultTolerant && tupleCache != null){
			tupleCache.cleanUp();
		}
		fetcher.deactivate();
	}

	@Override
	public void activate() {
		fetcher.activate();
	}

	@Override
	public void deactivate() {
		fetcher.deactivate();
	}

	@Override
	public void ack(Object msgId) {
		//System.out.println("SPOUT: ACK "+msgId);
		if(faultTolerant && tupleCache != null){
			tupleCache.invalidate(msgId);
		}
	}

	@Override
	public void fail(Object msgId) {
		//System.out.println("SPOUT: FAIL "+msgId);
		if(faultTolerant && tupleCache != null && tupleCache.getIfPresent(msgId) != null){
			collector.emit((Values)tupleCache.getIfPresent(msgId), msgId);
		}
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
		
}
