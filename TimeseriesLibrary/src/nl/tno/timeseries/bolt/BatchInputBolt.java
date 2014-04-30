package nl.tno.timeseries.bolt;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import nl.tno.timeseries.TSConfig;
import nl.tno.timeseries.model.Streamable;
import nl.tno.timeseries.operation.BatchOperation;
import nl.tno.timeseries.partioner.Partitioner;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * A StreamableBolt that stores all input in the History until some criteria are met. Items will remain in the History for a 
 * predefined period (stormcv.caches.timeout configuration key) after which they will be removed and failed automatically. 
 * The BatchInputBolt works like follows:
 * <ol>
 * <li>The execute(Tuple) function serializes the Tuple to a Streamable object</li>
 * <li>This object is added to the History based on the specified Group (i.e. set of fields, similar to fieldsGrouping used within storm).
 * Items in a group are ordered ASC based on their sequenceNr. If refreshExpirationOfOlderItems is true any existing items in the group with 
 * higher sequenceNr than the new one will have their expiration timer reset.</li>
 * <li>The provided {@link Partitioner} is called on the grouped items the new item was added to. The Partitioner is responsible for the
 * creation of appropriate batches which typically depend on the {@link BatchOperation} provided on construction. The Partitioner may return
 * zero or more batches and is responsible for the removal of items from the History that are no longer needed which will result in an 
 * ACK on that tuple. Not doing so will result in expiration of items from the cache which in turn will cause FAILures of tuples.</li>
 * <li>The BatchOperation is called for each batch and the results are emitted by the bolt</li>
 * </ol>
 * <p/>
 * An example setup might be to have a window Partitioner that waits until 10 input items have been received after which 
 * a BatchOperation calculates some average value for it. The partitioner will remove the 10 items from the history and
 * will wait for the next 10 etc.
 * 
 * <p/><b>Important: </b> due to the parallelism it is very unlikely that items will be received in their original order which can may result in
 * the scenario that items are evicted from the cache before the Partitioner could add them to a valid partition. Setting 
 * refreshExpirationOfOlderItems to true will <i>minimize</i> this problem by resetting the timer on all items in the history of a group when a new 
 * item with lower sequenceNr is received. This will however use more memory and does not solve the underlying problem of having a 'Bolt that is to slow' 
 * somewhere in the preceding topology. Using higher parallelism hints or setting setMaxSpoutPending() for the topology can truly solve the problem. 
 * 
 * <p/><b><Configuration:</b><br/>
 * Parameters set through storm configuration:
 * <ul>
 * <li>StreamableSpout.TOPOLOGY_CACHES_TIMEOUT_SEC (Integer): sets the timeout in seconds for items cashed by the History. The default is 20 seconds</li>
 * <li>StreamableSpout.TOPOLOGY_CACHES_MAXSIZE (Integer): sets the maximum size of the History cache (to avoid memory overload). The default is 252</li>
 * </ul>
 * 
 * @author Corne Versloot
 *
 */
public class BatchInputBolt extends StreamableBolt implements RemovalListener<Streamable, String>{

	private static final long serialVersionUID = -2394218774274388493L;

	private BatchOperation<? extends Streamable> operation;
	private Partitioner partitioner;
	private int TTL = 29;
	private int maxSize = 499;
	private Fields groupBy;
	private History history;
	private boolean refreshExperation = true;
	
	/**
	 * Creates a BatchInputBolt with given Partitioner and BatchOperation.
	 * @param partitioner
	 * @param operation
	 * @param refreshExpirationOfOlderItems
	 */
	public BatchInputBolt(Partitioner partitioner, BatchOperation<? extends Streamable> operation){
		this.operation = operation;
		this.partitioner = partitioner;
	}
	
	/**
	 * Sets the time to live for items being cached by this bolt
	 * @param ttl
	 * @return
	 */
	public BatchInputBolt ttl(int ttl){
		this.TTL = ttl;
		return this;
	}
	
	/**
	 * Specifies the maximum size of the cashe used. Hitting the maximum will cause oldest items to be 
	 * expired from the cache
	 * @param size
	 * @return
	 */
	public BatchInputBolt maxCacheSize(int size){
		this.maxSize = size;
		return this;
	}
	
	/**
	 * Specifies the fields used to group items on. 
	 * @param group
	 * @return
	 */
	public BatchInputBolt groupBy(Fields group){
		this.groupBy = group;
		return this;
	}
	
	/**
	 * Specifies weather items with hither sequence number within a group must have their
	 * ttl's refreshed if an item with lower sequence number is added
	 * @param refresh
	 * @return
	 */
	public BatchInputBolt refreshExpiration(boolean refresh){
		this.refreshExperation = refresh;
		return this;
	}
	
	
	@SuppressWarnings("rawtypes")
	@Override
	void prepare(Map conf, TopologyContext context) {
		// use TTL and maxSize from config if they were not set explicitly using the constructor (implicit way of doing this...)
		if(TTL == 21) TTL = conf.get(TSConfig.STORMCV_CACHES_TIMEOUT_SEC) == null ? TTL : ((Long)conf.get(TSConfig.STORMCV_CACHES_TIMEOUT_SEC)).intValue();
		if(maxSize == 256) maxSize = conf.get(TSConfig.STORMCV_CACHES_MAX_SIZE) == null ? maxSize : ((Long)conf.get(TSConfig.STORMCV_CACHES_MAX_SIZE)).intValue();
		history = new History(this);
		
		// IF NO grouping was set THEN select the first grouping registered for the spout as the grouping used within the Spout (usually a good guess)
		if(groupBy == null){
			Map<GlobalStreamId, Grouping> sources = context.getSources(context.getThisComponentId());
			for(GlobalStreamId id : sources.keySet()){
				Grouping grouping = sources.get(id);
				this.groupBy = new Fields(grouping.get_fields());
				break;
			}
		}
		
		// prepare the selector and operation
		try {
			partitioner.prepare(conf);
			operation.prepare(conf, context);
		} catch (Exception e) {
			logger.error("Unable to preapre the Selector or Operation", e);
		}
	}

//	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(operation.getSerializer().getFields());
	}

	/**
	 * Overloaded from superclass, adds the input to the history and asks the selector to create batches (if possible)
	 * for all the items in the history. If one or multiple batches could be created the operation
	 * will be executed for each batch and the results are emitted by this bolt.
	 */
//	@Override
	public void execute(Tuple input) {
		String group = generateKey(input);
		if(group == null){
			collector.ack(input);
			return;
		}
	
		Streamable streamable;
		try {
			streamable = deserialize(input);
			history.add(group, streamable);
			List<List<Streamable>> batches = partitioner.partition(history, history.getGroupedItems(group));
			for(List<Streamable> batch : batches){
				try{
					List<? extends Streamable> results = operation.execute(batch);
					for(Streamable result : results){
						collector.emit(input, serializers.get(result.getClass().getName()).toTuple(result));
					}
				}catch(Exception e){
					logger.warn("Unable to to process batch due to ", e);
				}
			}
		} catch (IOException e1) {
			logger.warn("Unable to deserialize Tuple", e1);
		}
		
	}
	
	@Override
	List<? extends Streamable> execute(Streamable input) {
		// TODO Auto-generated method stub
		return null;
	}
	
	/**
	 * Generates the key for the provided tuple using the fields provided at construction time
	 * @param tuple
	 * @return key created for this tuple or NULL if no key could be created (i.e. tuple does not contain any of groupBy Fields)
	 */
	private String generateKey(Tuple tuple){
		String key = new String();
		for(String field : groupBy){
			key += tuple.getValueByField(field)+"_";
		}
		if(key.length() == 0) return null;
		return key;
	}
	
	/**
	 * Callback method for removal of items from the histories cache. Items removed from the cache need to be acked or failed
	 * according to the reason they were removed
	 */
//	@Override
	public void onRemoval(RemovalNotification<Streamable, String> notification) {
		// make sure the streamable object is removed from the history (even if removal was automatic!)
		history.clear(notification.getKey(), notification.getValue());
		if(notification.getCause() == RemovalCause.EXPIRED || notification.getCause() == RemovalCause.SIZE){
			// item removed automatically --> fail the tuple
			collector.fail(notification.getKey().getTuple());
		}else{
			// item removed explicitly --> ack the tuple
			collector.ack(notification.getKey().getTuple());
		}
	}
	
	// ----------------------------- HISTORY CONTAINER ---------------------------
	/**
	 * Container that manages the history of tuples received and allows others to clean up the history retained.
	 *  
	 * @author Corne Versloot
	 *
	 */
	public class History implements Serializable{
		
		private static final long serialVersionUID = 5353548571380002710L;
		
		private Cache<Streamable, String> inputCache;
		private HashMap<String, List<Streamable>> groups;
		
		/**
		 * Creates a History object for the specified Bolt (which is used to ack or fail items removed from the history).
		 * @param bolt
		 */
		private History(BatchInputBolt bolt){
			groups = new HashMap<String, List<Streamable>>();
			inputCache = CacheBuilder.newBuilder()
					.maximumSize(maxSize)
					.expireAfterAccess(TTL, TimeUnit.SECONDS) // resets also on get(...)!
					.removalListener(bolt)
					.build();
		}
		
		/**
		 * Adds the new streamable object to the history and returns the list of items it was grouped with. 
		 * @param group the name of the group the streamable belongs to
		 * @param streamable the streamable object that needs to be added to the history.
		 */
		private void add(String group, Streamable streamable){
			if(!groups.containsKey(group)){
				groups.put(group, new ArrayList<Streamable>());
			}
			List<Streamable> list = groups.get(group);
			int i;
			for(i = list.size()-1; i>=0; i--){
				if( streamable.getSequenceNr() > list.get(i).getSequenceNr()){
					list.add(i+1, streamable);
					break;
				}
				if(refreshExperation){
					inputCache.getIfPresent(list.get(i)); // touch the item passed in the cache to reset its expiration timer
				}
			}
			if(i < 0) list.add(0, streamable);
			inputCache.put(streamable, group);
		}
		
		/**
		 * Removes the object from the history. This will tricker an ACK to be send. 
		 * @param streamable
		 */
		public void removeFromHistory(Streamable streamable){
			inputCache.invalidate(streamable);
		}
		
		/**
		 * Removes the object from the group
		 * @param streamable
		 * @param group
		 */
		private void clear(Streamable streamable, String group){
			if(!groups.containsKey(group)) return;
			groups.get(group).remove(streamable);
			if(groups.get(group).size() == 0) groups.remove(group);
		}
		
		/**
		 * Returns all the items in this history that belong to the specified group
		 * @param group
		 * @return
		 */
		public List<Streamable> getGroupedItems(String group){
			return groups.get(group); 
		}
		
		public long size(){
			return inputCache.size();
		}
		
		public String toString(){
			String result = "";
			for(String group : groups.keySet()){
				result += "  "+group+" : "+groups.get(group).size()+"\r\n";
			}
			return result;
		}
	}// end of History class

}
