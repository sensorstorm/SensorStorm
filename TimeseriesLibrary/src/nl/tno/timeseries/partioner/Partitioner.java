package nl.tno.timeseries.partioner;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import nl.tno.timeseries.bolt.BatchInputBolt.History;
import nl.tno.timeseries.model.Streamable;

/**
 * Partitioner implementations are responsible to partition a provided set of Streamable items into zero or more batches
 * of items suitable for batch processing. An implementation of a Partitioner might partition the input into batches
 * of a specific size. The provided set is grouped in some way (done by the {@link BatchInputBolt})
 * and ordered ascending on sequenceNr.
 * <br/><b>Implementations must remove items that are no longer needed from the History to avoid item expiration and failure of received items</b>
 * <p>
 * Implementations typically base their results on the items recieved (size of the set, some particular ordering etc) but it is also possible to 
 * partition the input based on external criteria like a clock (all items received within one minute) or a specific 'marker' item recieved. 
 * 
 * @author Corne Versloot
 *
 */
public interface Partitioner extends Serializable{

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf) throws Exception;
	
	public List<List<Streamable>> partition(History history, List<Streamable> currentSet);
	
}
