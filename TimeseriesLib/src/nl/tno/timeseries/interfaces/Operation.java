package nl.tno.timeseries.interfaces;

import java.io.Serializable;
import java.util.Map;

public interface Operation extends Serializable {
	
	public void init(String channelID, long startSequenceNr, @SuppressWarnings("rawtypes")Map stormConfig);
	
}
