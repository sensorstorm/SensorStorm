package nl.tno.timeseries.interfaces;

import java.util.List;
import java.util.Map;

public interface Batcher {

	public void init(String channelID, long startSequenceNr, @SuppressWarnings("rawtypes")Map stormConfig);

	public List<List<DataParticle>> batch(DataParticle inputParticle);
	

}
