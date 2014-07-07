package nl.tno.timeseries.interfaces;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface Operation extends Serializable {
	
	public void init(String channelID, long startSequenceNr, @SuppressWarnings("rawtypes")Map stormConfig);
	
	public List<Particle> execute(DataParticle inputParticles);
	
}
