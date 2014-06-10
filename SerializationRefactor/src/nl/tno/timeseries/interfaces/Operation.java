package nl.tno.timeseries.interfaces;

import java.io.Serializable;
import java.util.List;

import nl.tno.timeseries.stormcomponents.OperationContext;

public interface Operation extends Serializable {
	
	public void init(String channelID, long startSequenceNr, OperationContext operationContext);
	
	public List<Particle> execute(List<DataParticle> inputParticles);

}
