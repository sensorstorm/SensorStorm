package nl.tno.timeseries.interfaces;

import java.util.List;

public interface BatchOperation extends Operation {
	
	public List<DataParticle> execute(DataParticleBatch inputParticleBatch);
	
}
