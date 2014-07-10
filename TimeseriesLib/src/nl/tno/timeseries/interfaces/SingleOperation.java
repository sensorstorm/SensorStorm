package nl.tno.timeseries.interfaces;

import java.util.List;

public interface SingleOperation extends Operation {
	
	public List<DataParticle> execute(DataParticle inputParticles);
	
}
