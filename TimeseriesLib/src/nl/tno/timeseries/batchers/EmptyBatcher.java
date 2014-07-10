package nl.tno.timeseries.batchers;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.timeseries.interfaces.Batcher;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.DataParticleBatch;

public class EmptyBatcher implements Batcher, Serializable {

	private static final long serialVersionUID = 4857031702161919147L;

	@Override
	public void init(String channelID, long startSequenceNr, @SuppressWarnings("rawtypes")Map stormConfig) {
		
	}

	@Override
	public List<DataParticleBatch> batch(DataParticle inputParticle) {
		ArrayList<DataParticleBatch> result = new ArrayList<DataParticleBatch>();
		DataParticleBatch batchedParticles = new DataParticleBatch();
		batchedParticles.add(inputParticle);
		result.add(batchedParticles);
		return result;
	}

}
