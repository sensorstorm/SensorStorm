package nl.tno.timeseries.batchers;

import java.io.Serializable;
import java.util.List;

import nl.tno.storm.configuration.api.StormConfiguration;
import nl.tno.timeseries.interfaces.Batcher;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.DataParticleBatch;

/**
 * This batcher is empty, it is just passes each particle directly into a batch
 * of one particle.
 * 
 * @author waaijbdvd
 * 
 */
public class EmptyBatcher implements Batcher, Serializable {

	private static final long serialVersionUID = 4857031702161919147L;

	@Override
	public void init(String channelID, long startSequenceNr,
			StormConfiguration stormConfiguration) {
	}

	/**
	 * Returns a list of one DataParticleBatch containing only the
	 * inputParticle.
	 */
	@Override
	public List<DataParticleBatch> batch(DataParticle inputParticle) {
		DataParticleBatch batchedParticles = new DataParticleBatch();
		batchedParticles.add(inputParticle);
		return java.util.Collections.singletonList(batchedParticles);
	}

}
