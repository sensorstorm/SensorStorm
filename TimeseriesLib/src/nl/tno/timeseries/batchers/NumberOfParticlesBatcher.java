package nl.tno.timeseries.batchers;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.storm.configuration.api.ZookeeperStormConfigurationAPI;
import nl.tno.timeseries.interfaces.Batcher;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.DataParticleBatch;
import nl.tno.timeseries.interfaces.FaultTolerant;

public class NumberOfParticlesBatcher implements Batcher, Serializable {

	private static final long serialVersionUID = 2852865728648428422L;
	private int nrOfParticlesToBatch;
	private List<DataParticle> buffer;

	@Override
	public void init(String channelID, long startSequenceNr,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ZookeeperStormConfigurationAPI stormConfiguration,
			FaultTolerant delegator) {
		// TODO haal dit uit de stormConfig
		nrOfParticlesToBatch = 2;

		buffer = new ArrayList<DataParticle>();
	}

	@Override
	public List<DataParticleBatch> batch(DataParticle inputParticle) {
		ArrayList<DataParticleBatch> result = new ArrayList<DataParticleBatch>();

		buffer.add(inputParticle);
		while (buffer.size() >= nrOfParticlesToBatch) {
			DataParticleBatch batchedParticles = new DataParticleBatch(
					buffer.subList(0, nrOfParticlesToBatch));
			buffer.removeAll(batchedParticles);
			result.add(batchedParticles);
		}

		return result;
	}

}
