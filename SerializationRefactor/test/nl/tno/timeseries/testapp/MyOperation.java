package nl.tno.timeseries.testapp;

import java.util.List;

import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.Operation;
import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.stormcomponents.OperationContext;

public class MyOperation implements Operation {
	private static final long serialVersionUID = 773649574489299505L;

	@Override
	public void init(String channelID, long startSequenceNr, OperationContext operationContext) {
	}

	@Override
	public List<Particle> execute(List<DataParticle> inputParticles) {
		if ((inputParticles != null) && (inputParticles.size() > 0)) {
			System.out.println("Data particle received "+inputParticles.get(0));
		}
		return null;
	}

}
