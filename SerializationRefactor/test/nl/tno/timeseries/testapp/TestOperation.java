package nl.tno.timeseries.testapp;

import java.util.List;
import java.util.Map;

import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.Operation;
import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.timer.TimerParticleHandler;

@OperationDeclaration(inputs = MeasurementT.class, outputs = {}, metaParticleHandlers = { TimerParticleHandler.class })
public class TestOperation implements Operation {

	@Override
	public void init(String channelID, long startSequenceNr, Map stormConfig) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<Particle> execute(DataParticle inputParticles) {
		// TODO Auto-generated method stub
		return null;
	}

}
