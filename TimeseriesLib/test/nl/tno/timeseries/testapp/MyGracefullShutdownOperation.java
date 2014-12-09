package nl.tno.timeseries.testapp;

import java.util.List;
import java.util.Map;

import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.operations.OperationException;
import nl.tno.timeseries.operations.SingleOperation;
import nl.tno.timeseries.particles.DataParticle;
import nl.tno.timeseries.particles.gracefullshutdown.GracefulShutdownParticleHandler;
import nl.tno.timeseries.particles.gracefullshutdown.GracefullShutdownInterface;
import nl.tno.timeseries.particles.timer.TimerControllerInterface;

@OperationDeclaration(inputs = { MyDataParticle.class }, outputs = {}, metaParticleHandlers = { GracefulShutdownParticleHandler.class })
public class MyGracefullShutdownOperation implements SingleOperation,
		GracefullShutdownInterface {
	private static final long serialVersionUID = 773649574489299505L;
	TimerControllerInterface timerController = null;
	private String fieldGrouper;

	@Override
	public void init(String fieldGrouper,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration stormConfiguration) {
		this.fieldGrouper = fieldGrouper;
	}

	@Override
	public void prepareForFirstParticle(long startTimestamp)
			throws OperationException {
		System.out.println("init myoperation for fieldGrouper " + fieldGrouper
				+ " at " + startTimestamp);
	}

	@Override
	public List<DataParticle> execute(DataParticle inputParticle) {
		if (inputParticle != null) {
			if (inputParticle instanceof MyDataParticle<?>) {
				System.out.println("Operation fieldGrouper " + fieldGrouper
						+ " MyDataParticle received " + inputParticle);
			} else {
				System.out.println("Operation fieldGrouper " + fieldGrouper
						+ " Data particle received " + inputParticle);
			}
		}
		return null;
	}

	@Override
	public void gracefullShutdown() {
		System.out.println("fieldGrouper " + fieldGrouper
				+ " Gracefull shutdown!");
	}

}
