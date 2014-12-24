package nl.tno.timeseries.testapp;

import java.util.List;
import java.util.Map;

import nl.tno.sensorstorm.api.annotation.OperationDeclaration;
import nl.tno.sensorstorm.api.particles.DataParticle;
import nl.tno.sensorstorm.api.processing.SingleParticleOperation;
import nl.tno.sensorstorm.gracefullshutdown.GracefulShutdownParticleHandler;
import nl.tno.sensorstorm.gracefullshutdown.GracefullShutdownInterface;
import nl.tno.sensorstorm.timer.TimerControllerInterface;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;

@OperationDeclaration(inputs = { MyDataParticle.class }, outputs = {}, metaParticleHandlers = { GracefulShutdownParticleHandler.class })
public class MyGracefullShutdownOperation implements SingleParticleOperation,
		GracefullShutdownInterface {
	private static final long serialVersionUID = 773649574489299505L;
	TimerControllerInterface timerController = null;
	private String fieldGrouper;

	@Override
	public void init(String fieldGrouper, long startTimestamp,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration stormConfiguration) {
		this.fieldGrouper = fieldGrouper;
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
