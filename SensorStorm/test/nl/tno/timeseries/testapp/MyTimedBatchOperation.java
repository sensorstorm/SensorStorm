package nl.tno.timeseries.testapp;

import java.util.List;
import java.util.Map;

import nl.tno.sensorstorm.api.annotation.OperationDeclaration;
import nl.tno.sensorstorm.api.particles.DataParticle;
import nl.tno.sensorstorm.api.particles.DataParticleBatch;
import nl.tno.sensorstorm.api.processing.ParticleBatchOperation;
import nl.tno.sensorstorm.timer.TimerControllerInterface;
import nl.tno.sensorstorm.timer.TimerParticleHandler;
import nl.tno.sensorstorm.timer.TimerTaskInterface;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;

@OperationDeclaration(inputs = { MyDataParticle.class }, outputs = {}, metaParticleHandlers = { TimerParticleHandler.class })
public class MyTimedBatchOperation implements ParticleBatchOperation,
		TimerTaskInterface {
	private static final long serialVersionUID = 773649574489299505L;
	TimerControllerInterface timerController = null;
	private String fieldGrouper;

	@Override
	public void init(String fieldGrouper, long startTimestamp,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration stormConfiguration) {
		this.fieldGrouper = fieldGrouper;
		System.out.println("init myTimedBatchOperation for fieldGrouper "
				+ fieldGrouper + " at " + startTimestamp);
	}

	@Override
	public List<DataParticle> execute(DataParticleBatch inputParticles) {
		if (inputParticles != null) {
			System.out.print("Timed Batch Operation fieldGrouper "
					+ fieldGrouper + " batch received :[");
			for (DataParticle inputParticle : inputParticles) {
				System.out.print(inputParticle + ", ");
			}
			System.out.println("]");
		}
		return null;
	}

	@Override
	public void setTimerController(TimerControllerInterface timerController) {
		this.timerController = timerController;
		timerController.registerOperationForRecurringTimerTask(1500, this);
		timerController.registerOperationForSingleTimerTask(3300, this);
		System.out.println("Timers set");
	}

	@Override
	public List<DataParticle> doTimerRecurringTask(long timestamp) {
		System.out.println("Recurring task for fieldGrouper " + fieldGrouper
				+ " at " + timestamp);
		return null;
	}

	@Override
	public List<DataParticle> doTimerSingleTask(long timestamp) {
		System.out.println("Single task for fieldGrouper " + fieldGrouper
				+ " at " + timestamp);
		return null;
	}

}
