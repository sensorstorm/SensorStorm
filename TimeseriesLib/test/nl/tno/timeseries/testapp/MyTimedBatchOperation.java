package nl.tno.timeseries.testapp;

import java.util.List;
import java.util.Map;

import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.operations.BatchOperation;
import nl.tno.timeseries.operations.OperationException;
import nl.tno.timeseries.particles.DataParticle;
import nl.tno.timeseries.particles.DataParticleBatch;
import nl.tno.timeseries.particles.timer.TimerControllerInterface;
import nl.tno.timeseries.particles.timer.TimerParticleHandler;
import nl.tno.timeseries.particles.timer.TimerTaskInterface;

@OperationDeclaration(inputs = { MyDataParticle.class }, outputs = {}, metaParticleHandlers = { TimerParticleHandler.class })
public class MyTimedBatchOperation implements BatchOperation,
		TimerTaskInterface {
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
		timerController.registerOperationForRecurringTimerTask(fieldGrouper,
				1500, this);
		timerController.registerOperationForSingleTimerTask(fieldGrouper, 3300,
				this);
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
