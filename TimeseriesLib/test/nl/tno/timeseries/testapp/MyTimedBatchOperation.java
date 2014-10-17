package nl.tno.timeseries.testapp;

import java.util.List;
import java.util.Map;

import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.interfaces.BatchOperation;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.DataParticleBatch;
import nl.tno.timeseries.interfaces.OperationException;
import nl.tno.timeseries.timer.TimerControllerInterface;
import nl.tno.timeseries.timer.TimerParticleHandler;
import nl.tno.timeseries.timer.TimerTaskInterface;

@OperationDeclaration(inputs = { MyDataParticle.class }, outputs = {}, metaParticleHandlers = { TimerParticleHandler.class })
public class MyTimedBatchOperation implements BatchOperation,
		TimerTaskInterface {
	private static final long serialVersionUID = 773649574489299505L;
	TimerControllerInterface timerController = null;
	private String channelId;

	@Override
	public void init(String channelID,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration stormConfiguration) {
		this.channelId = channelID;
	}

	@Override
	public void prepareForFirstParticle(long startTimestamp)
			throws OperationException {
		System.out.println("init myTimedBatchOperation for channel "
				+ channelId + " at " + startTimestamp);
	}

	@Override
	public List<DataParticle> execute(DataParticleBatch inputParticles) {
		if (inputParticles != null) {
			System.out.print("Timed Batch Operation channel " + channelId
					+ " batch received :[");
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
		timerController.registerOperationForRecurringTimerTask(channelId, 1500,
				this);
		timerController.registerOperationForSingleTimerTask(channelId, 3300,
				this);
		System.out.println("Timers set");
	}

	@Override
	public List<DataParticle> doTimerRecurringTask(long timestamp) {
		System.out.println("Recurring task for channel " + channelId + " at "
				+ timestamp);
		return null;
	}

	@Override
	public List<DataParticle> doTimerSingleTask(long timestamp) {
		System.out.println("Single task for channel " + channelId + " at "
				+ timestamp);
		return null;
	}

}
