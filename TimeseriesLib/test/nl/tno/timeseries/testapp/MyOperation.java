package nl.tno.timeseries.testapp;

import java.util.List;
import java.util.Map;

import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.SingleOperation;
import nl.tno.timeseries.timer.TimerControllerInterface;
import nl.tno.timeseries.timer.TimerParticleHandler;
import nl.tno.timeseries.timer.TimerTaskInterface;

@OperationDeclaration(inputs = { Measurement.class }, outputs = {}, metaParticleHandlers = { TimerParticleHandler.class })
public class MyOperation implements SingleOperation, TimerTaskInterface {
	private static final long serialVersionUID = 773649574489299505L;
	TimerControllerInterface timerController = null;
	private String channelId;

	@Override
	public void init(String channelID, long startTimestamp,
			@SuppressWarnings("rawtypes") Map stormConfig) {
		this.channelId = channelID;
		System.out.println("init myoperation for channel " + channelID + " at "
				+ startTimestamp);
	}

	@Override
	public List<DataParticle> execute(DataParticle inputParticle) {
		if (inputParticle != null) {
			if (inputParticle instanceof Measurement<?>) {
				System.out.println("Operation channel " + channelId
						+ " MeasurementT received " + inputParticle);
			} else {
				System.out.println("Operation channel " + channelId
						+ " Data particle received " + inputParticle);
			}
		}
		return null;
	}

	@Override
	public void setTimerController(TimerControllerInterface timerController) {
		this.timerController = timerController;
		timerController.registerOperationForRecurringTimerTask(channelId, 3,
				this);
		timerController.registerOperationForSingleTimerTask(channelId, 5, this);
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
