package nl.tno.timeseries.testapp;

import java.util.List;
import java.util.Map;

import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.timer.TimedOperation;
import nl.tno.timeseries.timer.TimerControllerInterface;

public class MyOperation implements TimedOperation {
	private static final long serialVersionUID = 773649574489299505L;
	TimerControllerInterface timerController = null;
	private String channelId;

	@Override
	public void init(String channelID, long startTimestamp, @SuppressWarnings("rawtypes")Map stormConfig) {
		this.channelId = channelID;
		System.out.println("init myoperation at "+startTimestamp);
	}

	@Override
	public List<Particle> execute(DataParticle inputParticle) {
		if (inputParticle != null)  {
			System.out.println("Data particle received "+inputParticle);
		}
		return null;
	}

	@Override
	public void setTimerController(TimerControllerInterface timerController) {
		this.timerController = timerController;
		System.out.println("MyOperation.initTimer");
		timerController.registerOperationForRecurringTimerTask(channelId, 3, this);
		timerController.registerOperationForSingleTimerTask(channelId, 5, this);
	}

	
	@Override
	public List<Particle> doTimerRecurringTask(long timestamp) {
		System.out.println("Recurring task at "+timestamp);
		return null;
	}

	@Override
	public List<Particle> doTimerSingleTask(long timestamp) {
		System.out.println("Single task at "+timestamp);
		return null;
	}


}
