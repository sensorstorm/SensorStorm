package nl.tno.timeseries.testapp;

import java.util.List;
import java.util.Map;

import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.Operation;
import nl.tno.timeseries.timer.TimerControllerInterface;
import nl.tno.timeseries.timer.TimerParticleHandler;
import nl.tno.timeseries.timer.TimerTaskInterface;

@OperationDeclaration(inputs = {MeasurementT.class}, outputs = {}, metaParticleHandlers = {TimerParticleHandler.class})
public class MyOperationT implements Operation, TimerTaskInterface {
	private static final long serialVersionUID = 773649574489299505L;
	TimerControllerInterface timerController = null;
	private String channelId;

	@Override
	public void init(String channelID, long startTimestamp, @SuppressWarnings("rawtypes")Map stormConfig) {
		this.channelId = channelID;
		System.out.println("init myoperation at "+startTimestamp);
	}

	@Override
	public List<DataParticle> execute(List<DataParticle> inputParticles) {
		if (inputParticles != null)  {
			for (DataParticle inputParticle : inputParticles) {
				if (inputParticle instanceof MeasurementT<?>) {
					System.out.println("MeasurementT received "+inputParticle);
				} else {
					System.out.println("Data particle received "+inputParticle);
				}
			}
		}
		return null;
	}

	@Override
	public void setTimerController(TimerControllerInterface timerController) {
		this.timerController = timerController;
		System.out.println("MyOperation.initTimer");
		timerController.registerOperationForRecurringTimerTask(channelId, 3, this);
		timerController.registerOperationForSingleTimerTask(channelId, 5, this);
//		timerController.registerOperationForRecurringTimerTask(channelId, 1250, this);
//		timerController.registerOperationForSingleTimerTask(channelId, 1500, this);
	}

	
	@Override
	public List<DataParticle> doTimerRecurringTask(long timestamp) {
		System.out.println("Recurring task for channel "+channelId+" at "+timestamp);
		return null;
	}

	@Override
	public List<DataParticle> doTimerSingleTask(long timestamp) {
		System.out.println("Single task for channel "+channelId+" at "+timestamp);
		return null;
	}

}
