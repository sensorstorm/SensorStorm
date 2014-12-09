package nl.tno.timeseries.testapp;

import java.util.List;
import java.util.Map;

import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.operations.OperationException;
import nl.tno.timeseries.operations.SingleOperation;
import nl.tno.timeseries.particles.DataParticle;
import nl.tno.timeseries.particles.timer.TimerControllerInterface;
import nl.tno.timeseries.particles.timer.TimerParticleHandler;
import nl.tno.timeseries.particles.timer.TimerTaskInterface;

@OperationDeclaration(inputs = { MyDataParticle.class }, outputs = {}, metaParticleHandlers = { TimerParticleHandler.class })
public class MyTimedOperation implements SingleOperation, TimerTaskInterface {
	private static final long serialVersionUID = 773649574489299505L;
	TimerControllerInterface timerController = null;
	private String fieldGrouperValue;

	@Override
	public void init(String fieldGrouperValue,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration stormConfiguration) {
		this.fieldGrouperValue = fieldGrouperValue;
		System.out.println("myTimedOperation.init for fieldGrouperValue "
				+ fieldGrouperValue);
	}

	@Override
	public void prepareForFirstParticle(long startTimestamp)
			throws OperationException {
		System.out
				.println("myTimedOperation.prepareForFirstParticle for fieldGrouperValue "
						+ fieldGrouperValue + " at " + startTimestamp);
	}

	@Override
	public List<DataParticle> execute(DataParticle inputParticle) {
		if (inputParticle != null) {
			if (inputParticle instanceof MyDataParticle<?>) {
				System.out.println("myTimedOperation.MyDataParticle received "
						+ inputParticle);
			} else {
				System.out.println("myTimedOperation.Data particle received "
						+ inputParticle);
			}
		}
		return null;
	}

	@Override
	public void setTimerController(TimerControllerInterface timerController) {
		this.timerController = timerController;
		timerController.registerOperationForRecurringTimerTask(
				fieldGrouperValue, 1500, this);
		timerController.registerOperationForSingleTimerTask(fieldGrouperValue,
				3300, this);
		System.out.println("myTimedOperation.Timers set");
	}

	@Override
	public List<DataParticle> doTimerRecurringTask(long timestamp) {
		System.out
				.println("myTimedOperation.Recurring task for fieldGrouperValue "
						+ fieldGrouperValue + " at " + timestamp);
		return null;
	}

	@Override
	public List<DataParticle> doTimerSingleTask(long timestamp) {
		System.out
				.println("myTimedOperation.Single task for fieldGrouperValue "
						+ fieldGrouperValue + " at " + timestamp);
		return null;
	}

}
