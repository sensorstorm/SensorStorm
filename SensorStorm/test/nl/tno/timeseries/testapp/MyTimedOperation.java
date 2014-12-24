package nl.tno.timeseries.testapp;

import java.util.List;
import java.util.Map;

import nl.tno.sensorstorm.api.annotation.OperationDeclaration;
import nl.tno.sensorstorm.api.particles.DataParticle;
import nl.tno.sensorstorm.api.processing.SingleParticleOperation;
import nl.tno.sensorstorm.timer.TimerControllerInterface;
import nl.tno.sensorstorm.timer.TimerParticleHandler;
import nl.tno.sensorstorm.timer.TimerTaskInterface;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;

@OperationDeclaration(inputs = { MyDataParticle.class }, outputs = {}, metaParticleHandlers = { TimerParticleHandler.class })
public class MyTimedOperation implements SingleParticleOperation,
		TimerTaskInterface {
	private static final long serialVersionUID = 773649574489299505L;
	TimerControllerInterface timerController = null;
	private String fieldGrouperValue;

	@Override
	public void init(String fieldGrouperValue, long startTimestamp,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration stormConfiguration) {
		this.fieldGrouperValue = fieldGrouperValue;
		System.out.println("myTimedOperation.init for fieldGrouperValue "
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
		timerController.registerOperationForRecurringTimerTask(1500, this);
		timerController.registerOperationForSingleTimerTask(3300, this);
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
