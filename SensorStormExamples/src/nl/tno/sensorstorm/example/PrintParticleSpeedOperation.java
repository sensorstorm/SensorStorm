package nl.tno.sensorstorm.example;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import nl.tno.sensorstorm.annotation.OperationDeclaration;
import nl.tno.sensorstorm.operations.OperationException;
import nl.tno.sensorstorm.operations.SingleParticleOperation;
import nl.tno.sensorstorm.particles.DataParticle;
import nl.tno.sensorstorm.particles.timer.TimerControllerInterface;
import nl.tno.sensorstorm.particles.timer.TimerParticleHandler;
import nl.tno.sensorstorm.particles.timer.TimerTaskInterface;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;

@OperationDeclaration(inputs = SensorParticle.class, outputs = SensorParticle.class, metaParticleHandlers = TimerParticleHandler.class)
public class PrintParticleSpeedOperation implements SingleParticleOperation,
		TimerTaskInterface {

	private static final int BUCKET = 1000;
	private static final long serialVersionUID = -5799424083707865813L;
	private AtomicInteger cnt;

	@Override
	public void init(String fieldGrouperValue, long startTimeStamp,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration zookeeperStormConfiguration)
			throws OperationException {
		cnt = new AtomicInteger(0);
	}

	@Override
	public List<? extends DataParticle> execute(DataParticle inputParticle)
			throws OperationException {
		cnt.incrementAndGet();
		return Collections.singletonList(inputParticle);
	}

	@Override
	public void setTimerController(TimerControllerInterface timerController) {
		timerController.registerOperationForRecurringTimerTask(BUCKET, this);
	}

	@Override
	public List<DataParticle> doTimerRecurringTask(long timestamp) {
		System.err.println(timestamp
				+ ": Number of precessed particles since the last " + BUCKET
				+ " milliseconds: " + cnt.getAndSet(0));
		return null;
	}

	@Override
	public List<DataParticle> doTimerSingleTask(long timestamp) {
		// nothing to do
		return null;
	}

}
