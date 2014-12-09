package nl.tno.timeseries.particles.timer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import nl.tno.timeseries.annotation.MetaParticleHandlerDecleration;
import nl.tno.timeseries.operations.Operation;
import nl.tno.timeseries.particles.DataParticle;
import nl.tno.timeseries.particles.MetaParticle;
import nl.tno.timeseries.particles.MetaParticleHandler;
import nl.tno.timeseries.particles.Particle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This handler handles TimerTickParticles and uses them to service single and
 * recurring requests from the related operation to the handler.
 * 
 * @author waaijbdvd
 * 
 */
@MetaParticleHandlerDecleration(metaParticle = TimerTickParticle.class)
public class TimerParticleHandler implements MetaParticleHandler,
		TimerControllerInterface, Serializable {
	protected Logger logger = LoggerFactory
			.getLogger(TimerParticleHandler.class);

	private static final long serialVersionUID = 8622533504407023168L;
	private RecurringTask recurringTask = null;
	private SingleTask singleTask = null;

	/**
	 * Connect the operation to this Timer
	 */
	@Override
	public void init(Operation operation) {
		if (operation instanceof TimerTaskInterface) {
			((TimerTaskInterface) operation).setTimerController(this);
		} else {
			logger.error("Operation "
					+ operation.getClass().getName()
					+ " can not be connected to a timer. It does not implements the TimerTaskInterface");
		}

		logger.info("TimerParticleHandler initiated for operation "
				+ operation.getClass().getName());
	}

	@Override
	public List<Particle> handleMetaParticle(MetaParticle metaParticle) {
		List<Particle> result = new ArrayList<Particle>();
		if (metaParticle instanceof TimerTickParticle) {
			TimerTickParticle timerParticle = (TimerTickParticle) metaParticle;
			long timestamp = timerParticle.getTimestamp();

			// do first the single task, to give it the possibility to schedule
			// a recurring task at the current moment
			// TODO fix that all new registered tasks are all being processed
			// within the current moment
			result.addAll(executeSingleTasks(timestamp));
			result.addAll(executeRecurringTasks(timestamp));
		}
		return result;
	}

	/**
	 * Executes all recurring tasks pending before timestamp.
	 * 
	 * @param timestamp
	 * @return Returns an empty list or a list with one or more particles to be
	 *         outputed.
	 */
	protected List<Particle> executeRecurringTasks(long timestamp) {
		List<Particle> result = new ArrayList<Particle>();
		if (recurringTask != null) {
			if (recurringTask.timerFreq != 0) {
				if (recurringTask.lastTimestamp == 0) {
					recurringTask.lastTimestamp = timestamp;
				}
				while (timestamp - recurringTask.lastTimestamp >= recurringTask.timerFreq) {
					recurringTask.lastTimestamp = recurringTask.lastTimestamp
							+ recurringTask.timerFreq;
					List<DataParticle> outputParticles = recurringTask.recurringTimerTaskHandler
							.doTimerRecurringTask(recurringTask.lastTimestamp);
					if (outputParticles != null) {
						result.addAll(outputParticles);
					}
				}
			}
		}

		return result;
	}

	/**
	 * Executes all single tasks pending before timestamp.
	 * 
	 * @param timestamp
	 * @return Returns an empty list or a list with one or more particles to be
	 *         outputed.
	 */
	protected List<Particle> executeSingleTasks(long timestamp) {
		List<Particle> result = new ArrayList<Particle>();
		if (singleTask != null) {
			if (singleTask.wakeupTime != 0) {
				while ((singleTask.wakeupTime != 0)
						&& (timestamp >= singleTask.wakeupTime)) {
					// eerst het element uit de lijst voordat de task wordt
					// uitgevoerd
					// omdat die task wel eens weer voor dezelfde sensor een
					// timertask wil toevoegen
					long timerTaskTimestamp = singleTask.wakeupTime;
					singleTask.wakeupTime = 0;
					List<DataParticle> outputParticles = singleTask.singleTimerTaskHandler
							.doTimerSingleTask(timerTaskTimestamp);
					if (outputParticles != null) {
						result.addAll(outputParticles);
					}
				}
			}
		}

		return result;
	}

	@Override
	public void registerOperationForRecurringTimerTask(String channelId,
			long timerFreq, TimerTaskInterface timerTask) {
		recurringTask = new RecurringTask(timerFreq, 0, timerTask);
	}

	@Override
	public void registerOperationForSingleTimerTask(String channelId,
			long wakeupTime, TimerTaskInterface timerTask) {
		singleTask = new SingleTask(wakeupTime, timerTask);
	}

}

class RecurringTask {
	public long timerFreq;
	public long lastTimestamp;
	public TimerTaskInterface recurringTimerTaskHandler;

	public RecurringTask(long recurringTimerFreq, long lastRecurringTimestamp,
			TimerTaskInterface recurringTimerTaskInterface) {
		this.timerFreq = recurringTimerFreq;
		this.lastTimestamp = lastRecurringTimestamp;
		this.recurringTimerTaskHandler = recurringTimerTaskInterface;
	}
}

class SingleTask {
	public long wakeupTime;
	public TimerTaskInterface singleTimerTaskHandler;

	public SingleTask(long sleepTimeSingleWakeup,
			TimerTaskInterface recurringTimerTaskInterface) {
		this.wakeupTime = sleepTimeSingleWakeup;
		this.singleTimerTaskHandler = recurringTimerTaskInterface;
	}
}
