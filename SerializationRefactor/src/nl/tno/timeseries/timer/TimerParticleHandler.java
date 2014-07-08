package nl.tno.timeseries.timer;

import java.io.Serializable;
import java.util.List;

import nl.tno.timeseries.annotation.MetaParticleHandlerDecleration;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.EmitParticleInterface;
import nl.tno.timeseries.interfaces.MetaParticle;
import nl.tno.timeseries.interfaces.MetaParticleHandler;
import nl.tno.timeseries.interfaces.Operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@MetaParticleHandlerDecleration(metaParticle = TimerTickParticle.class )
public class TimerParticleHandler implements MetaParticleHandler, TimerControllerInterface, Serializable  {
	protected Logger logger = LoggerFactory.getLogger(TimerParticleHandler.class);

	private static final long serialVersionUID = 8622533504407023168L;
	private RecurringTask recurringTask = null;
	private SingleTask singleTask = null;
	private EmitParticleInterface emitParticleHandler;

	
	/**
	 * Connect the operation to this Timer
	 */
	@Override
	public void init(Operation operation, EmitParticleInterface emitParticleHandler) {
		this.emitParticleHandler= emitParticleHandler;
		
		if (operation instanceof TimerTaskInterface) {
			((TimerTaskInterface)operation).setTimerController(this);
		} else {
			logger.error("Operation "+operation.getClass().getName()+" can not be connected to a timer. It does not implements the TimerTaskInterface");
		}
	}

	
	@Override
	public void handleMetaParticle(MetaParticle metaParticle) {
		if (metaParticle instanceof TimerTickParticle) {
			TimerTickParticle timerParticle = (TimerTickParticle)metaParticle;
			long timestamp = timerParticle.getTimestamp();
			executeRecurringTasks(timestamp);
			executeSingleTasks(timestamp);
		} 
	}

	
	protected void executeRecurringTasks(long timestamp) {
		if (recurringTask != null) {
			if (recurringTask.timerFreq != 0) {
				if (recurringTask.lastTimestamp == 0) {
					recurringTask.lastTimestamp = timestamp;
				}
				while (timestamp - recurringTask.lastTimestamp >= recurringTask.timerFreq) {
					recurringTask.lastTimestamp = recurringTask.lastTimestamp + recurringTask.timerFreq;
					List<DataParticle> outputParticles = recurringTask.recurringTimerTaskHandler.doTimerRecurringTask(recurringTask.lastTimestamp);
					// emit all output particles
					if (outputParticles != null) {
						for (DataParticle outputParticle : outputParticles) {
							emitParticleHandler.emitParticle(outputParticle);
						}
					}
				}
			}
		}
	}
	
	
	protected void executeSingleTasks(long timestamp) {
		if (singleTask != null) {
			if (singleTask.wakeupTime != 0) {
				while ((singleTask.wakeupTime != 0) && (timestamp >= singleTask.wakeupTime)) {
					// eerst het element uit de lijst voordat de task wordt uitgevoerd 
					// omdat die task wel eens weer voor dezelfde sensor een timertask wil toevoegen
					long timerTaskTimestamp = singleTask.wakeupTime;
					singleTask.wakeupTime = 0;
					List<DataParticle> outputParticles = singleTask.singleTimerTaskHandler.doTimerSingleTask(timerTaskTimestamp);
					// emit all output particles
					if (outputParticles != null) {
						for (DataParticle outputParticle : outputParticles) {
							emitParticleHandler.emitParticle(outputParticle);
						}
					}
				}
			}
		}
	}
	
	
	@Override
	public void registerOperationForRecurringTimerTask(String channelId, long timerFreq, TimerTaskInterface timerTask) {
		recurringTask = new RecurringTask(timerFreq, 0, timerTask);
	}
	
	

	@Override
	public void registerOperationForSingleTimerTask(String channelId, long wakeupTime, TimerTaskInterface timerTask) {
		singleTask = new SingleTask(wakeupTime, timerTask);
	}


	
}


class RecurringTask {
	public long timerFreq;
	public long lastTimestamp;
	public TimerTaskInterface recurringTimerTaskHandler;
	
	public RecurringTask(long recurringTimerFreq, long lastRecurringTimestamp, TimerTaskInterface recurringTimerTaskÌnterface) {
		this.timerFreq = recurringTimerFreq;
		this.lastTimestamp = lastRecurringTimestamp;
		this.recurringTimerTaskHandler = recurringTimerTaskÌnterface;
	}
}

class SingleTask {
	public long wakeupTime;
	public TimerTaskInterface singleTimerTaskHandler;
	
	public SingleTask(long sleepTimeSingleWakeup, TimerTaskInterface singleTimerTaskÌnterface) {
		this.wakeupTime = sleepTimeSingleWakeup;
		this.singleTimerTaskHandler = singleTimerTaskÌnterface;
	}
}
