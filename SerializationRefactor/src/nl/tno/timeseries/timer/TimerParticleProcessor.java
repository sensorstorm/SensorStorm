package nl.tno.timeseries.timer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import nl.tno.timeseries.interfaces.MetaParticle;
import nl.tno.timeseries.interfaces.MetaParticleProcessor;
import nl.tno.timeseries.interfaces.Particle;

public class TimerParticleProcessor implements MetaParticleProcessor, TimerControllerÌnterface, Serializable  {

	private static final long serialVersionUID = 8622533504407023168L;
	private RecurringTask recurringTask = null;
	private SingleTask singleTask = null;

	@Override
	public List<Particle> execute(MetaParticle metaParticle) {
		ArrayList<Particle> result = new ArrayList<Particle>();

		if (metaParticle instanceof TimerTickParticle) {
			TimerTickParticle timerParticle = (TimerTickParticle)metaParticle;
			long timestamp = timerParticle.getTimestamp();
			System.out.println("process timertick for channel "+timerParticle.getChannelId()+" at "+timestamp);
			executeRecurringTasks(timestamp, result);
			
			executeSingleTasks(timestamp, result);
			
			return result;
		} else {
			return null;
		}
	}

	
	protected void executeRecurringTasks(long timestamp, List<Particle> result) {
		if (recurringTask != null) {
			if (recurringTask.timerFreq != 0) {
				if (recurringTask.lastTimestamp == 0) {
					recurringTask.lastTimestamp = timestamp;
				}
				while (timestamp - recurringTask.lastTimestamp >= recurringTask.timerFreq) {
					recurringTask.lastTimestamp = recurringTask.lastTimestamp + recurringTask.timerFreq;
					List<Particle> recurringParticles = recurringTask.recurringTimerTaskHandler.doTimerRecurringTask(recurringTask.lastTimestamp);
					if (recurringParticles != null) {
						result.addAll(recurringParticles);
					}
				}
			}
		}
	}
	
	
	protected void executeSingleTasks(long timestamp, List<Particle> result) {
		if (singleTask != null) {
			if (singleTask.wakeupTime != 0) {
				while ((singleTask.wakeupTime != 0) && (timestamp >= singleTask.wakeupTime)) {
					// eerst het element uit de lijst voordat de task wordt uitgevoerd 
					// omdat die task wel eens weer voor dezelfde sensor een timertask wil toevoegen
					long timerTaskTimestamp = singleTask.wakeupTime;
					singleTask.wakeupTime = 0;
					List<Particle> singleParticles = singleTask.singleTimerTaskHandler.doTimerSingleTask(timerTaskTimestamp);
					if (singleParticles != null) {
						result.addAll(singleParticles);
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
