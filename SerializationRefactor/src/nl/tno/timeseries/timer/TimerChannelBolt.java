package nl.tno.timeseries.timer;

import nl.tno.timeseries.interfaces.Operation;
import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.stormcomponents.ChannelBolt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimerChannelBolt extends ChannelBolt {
	private Logger logger = LoggerFactory.getLogger(TimerChannelBolt.class);
	private static final long serialVersionUID = -3958435345669417336L;
	protected TimerParticleHandler timerParticleProcessor;

	public TimerChannelBolt(Class<? extends TimedOperation> operationClass, Class<? extends Particle> outputParticleClass) {
		super(operationClass, outputParticleClass);
		timerParticleProcessor = new TimerParticleHandler();
		addMetaProcessor(TimerTickParticle.class, timerParticleProcessor); 
	}
	
	@Override
	protected void initOperation(Operation operation, Particle inputParticle) {
		super.initOperation(operation, inputParticle);
		
		if (operation instanceof TimerTaskInterface) {
			TimerTaskInterface timerTask = (TimerTaskInterface)operation;
			timerTask.setTimerController(timerParticleProcessor);
		} else {
			logger.warn("Unexpectedly InitOperation is not called on a TimedOperation, but on "+operation.getClass().getName());
		}
	}	

}
