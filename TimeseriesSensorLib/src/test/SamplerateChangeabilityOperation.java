package test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.SingleOperation;
import nl.tno.timeseries.sensor.measurements.Measurement;
import nl.tno.timeseries.timer.TimerControllerInterface;
import nl.tno.timeseries.timer.TimerParticleHandler;
import nl.tno.timeseries.timer.TimerTaskInterface;

@OperationDeclaration(inputs = { Measurement.class }, outputs = {}, metaParticleHandlers = { TimerParticleHandler.class })
public class SamplerateChangeabilityOperation implements SingleOperation, TimerTaskInterface {
	private static final long serialVersionUID = 773649574489299505L;
	TimerControllerInterface timerController = null;
	private String incommingSensorid;

	// config parameters
	private long d_max;
	private long d_min;
	private double alpha;
	private String outgoingSensorid;

	// internal parameters
	long T_forlast_arrival;
	long T_last_arrival;
	double N_received;
	double N_expected;
	long Twakeup;
	
	@Override
	public void init(String channelID, long startTimestamp, @SuppressWarnings("rawtypes") Map stormConfig) {
		this.incommingSensorid = channelID;
		
		
		outgoingSensorid = incommingSensorid + ".src";
		
		// config parameters
		d_max = conf.get(SRCConfig.SAMPLERATE_CHANGEABILITY_DMAX) == null ? 1000L : (Long)conf.get(SRCConfig.SAMPLERATE_CHANGEABILITY_DMAX);
		d_min = conf.get(SRCConfig.SAMPLERATE_CHANGEABILITY_DMIN) == null ? 100L : (Long)conf.get(SRCConfig.SAMPLERATE_CHANGEABILITY_DMIN);
		alpha = conf.get(SRCConfig.SAMPLERATE_CHANGEABILITY_ALPHA) == null ? 1.1 : (Double)conf.get(SRCConfig.SAMPLERATE_CHANGEABILITY_ALPHA);

		// system parameters
		T_forlast_arrival = -2;	// must be before T_last_arrival
		T_last_arrival = -1; // must be before initTimestamp
		N_expected = 1;
		N_received = 0;
		
		Twakeup = startTimestamp + d_min;
	}

	

	@Override
	public void setTimerController(TimerControllerInterface timerController) {
		this.timerController = timerController;
		timerController.registerOperationForSingleTimerTask(outgoingSensorid, Twakeup, this);
	}

	
	
	@Override
	public List<DataParticle> execute(DataParticle inputParticle) {
		if (inputParticle != null) {
			if (inputParticle instanceof Measurement<?>) {
				return handleMeasurement((Measurement<?>)inputParticle);
			} else {
				System.out.println("Operation channel " + incommingSensorid + " Data particle received " + inputParticle);
			}
		}
		return null;
	}
	

	public List<DataParticle> handleMeasurement(Measurement<?> measurement) {
		// ignore retransmitted measurements.
		if (T_last_arrival > measurement.getTimestamp()) {
			return null;
		} else {
			T_forlast_arrival = T_last_arrival;
			T_last_arrival = measurement.getTimestamp();
			N_received++;
			return null;
		}
	}

	
	
	@Override
	public List<DataParticle> doTimerSingleTask(long timestamp) {
		// Note denk nog aan retransmit en hoe we dan met de tijd om moeten gaan. 
		// Opnieuw inserten van de timerticks?
		// mogelij ktimershift nofitication inserten om de timers te pending tasks daarna te laten deleten.
		// gaat dit wel goed met single shot timers? Kans op stil vallen algortime....
		long Tnow = timestamp;
		double changeability = (N_received - N_expected) / N_expected;
		N_expected = 1;
		
		if (N_received == 0) {
			double delta_calc = Tnow - T_last_arrival;
			double delta = Math.min(d_max, alpha * delta_calc);
			delta = Math.max(d_min, delta);
			Twakeup = Tnow + Math.round(delta);
		} else {
			double delta_calc = T_last_arrival - T_forlast_arrival;
			double delta = (long) Math.min(d_max, alpha * delta_calc);
			if (delta < d_min) {
				delta = d_min;
				N_expected = delta / delta_calc;
			}
			Twakeup = T_last_arrival + Math.round(delta);
			if (Twakeup <= Tnow) {
				Twakeup = Tnow + Math.round(delta);
			}
		}
		// always wait at least 1 ms
		if (Twakeup == Tnow) {
			Twakeup = Tnow + 1;
		}
		N_received = 0;
		
		
		// return changeability measurement
		ArrayList<DataParticle> result = new ArrayList<DataParticle>();
		Measurement<Double> srcMeasurement = new Measurement<Double>(outgoingSensorid, Tnow, changeability);
		result.add(srcMeasurement);
		
		// register for next timer task
		timerController.registerOperationForSingleTimerTask(outgoingSensorid, Twakeup, this);
		return result;
	}

	
	
	@Override
	public List<DataParticle> doTimerRecurringTask(long timestamp) {
		return null;
	}

}
