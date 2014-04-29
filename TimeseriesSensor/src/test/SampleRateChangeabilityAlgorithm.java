package test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.sensortimeseries.algorithms.BaseSensorAlgorithm;
import nl.tno.sensortimeseries.model.Measurement;
import nl.tno.sensortimeseries.model.MeasurementDouble;
import nl.tno.sensortimeseries.operation.TimerControllerInterface;


public class SampleRateChangeabilityAlgorithm extends BaseSensorAlgorithm {
	private static final long serialVersionUID = -173454527539639463L;

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

	@Override
	public void initAlgorithm(String incommingSensorid, 
			  			      long initTimestamp,  
			  			      @SuppressWarnings("rawtypes") Map conf, 
			  			      TimerControllerInterface timerController) {
		super.initAlgorithm(incommingSensorid, initTimestamp, conf, timerController);
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
		
		long Twakeup = initTimestamp + d_min;
		registerSensorForSingleTimerTask(outgoingSensorid, Twakeup, this);
	}

	
	public List<Measurement> handleMeasurement(Measurement measurement) {
//		System.out.println("SampleRateChangeabilityAlgorithm.handleMeasurement("+measurement+")");
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

	
	public List<Measurement> doTimerSingleTask(long timestamp) {
		// Note denk nog aan retransmit en hoe we dan met de tijd om moeten gaan. 
		// Opnieuw inserten van de timerticks?
		// mogelij ktimershift nofitication inserten om de timers te pending tasks daarna te laten deleten.
		// gaat dit wel goed met single shot timers? Kans op stil vallen algortime....
		long Tnow = timestamp;
		double changeability = (N_received - N_expected) / N_expected;
		N_expected = 1;
		
		long Twakeup;
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
		ArrayList<Measurement> result = new ArrayList<Measurement>();
		MeasurementDouble srcMeasurement = new MeasurementDouble(outgoingSensorid, Tnow, changeability);
		result.add(srcMeasurement);
		
		// register for next timer task
		registerSensorForSingleTimerTask(outgoingSensorid, Twakeup, this);
		
		System.out.println("SRC: "+srcMeasurement);
		return result;
	}


	@Override
	public List<Measurement> doTimerRecurringTask(long timestamp) {
		return null;
	}


}
