package nl.tno.timeseries.groupers;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import nl.tno.timeseries.interfaces.MetaParticle;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;

public class TimeseriesChannelIdGrouping implements CustomStreamGrouping,
		Serializable {
	private static final long serialVersionUID = 9192870640924179453L;
	private int numTasks;
	private List<Integer> targetTasks;

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		Fields componentOutputFields = context.getComponentOutputFields(stream);
		componentOutputFields.fieldIndex("");

		this.targetTasks = targetTasks;
		numTasks = targetTasks.size();
		System.out.println("TimeseriesShuffleGrouping.prepare targetTasks="
				+ targetTasks);
	}

	// vermoedelijk is taskId het storm takstId, net zoals een bolt er eentje
	// krijgen. \
	// Deze geeft dus de instantie van deze grouper aan.
	// NIET het gekozen boltId.
	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		List<Integer> boltIds = new ArrayList<Integer>();
		// Is this tuple a Particle?
		try {
			if (values.size() > 3) {
				String channelId = (String) values.get(0);
				Long timestamp = (Long) values.get(1);
				String particleClassName = (String) values.get(2);
				System.out.println("  TSG incomming: taskId = " + taskId
						+ " channelId = " + channelId + " timestamp="
						+ timestamp + " particle class=" + particleClassName);
				Class<?> particleClass = Class.forName(particleClassName);
				// is this tuple a metaParticle?
				if (MetaParticle.class.isAssignableFrom(particleClass)) {
					// broadcast the MetaParticle tuple
					for (Integer boltId : targetTasks) {
						System.out
								.println("    TSG: metaparticle broadcast to bolt "
										+ boltId);
						boltIds.add(boltId);
					}
				} else {
					// DataParticle perform normal operation.
					// int targetTaskId = taskId % numTasks;
					// object.hashCode can be negative! first abs before %.
					int targetTaskId = determineTargetTaskId(channelId);
					System.out.println("    TSG: DATAparticle send to bolt "
							+ targetTaskId);
					boltIds.add(targetTaskId);
				}
			}
		} catch (Exception e) {
			// error -> no or unknown particle structure, perform normal shuffle
			int targetTaskId = 0;
			if (values.size() > 0) {
				targetTaskId = determineTargetTaskId(values.get(0));
			}
			System.out.println("    TSG: Unknown particle (exception="
					+ e.getMessage() + ") send to bolt " + targetTaskId);

			boltIds.add(taskId % numTasks);
		} finally {
			return boltIds;
		}
	}

	private int determineTargetTaskId(Object obj) {
		return targetTasks.get(Math.abs(obj.hashCode()) % numTasks);
	}
}
