package nl.tno.stormcv.operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.storm.configuration.api.ZookeeperStormConfigurationAPI;
import nl.tno.stormcv.particle.Frame;
import nl.tno.stormcv.util.ImageUtils;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.OperationException;
import nl.tno.timeseries.interfaces.SingleOperation;

@OperationDeclaration(inputs = { Frame.class }, outputs = {Frame.class})
public class GrayscaleOperation implements SingleOperation {

	private static final long serialVersionUID = -3544960244169531707L;
	
	@Override
	@SuppressWarnings("rawtypes")
	public void init(String channelId, long sequenceNr, Map stormConf, ZookeeperStormConfigurationAPI zookeperConf) throws OperationException{ }

	@Override
	public List<DataParticle> execute(DataParticle particle) throws OperationException {
		List<DataParticle> result = new ArrayList<DataParticle>();
		Frame frame = (Frame)particle;
		try {
			frame.setImage(ImageUtils.convertToGray(frame.getImage()));
		} catch (IOException e) {
			throw new OperationException(e);
		}
		result.add(frame);
		return result;
	}
	

}
