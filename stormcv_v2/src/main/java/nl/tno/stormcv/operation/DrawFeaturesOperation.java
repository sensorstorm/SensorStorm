package nl.tno.stormcv.operation;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import nl.tno.storm.configuration.api.ZookeeperStormConfigurationAPI;
import nl.tno.stormcv.particle.Descriptor;
import nl.tno.stormcv.particle.Feature;
import nl.tno.stormcv.particle.Frame;
import nl.tno.stormcv.util.adaptor.AdaptorHolder;
import nl.tno.stormcv.util.adaptor.FileAdaptor;
import nl.tno.stormcv.util.adaptor.LocalFileAdaptor;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.OperationException;
import nl.tno.timeseries.interfaces.SingleOperation;

/**
 * Draws the features contained within a {@link Frame} on the image itself and is primarily used for testing purposes.
 * The bounding boxes are drawn to indicate the location of a {@link Descriptor}. If a <code>writeLocation</code> is provided the image will be written
 * to that location as JPG image with filename: streamID_sequenceNR_randomNR.png. 
 *  
 * @author Corne Versloot
 *
 */
@OperationDeclaration(inputs = { Frame.class}, outputs = {Frame.class})
public class DrawFeaturesOperation implements SingleOperation {

	private static final long serialVersionUID = 7367137846166900741L;
	
	private static Color[] colors = new Color[]{Color.RED, Color.BLUE, Color.GREEN, Color.PINK, Color.YELLOW, Color.CYAN, Color.MAGENTA};
	private String writeLocation;
	private AdaptorHolder adaptorHolder;
	private boolean drawMetadata = false;

	/**
	 * Sets a (possible remote) location where each frame must be stored. This is primarily used for testing.s
	 * @param location
	 * @return
	 */
	public DrawFeaturesOperation destination(String location){
		this.writeLocation = location;
		return this;
	}
	
	/**
	 * Indicates if key, value pairs within the metadata map present within {@link Frame} must also be drawn on the frame;
	 * @param bool
	 * @return
	 */
	public DrawFeaturesOperation drawMetadata(boolean bool){
		this.drawMetadata = bool;
		return this;
	}
	
	@Override
	@SuppressWarnings("rawtypes")
	public void init(String channelId, long sequenceNr, Map stormConf, ZookeeperStormConfigurationAPI zookeperConf) throws OperationException{
		this.adaptorHolder = new AdaptorHolder(stormConf);
	}

	@Override
	public List<DataParticle> execute(DataParticle particle) throws OperationException {
		List<DataParticle> result = new ArrayList<DataParticle>();
		if(!(particle instanceof Frame)) return result;
		Frame frame = (Frame)particle;
		result.add(frame);
		BufferedImage image = null;
		try {
			image = frame.getImage();
		} catch (IOException ioe) {
			throw new OperationException(ioe);
		}
		
		if(image == null) return result;
		
		Graphics2D graphics = image.createGraphics();
		int colorIndex = 0;
		for(Feature feature : frame.features){
			graphics.setColor(colors[colorIndex % colors.length]);
			for(Descriptor descr : feature.sparseDescriptors){
				Rectangle box = descr.boundingBox.getBounds();
				if(box.width == 0 ) box.width = 1;
				if(box.height == 0) box.height = 1;
				graphics.draw(box);
			}
			colorIndex++;
		}
		
		int y = 10;
		// draw feature legenda on top of everything else
		for(colorIndex = 0; colorIndex < frame.features.size(); colorIndex++){
			graphics.setColor(colors[colorIndex % colors.length]);
			graphics.drawString(frame.features.get(colorIndex).name, 5, y);
			y += 12;
		}
		
		if(drawMetadata) for(String key : frame.metadata.keySet()){
			colorIndex++;
			graphics.setColor(colors[colorIndex % colors.length]);
			graphics.drawString(key+" = "+frame.metadata.get(key), 5, y);
			y += 12;
		}
		try{	
			frame.setImage(image);
			if(writeLocation != null){
				String destination = writeLocation + (writeLocation.endsWith("/") ? "" : "/") + frame.channelId+"_"+frame.timestamp+"_"+Math.random()+".jpg";
				FileAdaptor fl = adaptorHolder.getAdaptor(destination);
				if(fl != null){
					fl.moveTo(destination);
					if(fl instanceof LocalFileAdaptor){
						ImageIO.write(image, "jpg", fl.getAsFile());
					}else{
						File tmpImage = File.createTempFile(""+destination.hashCode(), ".jpg");
						ImageIO.write(image, "jpg", tmpImage);
						fl.copyFile(tmpImage, true);
					}
				}
			}
		}catch(IOException ioe){
			throw new OperationException(ioe);
		}
		
		return result;
	}

}
