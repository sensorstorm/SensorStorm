package nl.tno.stormcv.operation;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.imageio.ImageIO;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.sun.jersey.api.container.httpserver.HttpServerFactory;
import com.sun.jersey.api.core.ApplicationAdapter;
import com.sun.net.httpserver.HttpServer;

import nl.tno.storm.configuration.api.ZookeeperStormConfigurationAPI;
import nl.tno.stormcv.particle.Frame;
import nl.tno.timeseries.interfaces.BatchOperation;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.DataParticleBatch;
import nl.tno.timeseries.interfaces.OperationException;

/**
 * A {@link BatchOperation} that creates its own simple webservice used to provide MJPEG streams. The webservice 
 * supports the following:
 * <ol>
 * <li>http://IP:PORT/streaming/streams : lists the available JPG and MJPEG urls</li>
 * <li>http://IP:PORT/streaming/tiles : creates a grid with pictures from all channels being processed</li>
 * <li>http://IP:PORT/streaming/picture/{streamid}.jpg : url to grab jpg pictures </li>
 * <li>http://IP:PORT/streaming/mjpeg/{streamid}.mjpeg : provides a possibly never ending mjpeg formatted stream</li>
 * <li>http://IP:PORT/streaming/play?streamid={id of stream to watch} : provides a possibly never ending mjpeg formatted stream</li>
 * </ol> 
 * 
 * The service runs on port 8558 by default but this can be changed by using the port(int) method.
 * 
 * @author corne versloot
 *
 */
@SuppressWarnings("restriction")
@Path("/streaming")
public class MjpegStreamingOperation extends Application implements BatchOperation {

	private static final long serialVersionUID = -3389985297204010731L;
	private static Cache<String, BufferedImage> images = null;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private HttpServer server;
	private int port = 8558;
	private int frameRate = 2;
	
	private static Cache<String, BufferedImage> getImages(){
		if(images == null){
			images = CacheBuilder.newBuilder()
					.expireAfterWrite(20, TimeUnit.SECONDS) 
					.build();
		}
		return images;
	}

	public MjpegStreamingOperation port(int nr){
		this.port = nr;
		return this;
	}
	
	public MjpegStreamingOperation framerate(int nr){
		this.frameRate = nr;
		return this;
	}
	
	/**
	 * Sets the classes to be used as resources for this application
	 */
	public Set<Class<?>> getClasses() {
        Set<Class<?>> s = new HashSet<Class<?>>();
        s.add(MjpegStreamingOperation.class);
        return s;
    }

	@Override
	@SuppressWarnings("rawtypes")
	public void init(String channelId, long sequenceNr, Map stormConf, ZookeeperStormConfigurationAPI zookeperConf) throws OperationException{
		images = MjpegStreamingOperation.getImages();
		
		ApplicationAdapter adaptor = new ApplicationAdapter(new MjpegStreamingOperation());
		try {
			server = HttpServerFactory.create ("http://localhost:"+port+"/", adaptor);
		} catch (IllegalArgumentException | IOException e) {
			logger.warn("Unable to start webservice: "+e.getMessage());
			throw new OperationException("Unable to start webservice: "+e.getMessage(), e);
		}
		server.start();		
	}

	@Override
	public List<DataParticle> execute(DataParticleBatch particleBatch) throws OperationException {
		List<DataParticle> result = new ArrayList<DataParticle>();
		for(int i=0; i<particleBatch.size(); i++){
			DataParticle particle = particleBatch.get(i);
			if(!(particle instanceof Frame)) continue;
			Frame frame = (Frame)particle;
			result.add(frame);
			try{
				if(frame.getImage() == null) continue;
				images.put(frame.getChannelId(), frame.getImage());
			}catch(IOException ioe){
				throw new OperationException("Unable to add Frame to stream due to: "+ioe.getMessage(), ioe);
			}
			break;
		}
		return result;
	}

	@GET @Path("/streams")
	@Produces("text/plain")
	public String getStreamIds() throws IOException{
		String result = new String();
		for(String id : images.asMap().keySet()){
			result += "/streaming/picture/"+id+".jpeg\r\n";
		}
		System.out.println("\r\n");
		for(String id : images.asMap().keySet()){
			result += "/streaming/mjpeg/"+id+".mjpeg\r\n";
		}
		return result;
	}
	
	@GET @Path("/picture/{streamid}.jpeg")
	@Produces("image/jpg")
	public Response jpeg(@PathParam("streamid") final String streamId){
		BufferedImage image = null;
		if((image = images.getIfPresent(streamId)) != null){
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try {
				ImageIO.write(image, "jpg", baos);
				byte[] imageData = baos.toByteArray();
				return Response.ok(imageData).build(); // non streaming
				// return Response.ok(new ByteArrayInputStream(imageDAta)).build(); // streaming
			} catch (IOException ioe) {
				logger.warn("Unable to write image to output", ioe);
				return Response.serverError().build();
			}
		}else{
			return Response.noContent().build();
		}
	}
	
	@GET @Path("/playmultiple")
	@Produces("text/html")
	public String showPlayers( @DefaultValue("3") @QueryParam("cols") int cols,
			 @DefaultValue("0") @QueryParam("offset") int offset,
			 @DefaultValue("6") @QueryParam("number") int number) throws IOException{
		//number = Math.min(6, number);
		String result = "<html><head><title>Mjpeg stream players</title></head><body bgcolor=\"#3C3C3C\">";
		result += "<font style=\"color:#CCC;\">Streams: "+images.size()+" (showing "+offset+" - "+Math.min(images.size(), offset+number)+")</font><br/>";
		result += "<table style=\"border-spacing:0; border-collapse: collapse;\"><tr>";
		int videoNr = 0;
		for(String id : images.asMap().keySet()){
			if(videoNr < offset ){
				videoNr++;
				continue;
			}
			if(videoNr-offset > 0 &&(videoNr-offset) % cols == 0){
				result+="</tr><tr>";
			}
			result += "<td><video poster=\"mjpeg/"+id+".mjpeg\">"+
					"Your browser does not support the video tag.</video></td>";
			//result += "<td><img src=\"http://"+InetAddress.getLocalHost().getHostAddress()+":"+port+"/streaming/mjpeg/"+id+".mjpeg\"></td>";
			if(videoNr > offset + number) break;
			videoNr++;
		}
		result += "</tr></table></body></html>";
		return result;
	}
	
	@GET @Path("/play")
	@Produces("text/html")
	public String showPlayers( @QueryParam("streamid") String streamId) throws IOException{
		String result = "<html><head><title>Mjpeg stream: "+streamId+"</title></head><body bgcolor=\"#3C3C3C\">";
		result += "<font style=\"color:#CCC;\"><a href=\"tiles\">Back</a></font><br/>";
		result += "<table style=\"border-spacing:0; border-collapse: collapse;\"><tr>";
		result += "<video poster=\"mjpeg/"+streamId+".mjpeg\">"+
					"Your browser does not support the video tag.</video>";
		return result;
	}
	
	@GET @Path("/tiles")
	@Produces("text/html")
	public String showTiles( @DefaultValue("3") @QueryParam("cols") int cols,
			@DefaultValue("-1") @QueryParam("width") float width) throws IOException{
		String result = "<html><head><title>Mjpeg stream players</title>";
		result += "</head><body bgcolor=\"#3C3C3C\">";
		
		result += "<table style=\"border-spacing:0; border-collapse: collapse;\"><tr>";
		int videoNr = 0;
		for(String id : images.asMap().keySet()){
			if(videoNr > 0 && videoNr % cols == 0){
				result+="</tr><tr>";
			}
			result += "<td><a href=\"play?streamid="+id+"\"><img src=\"picture/"+id+".jpeg\" "+(width > 0 ? "width=\""+width+"\"" : "")+"/></a>";
			videoNr++;
		}
		result += "</tr></table></body></html>";
		return result;
	}
	
	@GET @Path("/mjpeg/{streamid}.mjpeg")
	@Produces("multipart/x-mixed-replace; boundary=--BoundaryString\r\n")
	public Response mjpeg(@PathParam("streamid") final String streamId){
		StreamingOutput output = new StreamingOutput() {
			
			private BufferedImage prevImage = null;
			private int sleep = 1000/frameRate;
			
			@Override
			public void write(OutputStream outputStream) throws IOException, WebApplicationException {
				BufferedImage image = null;
				try{
					while((image = images.getIfPresent(streamId)) != null) /*synchronized(image)*/ {
						if(prevImage == null || !image.equals(prevImage)){
							ByteArrayOutputStream baos = new ByteArrayOutputStream();
							ImageIO.write(image, "jpg", baos);
							byte[] imageData = baos.toByteArray();
							 outputStream.write((
								        "--BoundaryString\r\n" +
								        "Content-type: image/jpeg\r\n" +
								        "Content-Length: "+imageData.length+"\r\n\r\n").getBytes());
							outputStream.write(imageData);
							outputStream.write("\r\n\r\n".getBytes());
							outputStream.flush();
						}
						Utils.sleep(sleep);
						/*
						try {
							image.notifyAll();
							image.wait();
						} catch (InterruptedException e) {
							// just read the next image
						}
						*/
					}
					outputStream.flush();
					outputStream.close();
				}catch(IOException ioe){
					logger.info("Steam for ["+streamId+"] closed by client!");
				}
			}
		};
		return Response.ok(output)
				.header("Connection", "close")
				.header("Max-Age", "0")
				.header("Expires", "0")
				.header("Cache-Control", "no-cache, private")
				.header("Pragma", "no-cache")
				.build();
	}
}
