package nl.tno.stormcv.particle.serializer;

import java.awt.Rectangle;
import java.util.ArrayList;
import java.util.List;

import nl.tno.stormcv.particle.Feature;
import nl.tno.stormcv.particle.Frame;

import backtype.storm.Config;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * A Kryo @link {@link Serializer} for @link {@link Frame} objects which enables efficient serialization of 
 * nested Descriptor objects. This serializer must be registered with the Storm @link {@link Config} in order to work.
 * 
 * @author Corne Versloot
 *
 */
public class FrameSerializer extends Serializer<Frame>{

	@Override
	public void write(Kryo kryo, Output output, Frame frame) {
		output.writeString(frame.getChannelId());
		output.writeLong(frame.getTimestamp());
		
		output.writeLong(frame.timeStamp);
		output.writeString(frame.imageType);
		byte[] buffer = frame.imageBytes;
		if(buffer != null){
			output.writeInt(buffer.length);
			output.writeBytes(buffer);
		}else{
			output.writeInt(0);
		}
		output.writeFloat((float)frame.boundingBox.getX());
		output.writeFloat((float)frame.boundingBox.getY());
		output.writeFloat((float)frame.boundingBox.getWidth());
		output.writeFloat((float)frame.boundingBox.getHeight());
		
		kryo.writeObject(output, frame.features);
	}

	@Override
	public Frame read(Kryo kryo, Input input, Class<Frame> type) {
		String channelId = input.readString();
		long sequenceNr = input.readLong();
		
		long timeStamp = input.readLong();
		String imageType = input.readString();
		int buffSize = input.readInt();
		byte[] buffer = null;
		if(buffSize > 0){
			buffer = new byte[buffSize];
			input.readBytes(buffer);
		}
		Rectangle boundingBox = new Rectangle(Math.round(input.readFloat()), Math.round(input.readFloat()), 
				Math.round(input.readFloat()), Math.round(input.readFloat()));
		@SuppressWarnings("unchecked")
		List<Feature> features = kryo.readObject(input, ArrayList.class);
		
		return new Frame(channelId, sequenceNr, timeStamp, imageType, buffer, boundingBox, features);
	}

}
