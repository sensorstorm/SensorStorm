package nl.tno.stormcv.particle.serializer;

import java.awt.Rectangle;

import nl.tno.stormcv.particle.Descriptor;

import backtype.storm.Config;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * A Kryo @link {@link Serializer} for @link {@link Descriptor} objects which enables efficient serialization of 
 * nested Descriptor objects. This serializer must be registered with the Storm @link {@link Config} in order to work.
 * 
 * @author Corne Versloot
 *
 */
public class DescriptorSerializer extends Serializer<Descriptor>{

	@Override
	public void write(Kryo kryo, Output output, Descriptor descriptor) {
		output.writeString(descriptor.getChannelId());
		output.writeLong(descriptor.getTimestamp());
		
		output.writeFloat((float)descriptor.boundingBox.getX());
		output.writeFloat((float)descriptor.boundingBox.getY());
		output.writeFloat((float)descriptor.boundingBox.getWidth());
		output.writeFloat((float)descriptor.boundingBox.getHeight());
		
		output.writeLong(descriptor.duration);
		
		output.writeInt(descriptor.values.length);
		for(Float f : descriptor.values){
			output.writeFloat(f);
		}
	}

	@Override
	public Descriptor read(Kryo kryo, Input input, Class<Descriptor> type) {
		String channelId = input.readString();
		long sequenceNr = input.readLong();
		
		Rectangle rectangle = new Rectangle(Math.round(input.readFloat()), Math.round(input.readFloat()), 
				Math.round(input.readFloat()), Math.round(input.readFloat()));
		long duration = input.readLong();
		int length = input.readInt();
		float[] values = new float[length];
		for(int i=0; i<length; i++){
			values[i] = input.readFloat();
		}
		return new Descriptor(channelId, sequenceNr, rectangle, duration, values);
	}

}
