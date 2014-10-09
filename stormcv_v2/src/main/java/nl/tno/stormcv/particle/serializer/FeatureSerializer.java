package nl.tno.stormcv.particle.serializer;

import java.util.ArrayList;
import java.util.List;

import nl.tno.stormcv.particle.Descriptor;
import nl.tno.stormcv.particle.Feature;

import backtype.storm.Config;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * A Kryo @link {@link Serializer} for @link {@link Feature} objects which enables efficient serialization of 
 * nested Descriptor objects. This serializer must be registered with the Storm @link {@link Config} in order to work.
 * 
 * @author Corne Versloot
 *
 */

public class FeatureSerializer extends Serializer<Feature>{

	@Override
	public void write(Kryo kryo, Output output, Feature feature) {
		output.writeString(feature.getChannelId());
		output.writeLong(feature.getTimestamp());
		
		output.writeString(feature.name);
		output.writeLong(feature.duration);
		kryo.writeObject(output, feature.sparseDescriptors);
		float[][][] m = feature.denseDescriptors;
		output.writeInt(m.length); // write x
		if(m.length == 0) return;
		
		output.writeInt(m[0].length); // write y
		output.writeInt(m[0][0].length); // write z
		for(int x=0; x<m.length; x++){
			for(int y=0; y<m[0].length; y++){
				for(int z=0; z<m[0][0].length; z++){
					output.writeFloat(m[x][y][z]);
				}
			}
		}
		
	}

	@Override
	public Feature read(Kryo kryo, Input input, Class<Feature> type) {
		String channelId = input.readString();
		long sequenceNr = input.readLong();
		
		String name = input.readString();
		long duration = input.readLong();
		
		@SuppressWarnings("unchecked")
		List<Descriptor> sparseDescriptors = kryo.readObject(input, ArrayList.class);
		
			int xl = input.readInt();
			float[][][] denseDescriptor = null;
			if(xl > 0){
				int yl = input.readInt();
				int zl = input.readInt();
				denseDescriptor = new float[xl][yl][zl];
				for(int x=0; x<xl; x++){
					for(int y=0; y<yl; y++){
						for(int z=0; z<zl; z++){
							denseDescriptor[x][y][z] = input.readFloat();
						}
					}
				}
			}
		
		return new Feature(channelId, sequenceNr, name, duration, sparseDescriptors, denseDescriptor);
	}

}
