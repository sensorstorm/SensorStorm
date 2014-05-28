package nl.tno.timeseries.mapping;

import java.util.Map;

import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.mapper.annotation.TupleField;

public class AutoMappedParticle implements Particle {

	public String streamId;
	public long sequenceNr;

	@TupleField(name = "customNameForId")
	public String id;

	@TupleField
	public int intId;

	@TupleField
	public Map<String, Double> map;

	public int shouldNotBeSaved;

	@Override
	public String getChannelId() {
		return streamId;
	}

	@Override
	public long getSequenceNr() {
		return sequenceNr;
	}

	@Override
	public void setChannelId(String streamId) {
		this.streamId = streamId;
	}

	@Override
	public void setSequenceNr(long sequenceNr) {
		this.sequenceNr = sequenceNr;
	}

	@Override
	public int hashCode() {
		// does not take sholdNotBeSaved into consideration
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + intId;
		result = prime * result + ((map == null) ? 0 : map.hashCode());
		result = prime * result + (int) (sequenceNr ^ (sequenceNr >>> 32));
		result = prime * result + ((streamId == null) ? 0 : streamId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		// does not take sholdNotBeSaved into consideration
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AutoMappedParticle other = (AutoMappedParticle) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (intId != other.intId)
			return false;
		if (map == null) {
			if (other.map != null)
				return false;
		} else if (!map.equals(other.map))
			return false;
		if (sequenceNr != other.sequenceNr)
			return false;
		if (streamId == null) {
			if (other.streamId != null)
				return false;
		} else if (!streamId.equals(other.streamId))
			return false;
		return true;
	}

}
