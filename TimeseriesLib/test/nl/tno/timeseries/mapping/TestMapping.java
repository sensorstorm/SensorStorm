package nl.tno.timeseries.mapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;
import nl.tno.timeseries.mapper.ParticleMapper;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TestMapping extends TestCase {

	public void testAutoMappedParticle() {
		AutoMappedParticle p = new AutoMappedParticle();
		p.id = "id";
		p.intId = 14;
		p.sequenceNr = 100;
		p.streamId = "Stream-1";
		p.map = new HashMap<>();
		// Test fields
		List<String> expected = new ArrayList<String>() {
			{
				add("streamId");
				add("sequenceNr");
				add("particleClass");
				add("customNameForId");
				add("intId");
				add("map");
			}
		};
		Fields fields = ParticleMapper.getFields(p.getClass());
		Assert.assertTrue(expected.equals(fields.toList()));

		// Test particleToValues
		Values values = ParticleMapper.particleToValues(p);
		Assert.assertEquals("Stream-1", values.get(0));
		Assert.assertEquals(100l, values.get(1));
		// idx 2 = class
		Assert.assertEquals("id", values.get(3));
		Assert.assertEquals(14, values.get(4));
		Assert.assertEquals(new HashMap<>(), values.get(5));

		// Test tupleToParticle
		Tuple tuple = new MockTuple(fields, values);
		AutoMappedParticle particle = ParticleMapper.tupleToParticle(tuple,
				AutoMappedParticle.class);
		assertEquals(p, particle);
		// Test without providing class
		assertEquals(p, ParticleMapper.tupleToParticle(tuple));
	}

	public void testSelfMappedParticle() {
		SelfMappedParticle s = new SelfMappedParticle();
		s.id = "id";
		s.intId = 14;
		s.sequenceNr = 100;
		s.streamId = "Stream-1";
		s.map = new HashMap<>();
		// Test fields
		List<String> expected = new ArrayList<String>() {
			{
				add("streamId");
				add("sequenceNr");
				add("customNameForId");
				add("intId");
				add("map");
			}
		};
		Fields fields = ParticleMapper.getFields(s.getClass());
		Assert.assertTrue(expected.equals(fields.toList()));

		// Test particleToValues
		Values values = ParticleMapper.particleToValues(s);
		Assert.assertEquals("Stream-1", values.get(0));
		Assert.assertEquals(100l, values.get(1));
		Assert.assertEquals("id", values.get(2));
		Assert.assertEquals(14, values.get(3));
		Assert.assertEquals(new HashMap<>(), values.get(4));

		// Test tupleToParticle
		Tuple tuple = new MockTuple(fields, values);
		SelfMappedParticle particle = ParticleMapper.tupleToParticle(tuple,
				SelfMappedParticle.class);
		assertEquals(s, particle);
		// Test without providing class
		assertEquals(s, ParticleMapper.tupleToParticle(tuple));
	}

}
