package nl.tno.timeseries.mapping;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

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
		p.timestamp = 100;
		p.map = new HashMap<>();
		// Test fields
		List<String> expected = Arrays.asList(new String[] { "timestamp",
				"particleClass", "customNameForId", "intId", "map" });
		Fields fields = ParticleMapper.getFields(p.getClass());
		assertTrue(expected.equals(fields.toList()));

		// Test particleToValues
		Values values = ParticleMapper.particleToValues(p);
		assertEquals(100l, values.get(0));
		// idx 1 = class
		assertEquals("id", values.get(2));
		assertEquals(14, values.get(3));
		assertEquals(new HashMap<>(), values.get(4));

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
		s.timestamp = 100;
		s.map = new HashMap<>();
		// Test fields
		List<String> expected = Arrays.asList(new String[] { "timestamp",
				"customNameForId", "intId", "map" });
		Fields fields = ParticleMapper.getFields(s.getClass());
		assertTrue(expected.equals(fields.toList()));

		// Test particleToValues
		Values values = ParticleMapper.particleToValues(s);
		assertEquals(100l, values.get(0));
		assertEquals("id", values.get(1));
		assertEquals(14, values.get(2));
		assertEquals(new HashMap<>(), values.get(3));

		// Test tupleToParticle
		Tuple tuple = new MockTuple(fields, values);
		SelfMappedParticle particle = ParticleMapper.tupleToParticle(tuple,
				SelfMappedParticle.class);
		assertEquals(s, particle);
		// Test without providing class
		assertEquals(s, ParticleMapper.tupleToParticle(tuple));
	}

}
