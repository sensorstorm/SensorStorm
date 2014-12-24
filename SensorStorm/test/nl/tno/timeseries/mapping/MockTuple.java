package nl.tno.timeseries.mapping;

import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * NOTE: NOT FULLY IMPLEMENTED!
 */
public class MockTuple implements Tuple {

	private final List<String> fields;
	private final Values values;

	public MockTuple(Fields fields, Values values) {
		this.fields = fields.toList();
		this.values = values;
	}

	@Override
	public boolean contains(String field) {
		return fields.contains(field);
	}

	@Override
	public int fieldIndex(String field) {
		return fields.indexOf(field);
	}

	@Override
	public byte[] getBinary(int arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] getBinaryByField(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Boolean getBoolean(int i) {
		return (Boolean) values.get(i);

	}

	@Override
	public Boolean getBooleanByField(String field) {
		return (Boolean) values.get(fields.indexOf(field));

	}

	@Override
	public Byte getByte(int arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Byte getByteByField(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double getDouble(int arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double getDoubleByField(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Fields getFields() {
		return new Fields(fields);
	}

	@Override
	public Float getFloat(int arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Float getFloatByField(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer getInteger(int i) {
		return (Integer) values.get(i);

	}

	@Override
	public Integer getIntegerByField(String field) {
		return (Integer) values.get(fields.indexOf(field));

	}

	@Override
	public Long getLong(int i) {
		return (Long) values.get(i);

	}

	@Override
	public Long getLongByField(String field) {
		return (Long) values.get(fields.indexOf(field));
	}

	@Override
	public MessageId getMessageId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Short getShort(int arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Short getShortByField(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSourceComponent() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GlobalStreamId getSourceGlobalStreamid() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSourceStreamId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getSourceTask() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getString(int i) {
		return (String) values.get(i);
	}

	@Override
	public String getStringByField(String field) {
		return (String) values.get(fields.indexOf(field));
	}

	@Override
	public Object getValue(int i) {
		return values.get(i);

	}

	@Override
	public Object getValueByField(String field) {
		return values.get(fields.indexOf(field));

	}

	@Override
	public List<Object> getValues() {
		return values;
	}

	@Override
	public List<Object> select(Fields arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int size() {
		return fields.size();
	}

}
