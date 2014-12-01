package nl.tno.anysense.timelines.holder;

public class Blob {

	public long blobID;
	public byte[] value;

	
	public Blob(long blobID, byte[] value) {
		this.blobID = blobID;
		this.value = value;
	}

	public Blob(int blobID, byte[] value) {
		this.blobID = (long)blobID;
		this.value = value;
	}

}
