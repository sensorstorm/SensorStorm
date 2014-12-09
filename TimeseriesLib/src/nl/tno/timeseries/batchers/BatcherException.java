package nl.tno.timeseries.batchers;

public class BatcherException extends Exception {

	private static final long serialVersionUID = 6382103260176369288L;

	public BatcherException() {
		super();
	}

	public BatcherException(String message) {
		super(message);
	}

	public BatcherException(Throwable cause) {
		super(cause);
	}

	public BatcherException(String message, Throwable cause) {
		super(message, cause);
	}

	public BatcherException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
