package nl.tno.timeseries.operations;

public class OperationException extends Exception {

	private static final long serialVersionUID = -8326816195074071591L;

	public OperationException() {
		super();
	}

	public OperationException(String message) {
		super(message);
	}

	public OperationException(Throwable cause) {
		super(cause);
	}

	public OperationException(String message, Throwable cause) {
		super(message, cause);
	}

	public OperationException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
