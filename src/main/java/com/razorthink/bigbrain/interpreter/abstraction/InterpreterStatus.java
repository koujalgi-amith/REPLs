package com.razorthink.bigbrain.interpreter.abstraction;

public enum InterpreterStatus {
	IDLE("idle"), WAITING("waiting"), BUSY("busy"), ERROR("error");

	private final String value;

	InterpreterStatus(String val) {
		this.value = val;
	}

	public String getValue() {
		return value;
	}

	@Override
	public String toString() {
		return getValue();
	}

	public static boolean equals(InterpreterStatus status1, InterpreterStatus status2) {
		return status1.getValue().equals(status2.getValue());
	}
}
