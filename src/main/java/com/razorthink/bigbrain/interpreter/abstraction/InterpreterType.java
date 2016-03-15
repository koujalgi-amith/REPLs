package com.razorthink.bigbrain.interpreter.abstraction;

import com.razorthink.bigbrain.interpreter.test.PyInterpreter;
import com.razorthink.bigbrain.interpreter.test.RInterpreter;
import com.razorthink.bigbrain.interpreter.test.ScalaInterpreter;

public enum InterpreterType {
	SCALA_SPARK("scala-spark"), R_SPARK("spark-r"), PYTHON_SPARK("py-spark");
	private String value;

	InterpreterType(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	@Override
	public String toString() {
		return getValue();
	}

	public Class<?> getInterpreterImpl() {
		switch (this) {
		case PYTHON_SPARK:
			return PyInterpreter.class;
		case R_SPARK:
			return RInterpreter.class;
		case SCALA_SPARK:
			return ScalaInterpreter.class;
		default:
			return null;
		}
	}
}
