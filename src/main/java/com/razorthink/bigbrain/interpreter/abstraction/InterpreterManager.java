package com.razorthink.bigbrain.interpreter.abstraction;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.razorthink.bigbrain.interpreter.util.Configuration;

public class InterpreterManager {
	private List<BrainInterpreter> interpreters = new ArrayList<>();
	private static InterpreterManager instance;

	private InterpreterManager() {
	}

	public static synchronized InterpreterManager getInstance() {
		if (instance == null) {
			instance = new InterpreterManager();
		}
		return instance;
	}

	public List<BrainInterpreter> getInterpreters() {
		return interpreters;
	}

	private boolean doesInterpreterTypeExist(InterpreterType type) {
		for (BrainInterpreter i : getInterpreters()) {
			if (i.getInterpreterType() == type) {
				return true;
			}
		}
		return false;
	}

	public BrainInterpreter getInterpreter(InterpreterType type) throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, Exception {
		if (!doesInterpreterTypeExist(type)) {
			BrainInterpreter intp = (BrainInterpreter) type.getInterpreterImpl()
					.getDeclaredConstructor(Properties.class).newInstance(Configuration.getInstance().getProperties());
			interpreters.add(intp);
			return intp;
		} else {
			for (BrainInterpreter bi : interpreters) {
				if (bi.getInterpreterType() == type) {
					return bi;
				}
			}
			return null;
		}
	}
}
