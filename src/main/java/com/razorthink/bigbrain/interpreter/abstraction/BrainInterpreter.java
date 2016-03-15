package com.razorthink.bigbrain.interpreter.abstraction;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public abstract class BrainInterpreter {
	private Properties cfg;
	private InterpreterStatus status;
	private InterpreterInfo interpreterInfo;
	private List<InterpreterResultListener> listeners;
	private boolean inited = false;

	public BrainInterpreter(Properties cfg) {
		this.cfg = cfg;
		init();
	}

	private void init() {
		listeners = new ArrayList<>();
		status = InterpreterStatus.IDLE;
		initialise();
		inited = true;
	}

	protected abstract void initialise();

	public void setCfg(Properties cfg) {
		this.cfg = cfg;
	}

	public Properties getCfg() {
		return cfg;
	}

	public boolean isConfigured() {
		return !(cfg == null);
	}

	public boolean hasCfg(String key) {
		if (cfg == null) {
			return false;
		}
		String prop = cfg.getProperty(key);
		return !(prop == null || prop.isEmpty());
	}

	public boolean isBusy() {
		return InterpreterStatus.equals(InterpreterStatus.BUSY, status);
	}

	public boolean isIdle() {
		return InterpreterStatus.equals(InterpreterStatus.IDLE, status);
	}

	public void setStatus(InterpreterStatus status) {
		this.status = status;
	}

	public InterpreterStatus getStatus() {
		return status;
	}

	public void attachListener(InterpreterResultListener listener) {
		listeners.add(listener);
	}

	public List<InterpreterResultListener> getListeners() {
		return listeners;
	}

	public InterpreterInfo getInterpreterInfo() {
		return interpreterInfo;
	}

	public void setInterpreterInfo(InterpreterInfo interpreterInfo) {
		this.interpreterInfo = interpreterInfo;
	}

	public InterpreterType getInterpreterType() {
		return interpreterInfo.getType();
	}

	public boolean isInited() {
		return inited;
	}

	public void start() {
		if (!isInited()) {
			init();
		}
		startup();
	}

	protected abstract void startup();

	public abstract void shutdown();

	public abstract InterpreterCommandResult interpret(String cmd);

	public List<InterpreterCommandResult> interpret(String[] cmds) {
		List<InterpreterCommandResult> res = new ArrayList<>();
		for (String cmd : cmds) {
			InterpreterCommandResult r = interpret(cmd);
			res.add(r);
		}
		return res;
	}
}