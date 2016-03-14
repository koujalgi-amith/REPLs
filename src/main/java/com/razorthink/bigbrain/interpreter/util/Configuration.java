package com.razorthink.bigbrain.interpreter.util;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Configuration {
	public static class ConfigKey {
		public static final String WEBSPARK_PORT = "webspark.port";
		public static final String WEBSPARK_STATICFILES = "webspark.staticfiles";
		public static final String SPARK_URI = "spark.master.uri";
		public static final String SPARK_APPNAME = "spark.appname";
		public static final String INTERPRETER_HOST = "interpreter.host";
		public static final String INTERPRETER_APPNAME = "interpreter.appname";
		public static final String SPARK_EXECUTOR_MEMORY = "spark.executor.memory";
		public static final String SPARK_CORES_MAX = "spark.cores.max";
		public static final String SPARK_MAX_RESULTS = "spark.results.max";
		public static final String CLASSPATH_RESOURCES = "classpath.resources";
	}

	private static Configuration instance;
	private static Properties properties;
	public static String ENVIRONMENT = "";

	private Configuration() {
	}

	public static Configuration getInstance() throws Exception {
		if (!isInited()) {
			throw new Exception("Environment not set.");
		}
		if (instance == null) {
			instance = new Configuration();
			init();
		}
		return instance;
	}

	private static void init() throws Exception {
		System.out.println("Looking up configuration for environment '" + ENVIRONMENT + "'...");
		InputStream stream = Configuration.class.getClassLoader()
				.getResourceAsStream(Configuration.ENVIRONMENT + ".config.properties");
		if (stream == null) {
			throw new FileNotFoundException(
					"Configuration '" + Configuration.ENVIRONMENT + ".config.properties" + "' not found.");
		}
		properties = new Properties();
		properties.load(stream);
		System.out.println("Configuration '" + Configuration.ENVIRONMENT + ".config.properties" + "' found.");
		HashMap<Object, Object> map = new HashMap<>();
		for (@SuppressWarnings("rawtypes") Map.Entry e : properties.entrySet()) {
			map.put(e.getKey(), e.getValue());
		}
		JSONUtils.print(map);
	}

	private static boolean isInited() {
		if (Configuration.ENVIRONMENT == null || Configuration.ENVIRONMENT.trim().isEmpty()) {
			return false;
		}
		return true;
	}

	public String getProperty(String key) {
		return properties.getProperty(key);
	}

}
