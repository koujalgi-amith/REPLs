package com.razorthink.bigbrain.interpreter.interpreters;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.spark.HttpServer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.repl.SparkCommandLine;
import org.apache.spark.repl.SparkILoop;
import org.apache.spark.repl.SparkIMain;
import org.apache.spark.scheduler.Pool;
import org.apache.spark.sql.SQLContext;

import com.google.common.base.Joiner;
import com.razorthink.bigbrain.interpreter.scala.Interpreter;
import com.razorthink.bigbrain.interpreter.scala.InterpreterException;
import com.razorthink.bigbrain.interpreter.scala.InterpreterProperty;
import com.razorthink.bigbrain.interpreter.scala.InterpreterPropertyBuilder;
import com.razorthink.bigbrain.interpreter.scala.InterpreterResult;
import com.razorthink.bigbrain.interpreter.scala.InterpreterUtils;
import com.razorthink.bigbrain.interpreter.scala.ResultListener;
import com.razorthink.bigbrain.interpreter.util.Configuration;

import scala.Console;
import scala.Enumeration;
import scala.None;
import scala.Some;
import scala.tools.nsc.Settings;
import scala.tools.nsc.settings.MutableSettings;

public class SparkScalaInterpreter extends Interpreter {
	private SparkILoop interpreter;
	private SparkIMain intp;
	private OutputStream out;
	private SQLContext sqlc;
	private SparkContext sc;
	@SuppressWarnings("unused")
	private SparkEnv env;
	private Map<String, Object> binder;
	private List<ResultListener> listeners = new ArrayList<>();
	private PrintStream p = null;
	private String resultStr = "";
	private boolean inited = false;

	public SparkScalaInterpreter(Properties property) {
		super(property);
	}

	public void init() {
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		out = new OutputStream() {
			@Override
			public void write(int b) throws IOException {
				for (ResultListener l : listeners) {
					buffer.write(b);
					int newline = '\n';
					buffer.flush();
					if (b == newline) {
						byte[] bytes = buffer.toByteArray();
						buffer.reset();
						String line = new String(bytes);
						l.onUpdate(line);
						resultStr = resultStr + "\n" + line;
					}
				}
			}
		};
		p = new PrintStream(out);
		inited = true;
	}

	private boolean isInited() {
		return inited;
	}

	public void addListener(ResultListener l) {
		listeners.add(l);
	}

	static {
		try {
			String appName = Configuration.getInstance().getProperty(Configuration.ConfigKey.SPARK_APPNAME);
			String sparkURI = Configuration.getInstance().getProperty(Configuration.ConfigKey.SPARK_URI);
			String sparkExecMemory = Configuration.getInstance()
					.getProperty(Configuration.ConfigKey.SPARK_EXECUTOR_MEMORY);
			String sparkCoresMax = Configuration.getInstance().getProperty(Configuration.ConfigKey.SPARK_CORES_MAX);
			String sparkResultsMax = Configuration.getInstance().getProperty(Configuration.ConfigKey.SPARK_MAX_RESULTS);
			Interpreter.register("spark", "spark", SparkScalaInterpreter.class.getName(),
					new InterpreterPropertyBuilder().add("spark.app.name", appName, "The name of spark application.")
							.add("master", getSystemDefault("MASTER", "spark.master", sparkURI),
									"Spark master uri. Ex: spark://127.0.0.1:7077")
							.add("spark.executor.memory",
									getSystemDefault(null, "spark.executor.memory", sparkExecMemory),
									"Executor memory per worker instance. ex) 512m, 32g")

							.add("spark.network.timeout", getSystemDefault(null, "spark.network.timeout", "3s"), "")
							.add("spark.cores.max", getSystemDefault(null, "spark.cores.max", sparkCoresMax),
									"Total number of cores to use. Empty value uses all available core.")
							.add("zeppelin.spark.maxResult",
									getSystemDefault("ZEPPELIN_SPARK_MAXRESULT", "zeppelin.spark.maxResult",
											sparkResultsMax),
									"Max number of SparkSQL result to display.")
							.add("args", "", "spark commandline args").build());
		} catch (Exception e) {
			System.err.println("Couldn't load props");
			e.printStackTrace();
		}

	}

	public void start() {
		if (listeners.isEmpty()) {
			logger.info("No result listeners added. Adding default console listener...");
			// add default console listener
			listeners.add(new ResultListener() {
				@Override
				public void onUpdate(String str) {
					System.out.println(str);
				}
			});
		} else {
			if (listeners.size() == 1) {
				logger.info(listeners.size() + " listener registered. Results will be logged to that listener.");
			} else
				logger.info(listeners.size() + " listeners registered. Results will be logged to those listeners.");
		}
		open();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void open() {
		if (!isInited()) {
			init();
		}
		URL[] urls = getClassloaderUrls();

		Settings settings = new Settings();
		if (getProperty("args") != null) {
			String[] argsArray = getProperty("args").split(" ");
			LinkedList<String> argList = new LinkedList<String>();
			for (String arg : argsArray) {
				argList.add(arg);
			}

			SparkCommandLine command = new SparkCommandLine(
					scala.collection.JavaConversions.asScalaBuffer(argList).toList());
			settings = command.settings();
		}

		// set classpath for scala compiler
		MutableSettings.PathSetting pathSettings = settings.classpath();
		String classpath = "";
		List<File> paths = currentClassPath();
		paths = loadUserClassPath(paths);
		for (File f : paths) {
			if (classpath.length() > 0) {
				classpath += File.pathSeparator;
			}
			classpath += f.getAbsolutePath();
		}

		if (urls != null) {
			for (URL u : urls) {
				if (classpath.length() > 0) {
					classpath += File.pathSeparator;
				}
				classpath += u.getFile();
			}
		}

		pathSettings.v_$eq(classpath);
		settings.scala$tools$nsc$settings$ScalaSettings$_setter_$classpath_$eq(pathSettings);

		// set classloader for scala compiler
		settings.explicitParentLoader_$eq(new Some<ClassLoader>(Thread.currentThread().getContextClassLoader()));
		MutableSettings.BooleanSetting b = (MutableSettings.BooleanSetting) settings.usejavacp();
		b.v_$eq(true);
		settings.scala$tools$nsc$settings$StandardScalaSettings$_setter_$usejavacp_$eq(b);

		interpreter = new SparkILoop(null, new PrintWriter(out));

		interpreter.settings_$eq(settings);

		interpreter.createInterpreter();

		intp = interpreter.intp();
		intp.setContextClassLoader();
		intp.initializeSynchronous();

		sc = getSparkContext();
		if (sc.getPoolForName("fair").isEmpty()) {
			Enumeration.Value schedulingMode = org.apache.spark.scheduler.SchedulingMode.FAIR();
			int minimumShare = 0;
			int weight = 1;
			Pool pool = new Pool("fair", schedulingMode, minimumShare, weight);
			sc.taskScheduler().rootPool().addSchedulable(pool);
		}

		sqlc = getSQLContext();

		intp.interpret("@transient var _binder = new java.util.HashMap[String, Object]()");
		binder = (Map<String, Object>) getValue("_binder");
		binder.put("sc", sc);
		binder.put("sqlc", sqlc);

		intp.interpret("@transient val sc = " + "_binder.get(\"sc\").asInstanceOf[org.apache.spark.SparkContext]");
		intp.interpret(
				"@transient val sqlc = " + "_binder.get(\"sqlc\").asInstanceOf[org.apache.spark.sql.SQLContext]");
		intp.interpret(
				"@transient val sqlContext = " + "_binder.get(\"sqlc\").asInstanceOf[org.apache.spark.sql.SQLContext]");
		intp.interpret("import org.apache.spark.SparkContext._");

		intp.interpret("import sqlContext.implicits._");
		intp.interpret("import sqlContext.sql");
		intp.interpret("import org.apache.spark.sql.functions._");

		try {
			Method loadFiles = this.interpreter.getClass().getMethod("org$apache$spark$repl$SparkILoop$$loadFiles",
					Settings.class);
			loadFiles.invoke(this.interpreter, settings);

		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw new InterpreterException(e);
		}
	}

	public void kill() {
		System.out.println("Killing Spark context...");
		getSparkContext().stop();
		while (true) {
			if (getSparkContext().isStopped()) {
				break;
			}
		}
		System.out.println("Killing interpreter...");
		intp.close();
		intp = null;
	}

	private List<File> loadUserClassPath(List<File> path) {
		try {
			Files.walk(Paths.get(Configuration.getInstance().getProperty("classpath.resources")))
					.filter(file -> file.getFileName().toString().endsWith(".jar")).collect(Collectors.toList())
					.forEach(file -> path.add(new File(file.toString())));
		} catch (Exception ioe) {
			ioe.printStackTrace();
		}
		logger.info("Current Classpath: ");
		path.forEach(classpath -> logger.info(classpath.toString()));
		return path;
	}

	@Override
	public void close() {

	}

	@SuppressWarnings("rawtypes")
	public Object getValue(String name) {
		Object ret = intp.valueOfTerm(name);
		if (ret instanceof None) {
			return null;
		} else if (ret instanceof Some) {
			return ((Some) ret).get();
		} else {
			return ret;
		}
	}

	public List<InterpreterResult> interpret(String[] lines) {
		List<InterpreterResult> results = new ArrayList<>();
		for (String line : lines) {
			InterpreterResult res = interpret(line);
			res.setCommand(line);
			results.add(res);
		}
		return results;
	}

	@Override
	public InterpreterResult interpret(String line) {
		resultStr = "";
		if (line == null || line.trim().length() == 0) {
			InterpreterResult res = new InterpreterResult(InterpreterResult.Code.SUCCESS);
			cleanResultString();
			res.setResult(resultStr);
			return res;
		}
		return interpretLines(line.split("\n"));
	}

	private InterpreterResult interpretLines(String[] lines) {
		synchronized (this) {
			InterpreterResult r = interpretInput(lines);
			cleanResultString();
			r.setResult(resultStr);
			return r;
		}
	}

	private void cleanResultString() {
		while (true) {
			if (resultStr.startsWith("\n")) {
				resultStr = resultStr.substring(1, resultStr.length());
			}
			break;
		}
		while (true) {
			if (resultStr.endsWith("\n")) {
				resultStr = resultStr.substring(0, resultStr.length() - 1);
			}
			break;
		}
	}

	private InterpreterResult interpretInput(String[] lines) {
		if (intp == null) {
			throw new InterpreterException("Interpreter has been shutdown!");
		}
		// add print("") to make sure not finishing with comment
		String[] linesToRun = new String[lines.length + 1];
		for (int i = 0; i < lines.length; i++) {
			linesToRun[i] = lines[i];
		}
		linesToRun[lines.length] = "print(\"\")";

		Console.setOut(p);

		InterpreterResult.Code r = null;
		String incomplete = "";

		for (int l = 0; l < linesToRun.length; l++) {
			String s = linesToRun[l];
			// check if next line starts with "." (but not ".." or "./") it is
			// treated as an invocation
			if (l + 1 < linesToRun.length) {
				String nextLine = linesToRun[l + 1].trim();
				if (nextLine.startsWith(".") && !nextLine.startsWith("..") && !nextLine.startsWith("./")) {
					incomplete += s + "\n";
					continue;
				}
			}
			scala.tools.nsc.interpreter.Results.Result res = null;
			try {
				res = intp.interpret(incomplete + s);
			} catch (Exception e) {
				logger.info("Interpreter exception", e);
				return new InterpreterResult(InterpreterResult.Code.ERROR, InterpreterUtils.getMostRelevantMessage(e));
			}

			r = getResultCode(res);

			if (r == InterpreterResult.Code.ERROR) {
				return new InterpreterResult(r, "");
			} else if (r == InterpreterResult.Code.INCOMPLETE) {
				incomplete += s + "\n";
			} else {
				incomplete = "";
			}
		}

		if (r == InterpreterResult.Code.INCOMPLETE) {
			return new InterpreterResult(r, "Incomplete expression");
		} else {
			return new InterpreterResult(InterpreterResult.Code.SUCCESS);
		}
	}

	public SQLContext getSQLContext() {
		if (sqlc == null) {
			sqlc = new SQLContext(getSparkContext());
		}
		return sqlc;
	}

	public synchronized SparkContext getSparkContext() {
		if (sc == null) {
			sc = createSparkContext();
			env = SparkEnv.get();
		}
		return sc;
	}

	public SparkContext createSparkContext() {
		System.err.println("------ Create new SparkContext " + getProperty("master") + " -------");

		String execUri = System.getenv("SPARK_EXECUTOR_URI");
		String[] jars = SparkILoop.getAddedJars();

		String classServerUri = null;

		try { // in case of spark 1.1x, spark 1.2x
			Method classServer = interpreter.intp().getClass().getMethod("classServer");
			HttpServer httpServer = (HttpServer) classServer.invoke(interpreter.intp());
			classServerUri = httpServer.uri();
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			// continue
		}

		if (classServerUri == null) {
			try { // for spark 1.3x
				Method classServer = interpreter.intp().getClass().getMethod("classServerUri");
				classServerUri = (String) classServer.invoke(interpreter.intp());
			} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				throw new InterpreterException(e);
			}
		}

		SparkConf conf = new SparkConf().setMaster(getProperty("master")).setAppName(getProperty("spark.app.name"))
				.set("spark.repl.class.uri", classServerUri);

		if (jars.length > 0) {
			conf.setJars(jars);
		}

		if (execUri != null) {
			conf.set("spark.executor.uri", execUri);
		}
		if (System.getenv("SPARK_HOME") != null) {
			conf.setSparkHome(System.getenv("SPARK_HOME"));
		}
		conf.set("spark.scheduler.mode", "FAIR");

		Properties intpProperty = getProperty();

		for (Object k : intpProperty.keySet()) {
			String key = (String) k;
			String val = intpProperty.get(key).toString();
			if (!key.startsWith("spark.") || !val.trim().isEmpty()) {
				logger.debug(String.format("SparkConf: key = [%s], value = [%s]", key, val));
				conf.set(key, val);
			}
		}

		// TODO(jongyoul): Move these codes into PySparkInterpreter.java
		String pysparkBasePath = getSystemDefault("SPARK_HOME", null, null);
		File pysparkPath;
		if (null == pysparkBasePath) {
			pysparkBasePath = getSystemDefault("ZEPPELIN_HOME", "zeppelin.home", "../");
			pysparkPath = new File(pysparkBasePath,
					"interpreter" + File.separator + "spark" + File.separator + "pyspark");
		} else {
			pysparkPath = new File(pysparkBasePath, "python" + File.separator + "lib");
		}

		// Only one of py4j-0.9-src.zip and py4j-0.8.2.1-src.zip should exist
		String[] pythonLibs = new String[] { "pyspark.zip", "py4j-0.9-src.zip", "py4j-0.8.2.1-src.zip" };
		ArrayList<String> pythonLibUris = new ArrayList<>();
		for (String lib : pythonLibs) {
			File libFile = new File(pysparkPath, lib);
			if (libFile.exists()) {
				pythonLibUris.add(libFile.toURI().toString());
			}
		}
		pythonLibUris.trimToSize();
		if (pythonLibs.length == pythonLibUris.size()) {
			conf.set("spark.yarn.dist.files", Joiner.on(",").join(pythonLibUris));
			if (!useSparkSubmit()) {
				conf.set("spark.files", conf.get("spark.yarn.dist.files"));
			}
			conf.set("spark.submit.pyArchives", Joiner.on(":").join(pythonLibs));
		}

		// Distributes needed libraries to workers.
		if (getProperty("master").equals("yarn-client")) {
			conf.set("spark.yarn.isPython", "true");
		}

		// conf.set("spark.dynamicAllocation.cachedExecutorIdleTimeout", "3s");
		// conf.set("spark.dynamicAllocation.executorIdleTimeout", "3s");
		// conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "3s");
		// conf.set("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout",
		// "3s");
		// conf.set("spark.network.timeout", "3s");
		// conf.set("spark.worker.timeout", "3");
		// conf.set("spark.akka.timeout", "3s");
		SparkContext sparkContext = new SparkContext(conf);
		return sparkContext;
	}

	private boolean useSparkSubmit() {
		return null != System.getenv("SPARK_SUBMIT");
	}

	@Override
	public FormType getFormType() {
		return null;
	}

	@Override
	public List<String> completion(String buf, int cursor) {
		return null;
	}

	public static String getSystemDefault(String envName, String propertyName, String defaultValue) {

		if (envName != null && !envName.isEmpty()) {
			String envValue = System.getenv().get(envName);
			if (envValue != null) {
				return envValue;
			}
		}

		if (propertyName != null && !propertyName.isEmpty()) {
			String propValue = System.getProperty(propertyName);
			if (propValue != null) {
				return propValue;
			}
		}
		return defaultValue;
	}

	private List<File> currentClassPath() {
		List<File> paths = classPath(Thread.currentThread().getContextClassLoader());
		String[] cps = System.getProperty("java.class.path").split(File.pathSeparator);
		if (cps != null) {
			for (String cp : cps) {
				paths.add(new File(cp));
			}
		}
		return paths;
	}

	private List<File> classPath(ClassLoader cl) {
		List<File> paths = new LinkedList<File>();
		if (cl == null) {
			return paths;
		}

		if (cl instanceof URLClassLoader) {
			URLClassLoader ucl = (URLClassLoader) cl;
			URL[] urls = ucl.getURLs();
			if (urls != null) {
				for (URL url : urls) {
					paths.add(new File(url.getFile()));
				}
			}
		}
		return paths;
	}

	public Properties getProperty() {
		Properties p = new Properties();
		p.putAll(property);

		Map<String, InterpreterProperty> defaultProperties = Interpreter
				.findRegisteredInterpreterByClassName(getClassName()).getProperties();
		for (String k : defaultProperties.keySet()) {
			if (!p.containsKey(k)) {
				String value = defaultProperties.get(k).getDefaultValue();
				if (value != null) {
					p.put(k, defaultProperties.get(k).getDefaultValue());
				}
			}
		}

		return p;
	}

	public String getProperty(String key) {
		if (property.containsKey(key)) {
			return property.getProperty(key);
		}

		Map<String, InterpreterProperty> defaultProperties = Interpreter
				.findRegisteredInterpreterByClassName(getClassName()).getProperties();
		if (defaultProperties.containsKey(key)) {
			return defaultProperties.get(key).getDefaultValue();
		}

		return null;
	}

	private InterpreterResult.Code getResultCode(scala.tools.nsc.interpreter.Results.Result r) {
		if (r instanceof scala.tools.nsc.interpreter.Results.Success$) {
			return InterpreterResult.Code.SUCCESS;
		} else if (r instanceof scala.tools.nsc.interpreter.Results.Incomplete$) {
			return InterpreterResult.Code.INCOMPLETE;
		} else {
			return InterpreterResult.Code.ERROR;
		}
	}
}
