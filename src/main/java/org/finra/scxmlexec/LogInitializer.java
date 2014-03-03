package org.finra.scxmlexec;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.finra.datagenerator.SystemProperties;

public class LogInitializer {

    private static final AtomicBoolean init = new AtomicBoolean(false);

    public static void initialize(String loggerLevel) {
        if (!init.compareAndSet(false, true)) {
            return;
        }

        ConsoleAppender consoleAppender = new ConsoleAppender(
                new PatternLayout("<%d{yyMMdd HHmmss} %5p %C{1}:%L> %m%n"), ConsoleAppender.SYSTEM_ERR);
        BasicConfigurator.configure(consoleAppender);

        Level level;

        String logLevel;

        if (loggerLevel != null) {
            logLevel = loggerLevel.toLowerCase();
        } else {
            logLevel = SystemProperties.logLevel.toLowerCase();
        }

        if (logLevel.equals("all")) {
            level = Level.ALL;
        } else if (logLevel.equals("debug")) {
            level = Level.DEBUG;
        } else if (logLevel.equals("error")) {
            level = Level.ERROR;
        } else if (logLevel.equals("fatal")) {
            level = Level.FATAL;
        } else if (logLevel.equals("info")) {
            level = Level.INFO;
        } else if (logLevel.equals("off")) {
            level = Level.OFF;
        } else if (logLevel.equals("trace")) {
            level = Level.TRACE;
        } else if (logLevel.equals("warn")) {
            level = Level.WARN;
        } else {
            level = Level.WARN;
        }

        Logger.getLogger("org.finra").setLevel(level);

        System.err.println("Set loglevel to " + level.toString());
    }
}
