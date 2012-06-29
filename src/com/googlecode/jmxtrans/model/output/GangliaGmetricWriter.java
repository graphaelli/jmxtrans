package com.googlecode.jmxtrans.model.output;

import com.googlecode.jmxtrans.model.Query;
import com.googlecode.jmxtrans.model.Result;
import com.googlecode.jmxtrans.util.BaseOutputWriter;
import com.googlecode.jmxtrans.util.JmxUtils;
import com.googlecode.jmxtrans.util.ValidationException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool.KeyedObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;

/**
 * A writer for ganglia using gmetric.
 *
 * @author graphaelli, Monetate Inc.
 */
public class GangliaGmetricWriter extends BaseOutputWriter {

    private enum Slope {
        ZERO, POSITIVE, NEGATIVE, BOTH;

        private static Slope fromName(String slopeName) {
            for (Slope slope : values())
                if (slopeName.equalsIgnoreCase(slope.name()))
                    return slope;
            return BOTH;
        }
    }

    private enum DataType {
        INT("int32"), DOUBLE("double"), STRING("string");

        private static DataType forValue(Object value) {
            if (value instanceof Integer || value instanceof Byte || value instanceof Short)
                return INT;
            if (value instanceof Long || value instanceof Float || value instanceof Double)
                return DOUBLE;

            // Convert to double or int if possible
            try {
                Double.parseDouble(value.toString());
                return DOUBLE;
            } catch (NumberFormatException e) {
                // Not a double
            }
            try {
                Integer.parseInt(value.toString());
                return INT;
            } catch (NumberFormatException e) {
                // Not an int
            }

            return STRING;
        }

        private final String typeName;

        private DataType(String typeName) {
            this.typeName = typeName;
        }

        public String getTypeName() {
            return typeName;
        }

        public String asString(Object value) {
            return value == null ? "" : value.toString();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(GangliaGmetricWriter.class);

    private static final String DEFAULT_GMETRIC_PATH = "/usr/bin/gmetric";
    private static final String DEFAULT_GMOND_CONFIG = "/etc/ganglia/gmond.conf";
    private static final String DEFAULT_UNITS = "";
    private static final Slope DEFAULT_SLOPE = Slope.BOTH;
    private static final int DEFAULT_TMAX = 60;
    private static final int DEFAULT_DMAX = 0;

    public static final String GMETRIC_PATH = "gmetricPath";
    public static final String GMOND_CONFIG = "gmondConfig";
    public static final String GROUP_NAME = "groupName";
    public static final String SLOPE = "slope";
    public static final String UNITS = "units";
    public static final String DMAX = "dmax";
    public static final String TMAX = "tmax";

    private String gmetricPath;
    private String gmondConfig;
    private String groupName;
    private Slope slope = DEFAULT_SLOPE;
    private String units = DEFAULT_UNITS;
    private int tmax = DEFAULT_TMAX;
    private int dmax = DEFAULT_DMAX;

    /** */
    public GangliaGmetricWriter() {
    }

    /** */
    public GangliaGmetricWriter(Map<String, KeyedObjectPool> poolMap) {
    }

    /**
     * Validate gmetric options
     */
    public void validateSetup(Query query) throws ValidationException {
        gmetricPath = getStringSetting(GMETRIC_PATH, DEFAULT_GMETRIC_PATH);
        gmondConfig = getStringSetting(GMOND_CONFIG, DEFAULT_GMOND_CONFIG);
        groupName = getStringSetting(GROUP_NAME, null);
        units = getStringSetting(UNITS, DEFAULT_UNITS);
        slope = Slope.fromName(getStringSetting(SLOPE, DEFAULT_SLOPE.name()));
        tmax = getIntegerSetting(TMAX, DEFAULT_TMAX);
        dmax = getIntegerSetting(DMAX, DEFAULT_DMAX);

        log.debug("validated ganglia metric -- group: " + groupName + ", units: " + units + ", slope:" + slope +
                ", tmax: " + tmax + ", dmax: " + dmax + ", config: " + gmondConfig + ", gmetric path: " + gmetricPath);
    }

    public List<String> assembleGmetricCommand(String name, String value, String type) {
        /*
       Usage: gmetric [OPTIONS]...

         -h, --help            Print help and exit
         -V, --version         Print version and exit
         -c, --conf=STRING     The configuration file to use for finding send channels
                                  (default=`/etc/ganglia/gmond.conf')
         -n, --name=STRING     Name of the metric
         -v, --value=STRING    Value of the metric
         -t, --type=STRING     Either
                                 string|int8|uint8|int16|uint16|int32|uint32|float|double
         -u, --units=STRING    Unit of measure for the value e.g. Kilobytes, Celcius
                                 (default=`')
         -s, --slope=STRING    Either zero|positive|negative|both  (default=`both')
         -x, --tmax=INT        The maximum time in seconds between gmetric calls
                                 (default=`60')
         -d, --dmax=INT        The lifetime in seconds of this metric  (default=`0')
         -g, --group=STRING    Group of the metric
         -C, --cluster=STRING  Cluster of the metric
         -D, --desc=STRING     Description of the metric
         -T, --title=STRING    Title of the metric
         -S, --spoof=STRING    IP address and name of host/device (colon separated) we
                                 are spoofing  (default=`')
         -H, --heartbeat       spoof a heartbeat message (use with spoof option)
        */
        List<String> command = new ArrayList<String>();
        command.add(gmetricPath);
        command.add("-c");
        command.add(gmondConfig);
        command.add("-n");
        command.add(name);
        command.add("-v");
        command.add(value);
        command.add("-t");
        command.add(type);
        command.add("-u");
        command.add(units);
        command.add("-s");
        command.add(slope.name());
        command.add("-x");
        command.add(String.valueOf(tmax));
        command.add("-d");
        command.add(String.valueOf(dmax));
        command.add("-g");
        command.add(groupName);

        return command;
    }

    public void doWrite(Query query) throws Exception {
        List<String> typeNames = this.getTypeNames();

        for (Result result : query.getResults()) {
            Map<String, Object> resultValues = result.getValues();
            if (resultValues != null) {
                for (Entry<String, Object> values : resultValues.entrySet()) {
                    Object val = values.getValue();
                    if (JmxUtils.isNumeric(val)) {
                        String key = JmxUtils.getKeyString2(query, result, values, typeNames, null);
                        String type = DataType.forValue(val).getTypeName();
                        List<String> command = assembleGmetricCommand(key, val.toString(), type);
                        Process proc = new ProcessBuilder(command).start();
                        if (log.isDebugEnabled())
                            log.debug("executing: " + StringUtils.join(command, ' '));

                        int exitCode = proc.waitFor();
                        if (exitCode != 0) {
                            log.error("failed to execute " + StringUtils.join(command, ' ') + ", exited: " + exitCode);
                        }
                    }
                }
            }
        }
    }
}
