package org.example.hydrator.plugin.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import static org.example.hydrator.plugin.SchemaValidatorPlugin.*;

public class SchemaValidationUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaValidationUtils.class);


    /**
     * Parsing method for numbers
     * @param recordValue Record value
     * @param recordType Record datatype
     */
    public static void numberTryParse (String recordValue, String recordType) {

        LOG.info("Record type: " + recordType);
        switch (recordType) {
            case "int":

                try {
                    Integer intValue = Integer.parseInt(recordValue);
                    validRecordList.add(intValue);
                    LOG.info("Int: " + intValue);

                } catch (Exception e) {
                    invalidRecordList.add(recordValue);
                    validRecordList.add(recordValue);

                    errorMsg = errorMsg + recordValue + " doesn't match schema type (INT)\n";
                    LOG.info(errorMsg);

                }
                break;

            case "float":
                try {
                    Float floatValue = Float.parseFloat(recordValue);
                    validRecordList.add(floatValue);

                    LOG.info("Float: " + floatValue);
                }
                catch (Exception e) {
                    invalidRecordList.add(recordValue);
                    validRecordList.add(recordValue);

                    errorMsg = errorMsg + recordValue + "doesn't match schema type (FLOAT)\n";

                    System.out.print("Exception:" + e);
                }
                break;

            case "double":
                try {
                    Double doubleValue = Double.parseDouble(recordValue);
                    validRecordList.add(doubleValue);

                }
                catch (Exception e) {
                    invalidRecordList.add(recordValue);
                    validRecordList.add(recordValue);

                    errorMsg = errorMsg + recordValue + " doesn't match schema type (DOUBLE)\n";

                    System.out.print("Exception:" + e);
                }
                break;

            case "long":
                try {
                    Long longValue = Long.parseLong(recordValue);
                    LOG.info("Long: " + longValue);
                    validRecordList.add(longValue);

                }
                catch (Exception e) {
                    invalidRecordList.add(recordValue);
                    validRecordList.add(recordValue);

                    errorMsg = errorMsg + recordValue + " doesn't match schema type (LONG)\n";
                    LOG.info(errorMsg);

                    System.out.print("Exception:" + e);
                }
                break;
        }
    }

    /** Parsing method for strings
     * @param recordValue Record value
     */
    public static void stringTryParse (String recordValue) {
        System.out.print("recordValue String: " + recordValue);
        validRecordList.add(recordValue);
    }

    /** Parsing method for byte array
     * @param recordValue Record value
     */
    public static void byteTryParse (String recordValue) {
        byte[] byteValue = recordValue.getBytes();
        validRecordList.add(recordValue);
    }

    /** Parsing method for simple date
     * @param recordValue Record value
     */
    public static void simpleDateTryParse (String recordValue) {

        try {
            DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
            formatter.setLenient(false);
            Date date = formatter.parse(recordValue);

            ZonedDateTime zonedDateTime = ZonedDateTime.from(date.toInstant().atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)));

            // Calculate number of days since epoch
            Long daysLong = ChronoUnit.DAYS.between(Instant.EPOCH, zonedDateTime);
            Integer daysInt = daysLong.intValue();

            validRecordList.add(daysInt);
            //LOG.info(zonedDateTime);
        }
        catch (DateTimeParseException e) {
            LOG.warn("Date Parse Exception (DATETIME PARSE): " + e);
            invalidRecordList.add(recordValue);
            validRecordList.add(recordValue);

            errorMsg = errorMsg + recordValue + " doesn't match schema type (DATE)\n";

        }
        catch (ParseException e) {
            LOG.warn("Date Parse Exception (PARSE): " + e);
            invalidRecordList.add(recordValue);
            validRecordList.add(recordValue);

            errorMsg = errorMsg + recordValue + " doesn't match schema type (DATE)\n";
        }
    }

    /** Parsing method for timestamps
     * @param recordValue Record value
     * @param recordType Record type
     */
    public static void timestampTryParse (String recordValue, String recordType) {

        // Trim any whitespace
        recordValue = recordValue.trim();

        switch (recordType) {
            case "timestamp_millis":

                try {

                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss.SSS");
                    LocalDateTime localDateTime = LocalDateTime.from(formatter.parse(recordValue));

                    Timestamp timestamp = Timestamp.valueOf(localDateTime);

                    Long millisLong = ChronoUnit.MILLIS.between(Instant.EPOCH, timestamp.toInstant());

                    validRecordList.add(millisLong);
                    LOG.info("Timestamp millis: " + millisLong);

                }
                catch (DateTimeParseException e) {
                    LOG.warn("Timestamp Parse Millis Exception (DATETIME PARSE): " + e);
                    invalidRecordList.add(recordValue);
                    validRecordList.add(recordValue);

                    errorMsg = errorMsg + recordValue + " doesn't match schema type (TIMESTAMP_MILLIS)\n";
                }

                break;

            case "timestamp_micros":

                try {
                    LOG.info(recordValue);
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss.SSSSSS");
                    LocalDateTime localDateTime = LocalDateTime.from(formatter.parse(recordValue));

                    Timestamp timestamp = Timestamp.valueOf(localDateTime);

                    Long microsLong = ChronoUnit.MICROS.between(Instant.EPOCH, timestamp.toInstant());

                    //LOG.info(timestamp);
                    validRecordList.add(microsLong);
                    LOG.info("Timestamp micros: " + microsLong);
                }
                catch (DateTimeParseException e) {
                    LOG.warn("Timestamp Micros Parse Exception (DATETIME PARSE): " + e);
                    invalidRecordList.add(recordValue);
                    validRecordList.add(recordValue);

                    errorMsg = errorMsg + recordValue + " doesn't match schema type (TIMESTAMP_MICROS)\n";
                }
                break;
        }
    }

    /** Parsing method for time
     * @param recordValue Record value
     * @param recordType Record Type
     */
    public static void timeTryParse (String recordValue, String recordType) {

        switch (recordType) {
            case "time_micros":
                try {
                    LocalTime localTime = LocalTime.parse(recordValue);

                    long timeValueMicros = localTime.toNanoOfDay() / 1000;

                    LOG.info("Result" + timeValueMicros);
                    validRecordList.add(timeValueMicros);
                    LOG.info("Result2" + timeValueMicros);
                    //LOG.info(validRecordList.get(3));
                    LOG.info("Time micros: " + timeValueMicros);
                }
                catch (DateTimeParseException e) {
                    LOG.warn("Time Micros Parse Exception: " + e);
                    invalidRecordList.add(recordValue);
                    validRecordList.add(recordValue);

                    errorMsg = errorMsg + recordValue + " doesn't match schema type (TIME_MICROS)\n";
                    LOG.info(errorMsg);
                }
                break;

            case "time_millis":
                try {
                    LocalTime localTime = LocalTime.parse(recordValue);

                    long timeValueMillis = localTime.toNanoOfDay() / 1000000;

                    validRecordList.add(timeValueMillis);
                    LOG.info("Time millis: " + timeValueMillis);
                }
                catch (DateTimeParseException e) {
                    LOG.warn("Time Millis Parse Exception: " + e + "\n");
                    invalidRecordList.add(recordValue);
                    validRecordList.add(recordValue);

                    errorMsg = errorMsg + recordValue + " doesn't match schema type (TIME_MILLIS)\n";
                }
                break;
        }
    }


    /** Parsing method for booleans
     * @param recordValue Record value
     */
    public static void booleanTryParse (String recordValue) {

        recordValue = recordValue.toLowerCase();

        if (recordValue.equals("true") || recordValue.equals("false")) {
            Boolean booleanValue = Boolean.valueOf(recordValue);
            LOG.info("Boolean parsed as: " + booleanValue);
            validRecordList.add(booleanValue);
        }

        else {
            validRecordList.add(recordValue);
            invalidRecordList.add(recordValue);

            errorMsg = errorMsg + recordValue + " doesn't match schema type (BOOLEAN)\n";
        }
    }

    /** Determines whether to emit a success or error record
     */
    public static int setRecords() {

        if (invalidRecordList.isEmpty()) {
            LOG.info("empty");
            return 1;
        }

        else {
            LOG.info("If outputted, all good");
            return 2;
        }
    }

}
