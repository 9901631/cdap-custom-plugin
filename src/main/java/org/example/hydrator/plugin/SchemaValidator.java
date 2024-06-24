package org.example.hydrator.plugin;

import com.google.cloud.storage.*;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.*;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

import java.util.ArrayList;
import java.util.List;

import static org.example.hydrator.plugin.utils.SchemaValidationUtils.*;

@Plugin(type = Transform.PLUGIN_TYPE)
@Name("SchemaValidator") // <- The name of the plugin should match the name of the docs and widget json files.
@Description("Performs schema validation")
public class SchemaValidator extends Transform<StructuredRecord, StructuredRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaValidator.class);
    private final Config config;
    private Schema outputSchema;

    // Create list of records that will be dynamically updated
    // For valid records
    public static final ArrayList<Object> validRecordList = new ArrayList<>();

    // For invalid records
    public static final ArrayList<Object> invalidRecordList = new ArrayList<>();

    // Record error message
    public static String errorMsg = "";
    private static String jsonSchemaString = "";

    public SchemaValidator(Config config) {
        this.config = config;
    }

    /**
     * This function is called when the pipeline is published. You should use this for validating the config and setting
     * additional parameters in pipelineConfigurer.getStageConfigurer(). Those parameters will be stored and will be made
     * available to your plugin during runtime via the TransformContext. Any errors thrown here will stop the pipeline
     * from being published.
     * Used to retrieve schema from local storage.
     * @param pipelineConfigurer Configures an ETL Pipeline. Allows adding datasets and streams and storing parameters
     * @throws IllegalArgumentException If the config is invalid.
     */
    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
        super.configurePipeline(pipelineConfigurer);

        /* It's usually a good idea to validate the configuration at this point.
         * It will stop the pipeline from being published if this throws an error.
         */
        //Schema oschema;
        Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
        //config.validate(inputSchema);


        String jsonSchemaString = "";
        StorageOptions options = null;

//        BlobId blobId = BlobId.of(config.gcsBucket.toString(), "blob_name");
//        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
//        Blob blob1 = storage.create(blobInfo, fileContent.getBytes(UTF_8));

        //config.validate(fileContent);
        try {
            // to read from local file
//            String SchemaPath = "/usr/data/schema/int-schema.json";
//            File schemaFile = new File(SchemaPath);
//            jsonSchemaString = FileUtils.readFileToString(schemaFile, StandardCharsets.UTF_8);

            //to read from GCS bucket
            Storage storage = StorageOptions.getDefaultInstance().getService();
            Blob blob = storage.get(config.gcsBucket.toString(),config.schemaPath.toString());
            jsonSchemaString = new String(blob.getContent());
            LOG.info("jsonSchemaString -- :" + jsonSchemaString);

            // Removes all whitespace
            jsonSchemaString = jsonSchemaString.replaceAll("\\s", "");
            // Remove first two lines
            jsonSchemaString = jsonSchemaString.replaceAll("\\[\\{\"name\":\"etlSchemaBody\",\"schema\":", "");
            // Remove last two characters
            jsonSchemaString = jsonSchemaString.substring(0, jsonSchemaString.length() - 2);
            LOG.info("jsonschema after cleansing-- :" + jsonSchemaString);
            // Finally parses schema
            outputSchema = Schema.parseJson(jsonSchemaString);

        } catch (IOException e) {
            throw new RuntimeException("Error" + e);
        }

        config.validate(outputSchema);
        // Sets output schema
        pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
    }

    /**
     * This function is called when the pipeline has started. The values configured in here will be made available to the
     * transform function. Use this for initializing costly objects and opening connections that will be reused.
     * Used to set the output schema.
     * @param context Context for a pipeline stage, providing access to information about the stage, metrics, and plugins.
     * @throws Exception If there are any issues before starting the pipeline.
     */
    @Override
    public void initialize(TransformContext context) throws Exception {
        super.initialize(context);
        LOG.info("Inside initialize method -");
        outputSchema=context.getOutputSchema();
        LOG.info("OutputSchema -"+outputSchema);

    }

    /**
     * This is the method that is called for every record in the pipeline and allows you to make any transformations
     * you need and emit one or more records to the next stage.
     * Validates data types.
     * @param input The record that is coming into the plugin
     * @param emitter An emitter allowing you to emit one or more records to the next stage
     * @throws Exception
     */
    @Override
    public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
        // Get all the outputFields that are in the output schema
        LOG.info("inside transform - ");
        List<Schema.Field> inputFields = input.getSchema().getFields();
        List<Schema.Field> outputFields = outputSchema.getFields();
        LOG.info("input Fields - "+inputFields);
        LOG.info("output Fields - "+outputFields);
        // Create a builder for creating the output records
        StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
        // Create a builder for creating the error records
        StructuredRecord.Builder error = StructuredRecord.builder(input.getSchema());

        // Clear lists and error messages after each transformed row
        validRecordList.clear();
        invalidRecordList.clear();
        errorMsg = "";

        // Create schema list to store data types
        ArrayList<String> inputSchema = new ArrayList<>();

        int i = 0;
        for (Schema.Field fd : outputFields) {
            if (fd.getSchema().getLogicalType() == null) {
                inputSchema.add(fd.getSchema().toString().toLowerCase().replace("\"", ""));
            }
            else {
                inputSchema.add(fd.getSchema().getLogicalType().toString().toLowerCase().replace("\"", ""));
            }
            LOG.info("Logical type:" + fd.getSchema().getLogicalType());
            LOG.info("Type:" + fd.getSchema().getType());
            LOG.info(fd.getSchema().toString());
            LOG.info("Input schema:" + inputSchema.get(i));
            i++;
        }

        // Schema list iterator
        int iterator = 0;

        // Add all the values to the builder
        for (Schema.Field field : outputFields) {

            String oName = field.getName();
            String iName = inputFields.get(iterator).getName();
            if (input.get(iName) != null) {

                // Comparing outputFields for schema validation
            /*
            1. Establish a list of outputFields and data types from GCS schema bucket
            2. Use a for loop to compare each field of the raw data to schema data types
               Can use built-in Java functions for thi
            3. Records that pass the validation should be emitted
            */

                // Validates numbers
                if (inputSchema.get(iterator).matches("int|float|double|long")) {
                    LOG.info(inputSchema.get(iterator));
                    numberTryParse(input.get(iName), inputSchema.get(iterator));
                }

                // Validates strings
                else if (inputSchema.get(iterator).equals("string")) {
                    stringTryParse(input.get(iName));
                }

                // Validates booleans
                else if (inputSchema.get(iterator).equals("boolean")) {
                    booleanTryParse(input.get(iName));
                }

                // Validates byte arrays
                else if (inputSchema.get(iterator).equals("bytes")) {
                    byteTryParse(input.get(iName));
                }

                // Validates simple dates
                else if (inputSchema.get(iterator).equals("date")) {
                    simpleDateTryParse(input.get(iName));
                }

                // Validates timestamps
                else if (inputSchema.get(iterator).matches("timestamp_micros|timestamp_millis")) {
                    LOG.info("timestamp reached");
                    LOG.info("timestamp reached");
                    timestampTryParse(input.get(iName), inputSchema.get(iterator));
                }

                else if (inputSchema.get(iterator).matches("time_micros|time_millis")) {
                    timeTryParse(input.get(iName), inputSchema.get(iterator));
                }

                //LOG.info("Current record " + validRecordList.get(iterator));
                iterator++;
            }
        }

        int result = setRecords();

        LOG.info("Finished validation");

        LOG.info(String.valueOf(outputFields.size()));
        LOG.warn(String.valueOf(validRecordList.size()));

        int rt = 0;
        // No errors
        if (result == 1) {
            while (rt < outputFields.size()) {
                String record = outputFields.get(rt).getName() + "|" + validRecordList.get(rt);
                LOG.info("Success" + outputFields.get(rt).getName() + "|" + validRecordList.get(rt));
                builder.set(outputFields.get(rt).getName(), validRecordList.get(rt));
                rt++;
            }
        }
        else if (result == 2) {
            while (rt < outputFields.size()) {
                LOG.info("Invalid" + outputFields.get(rt).getName() + "|" + validRecordList.get(rt));
                //LOG.info(outputFields.get(rt).getSchema());
                error.set(inputFields.get(rt).getName(), validRecordList.get(rt).toString());
                rt++;
            }
        }

        // If you wanted to make additional changes to the output record, this might be a good place to do it.

        if (!invalidRecordList.isEmpty()) {
            InvalidEntry<StructuredRecord> invalidEntry = new InvalidEntry<>(1, errorMsg, error.build());
            emitter.emitError(invalidEntry);
        }

        else {
            // Finally, build and emit the record.
            emitter.emit(builder.build());
        }
    }

    /** Sets a custom output schema for testing framework
     * @param config config
     * @param inputSchema input schema
     * @return returns field names and record values
     */
    private static Schema getOutputSchema(Config config, Schema inputSchema) {
        List<Schema.Field> outputFields = new ArrayList<>();

        outputFields.add(Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
        outputFields.add(Schema.Field.of("age", Schema.of(Schema.Type.INT)));
        //outputFields.add(Schema.Field.of("date", Schema.of(Schema.Type.STRING)));

        return Schema.recordOf(inputSchema.getRecordName(), outputFields);
    }

    /**
     * This function will be called at the end of the pipeline. You can use it to clean up any variables or connections.
     */
    @Override
    public void destroy() {
        // No Op
    }

    /**
     * Your plugin's configuration class. The outputFields here will correspond to the outputFields in the UI for configuring the
     * plugin.
     */
    public static class Config extends PluginConfig {

        @Name("GCS Bucket")
        @Description("Specifies the GCS bucket name.")
        private final String gcsBucket;
        @Name("Schema Path")
        @Description("This specifies the schema object path")
        @Macro // <- Macro means that the value will be substituted at runtime by the user.
        private final String schemaPath;

        public Config(String schemaPath, String gcsBucket) {
            this.schemaPath = schemaPath;
            this.gcsBucket = gcsBucket;
        }

        private void validate(Schema inputSchema) throws IllegalArgumentException {
            // It's usually a good idea to check the schema. Sometimes users edit
            // the JSON config directly and make mistakes.
            try {
                LOG.info("validate input Schema  :" + inputSchema.toString());
                Schema.parseJson(inputSchema.toString());
                LOG.info("validate input Schema post parseJson :" + inputSchema);
            } catch (IOException e) {
                throw new IllegalArgumentException("Output schema cannot be parsed.", e);
            }
            // This method should be used to validate that the configuration is valid.
            if (schemaPath == null || schemaPath.isEmpty()) {
                throw new IllegalArgumentException("myOption is a required field.");
            }
            // You can use the containsMacro() function to determine if you can validate at deploy time or runtime.
            // If your plugin depends on outputFields from the input schema being present or the right type, use inputSchema
        }
    }
}