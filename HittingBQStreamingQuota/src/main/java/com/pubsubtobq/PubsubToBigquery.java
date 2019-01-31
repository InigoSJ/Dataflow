package com.pubsubtobq;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.pubsubtobq.utils.Options;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import java.util.ArrayList;
import java.util.List;


public class PubsubToBigquery {

	interface testOptions extends Options, StreamingOptions {

		@Description("Pub/Sub topic to read from. Used if --input is empty.")
		@Validation.Required
		String getTopic();

		void setTopic(String value);

		@Description("BigQuery Dataset to write tables to. Must already+ exist.")
		@Validation.Required
		String getOutputDataset();
		void setOutputDataset(String value);

		@Description("The BigQuery table name. Should not already exist.")
		@Validation.Required
		String getOutputTableName();

		void setOutputTableName(String value);
	}

	public static void main(String[] args) throws Exception {
		testOptions options =
				PipelineOptionsFactory.fromArgs(args).withValidation().as(testOptions.class);
		// Enforce that this pipeline is always run in streaming mode.
		options.setStreaming(true);
		options.setRunner(DataflowRunner.class);
		Pipeline pipeline = Pipeline.create(options);

		TableReference testTable = new TableReference();
		testTable.setDatasetId(options.getOutputDataset());
		testTable.setProjectId(options.as(GcpOptions.class).getProject());
		testTable.setTableId(options.getOutputTableName());


		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("name").setType("STRING"));
		fields.add(new TableFieldSchema().setName("surname").setType("STRING"));
		fields.add(new TableFieldSchema().setName("ID").setType("STRING"));
		fields.add(new TableFieldSchema().setName("urgency").setType("INTEGER"));
		TableSchema schema = new TableSchema().setFields(fields);

		//Duration triggeringFrequency = Duration.standardSeconds(10);

		pipeline.apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(options.getTopic()))
				.apply("ConvertToTableRow", ParDo.of(new DoFn<String, TableRow>() {
					@ProcessElement
					public void processElement(ProcessContext c) {
						String[] message = c.element().split(",");
						TableRow row = new TableRow();
						row.set("Name", message[0]);
						row.set("Component", message[1]);
						row.set("Package", message[2]);
						row.set("Priority", Integer.parseInt(message[3]));
						c.output(row);
					}}))
				.apply("WriteInBigQuery", BigQueryIO.writeTableRows().to(testTable)
						.withSchema(schema)
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));


		pipeline.run().waitUntilFinish();
	}




}
