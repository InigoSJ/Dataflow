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
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.List;


public class PubsubToBigqueryMixed {

	interface testOptions extends Options, StreamingOptions {

		@Description("Pub/Sub topic to read from. Used if --input is empty.")
		@Required
		String getTopic();

		void setTopic(String value);

		@Description("BigQuery Dataset to write tables to. Must already+ exist.")
		@Required
		String getOutputDataset();
		void setOutputDataset(String value);

		@Description("The BigQuery table name. Should not already exist.")
		@Required
		String getOutputTableName();

		void setOutputTableName(String value);
	}

	public static void main(String[] args) throws Exception {
		testOptions options =
				PipelineOptionsFactory.fromArgs(args).withValidation().as(testOptions.class);
		// Enforce that this pipeline is always run in streaming mode.
		options.setStreaming(true);
		options.setRunner(DataflowRunner.class);
		Pipeline p = Pipeline.create(options);

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

		final TupleTag<TableRow> LowPriority= new TupleTag<TableRow>(){};
		final TupleTag<TableRow> HighPriority = new TupleTag<TableRow>(){};

		PCollectionTuple results = p.apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(options.getTopic()))
				.apply("ConvertToTableRow", ParDo.of(new DoFn<String, TableRow>() {
					@ProcessElement
					public void processElement(ProcessContext c) {
						String[] message = c.element().split(",");
						TableRow row = new TableRow();
						row.set("Name", message[0]);
						row.set("Component", message[1]);
						row.set("Package", message[2]);
						row.set("Priority", Integer.parseInt(message[3]));
						Integer priority=Integer.parseInt(message[3]);
						if (priority<2){
							c.output(LowPriority, row);
						} else {
							c.output(row);
						}
					}}).withOutputTags(HighPriority, TupleTagList.of(LongFMR)));
		results.get(LowPriority)
				.apply("WriteInBigQueryLoad", BigQueryIO.writeTableRows().to(testTable)
				.withSchema(schema)
				.withMethod(Method.FILE_LOADS)
				.withTriggeringFrequency(Duration.standardMinutes(2))
				.withNumFileShards(1)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
		results.get(HighPriority)
				.apply("WriteInBigQueryStreaming", BigQueryIO.writeTableRows().to(testTable)
				.withSchema(schema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));


		p.run().waitUntilFinish();
	}




}
