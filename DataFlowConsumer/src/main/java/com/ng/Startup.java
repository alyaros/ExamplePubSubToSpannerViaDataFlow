package com.ng;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Startup {

    //==[References]==
    //https://github.com/apache/beam
    //https://github.com/GoogleCloudPlatform/DataflowJavaSDK
    //https://github.com/GoogleCloudPlatform/DataflowSDK-examples/tree/master/java/examples-java8/src/main/java/com/google/cloud/dataflow
    //https://cloud.google.com/dataflow/docs/quickstarts/quickstart-java-maven
    //https://github.com/apache/beam/blob/master/examples/java8/src/main/java/org/apache/beam/examples/complete/game/GameStats.java
    //https://github.com/apache/beam/blob/master/examples/java8/src/main/java/org/apache/beam/examples/complete/game/LeaderBoard.java
    //https://github.com/apache/beam/blob/master/examples/java8/src/main/java/org/apache/beam/examples/complete/game/UserScore.java
    //https://beam.apache.org/get-started/wordcount-example/
    //https://beam.apache.org/documentation/programming-guide/
    //https://beam.apache.org/documentation/runners/dataflow/
    //https://github.com/apache/beam/blob/5f972e8b2525660a2c09e6f9f21a13b5b7b46366/examples/java8/src/main/java/org/apache/beam/examples/complete/game/utils/WriteToText.java
    //https://github.com/GoogleCloudPlatform/google-cloud-java
    //Creating a Custom Sink (https://cloud.google.com/dataflow/model/custom-io)
    //CustomSink Example: https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master-1.x/sdk/src/main/java/com/google/cloud/dataflow/sdk/io/DatastoreIO.java
    //https://beam.apache.org/documentation/sdks/javadoc/2.1.0/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.html
    //http://googlecloudplatform.github.io/google-cloud-java/0.9.2/apidocs/com/google/cloud/spanner/Mutation.html
    //https://github.com/apache/beam/blob/master/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.java
    //https://beam.apache.org/documentation/programming-guide/#io
    //https://github.com/apache/beam/tree/master/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/spanner


    //==[Run]==
    //On Cloud SDK Shell ---> gcloud auth application-default login
    //On Cloud SDK Shell ---> mvn compile exec:java -Dexec.mainClass=com.ng.Startup


    public static class ParseGameEventFn extends DoFn<String, GameEvent> {
        private static final Logger LOG = LoggerFactory.getLogger(ParseGameEventFn.class);

        @ProcessElement
        public void processElement(ProcessContext c) {
            try {
                String gameEventJson = c.element();
                ObjectMapper mapper = new ObjectMapper();
                GameEvent gameEvent = mapper.readValue(gameEventJson, GameEvent.class);
                c.output(gameEvent);
            } catch (Exception ex) {
                LOG.error(ex.getMessage());
            }
        }
    }

    public static void main(String[] args) {

        String projectId = "my-project-id";
        String tmpWorkingFolder = "gs://my-bucket/tmp";
        String outputFolder = "gs://my-bucket/output/";
        String gameEventsPubSubTopic = "projects/" + projectId + "/topics/GameEvents";
        String spannerInstanceId = "nginstance";
        String spannerDatabaseId = "ngdb";

        boolean writeToTextFiles = false;
        boolean writeToSpannerTable = true;

        //Create Pipeline
        DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
        options.setRunner(DataflowRunner.class);
        options.setProject(projectId);
        options.setTempLocation(tmpWorkingFolder);
        options.setStreaming(true);
        Pipeline p = Pipeline.create(options);

        //Read From Pub\Sub Topic
        PCollection<GameEvent> rawEvents = p
                .apply(PubsubIO.readStrings().fromTopic(gameEventsPubSubTopic))
                .apply("ParseGameEvent", ParDo.of(new ParseGameEventFn()));


        //Add Timestamp
        //Todo: Can be done also by pub/sub attribute of time
        PCollection<GameEvent> stampedEvents =
                rawEvents.apply(ParDo.of(new DoFn<GameEvent, GameEvent>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        DateTime t = new DateTime(c.element().getCreationDate(), DateTimeZone.UTC);
                        c.outputWithTimestamp(c.element(), t.toInstant());
                    }
                }));


        //Window function
        PCollection<GameEvent> gameEventsWindow = stampedEvents.apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))));


        if (writeToTextFiles) {

            gameEventsWindow.apply("FormatGameEventsToString", MapElements.via(new SimpleFunction<GameEvent, String>() {
                public String apply(GameEvent gameEvent) {
                    return gameEvent.getGameId().toString() + "," + gameEvent.getCreationDate().toString();
                }
            })).apply(TextIO.write().to(outputFolder).withSuffix(".csv").withWindowedWrites().withNumShards(1));

        }

        if (writeToSpannerTable) {

            PCollection<Mutation> mutations = gameEventsWindow.apply("FormatGameEventsToMutation", MapElements.via(new SimpleFunction<GameEvent, Mutation>() {
                public Mutation apply(GameEvent input) {
                    return Mutation.newInsertBuilder("gameEvents")
                            .set("creationDate").to(Timestamp.of(input.getCreationDate()))
                            .set("gameId").to(input.getGameId().toString())
                            .build();
                }
            }));

            mutations.apply("Write", SpannerIO.write().withInstanceId(spannerInstanceId).withDatabaseId(spannerDatabaseId).withProjectId(projectId));
        }

        // Run the pipeline.
        p.run().waitUntilFinish();
    }

}
