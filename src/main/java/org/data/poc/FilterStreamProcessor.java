package org.data.poc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.data.encoder.PersonCoder;
import org.data.pojo.Person;
import org.joda.time.Duration;

import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;

public class FilterStreamProcessor {

    public static void main(String[] args) {

        // Define Kafka configurations
        String bootstrapServers = "localhost:9092";
        String inputTopic = "Test";
        String evenTopic = "even";
        String oddTopic = "odd";
        String evenFilePath = "/Users/shivaniarora/bigdata/beam/filter/even";
        String oddFilePath = "/Users/shivaniarora/bigdata/beam/filter/odd";

        // Define pipeline options
        PipelineOptions options = PipelineOptionsFactory.create();

        // Set the runner to FlinkRunner
        options.setRunner(FlinkRunner.class);

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        //Declaring Kafka Source to read unbounded stream of data
        PCollection<KV<String, String>> kafkaStream = pipeline
                .apply("KafkaSource", KafkaIO.<String, String>read()
                        .withBootstrapServers(bootstrapServers)
                        .withTopic(inputTopic)
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withoutMetadata());

        // Converting Json payload getting from Kafka Stream to Person Object through Jackson API
        PCollection<Person> processedStream = kafkaStream.apply("ProcessMessages", ParDo.of(new DoFn<KV<String, String>, Person>() {
            @ProcessElement
            public void processElement(@Element KV<String, String> message, OutputReceiver<Person> out) {
                try {
                    ObjectMapper objectMapper = new ObjectMapper();
                    String messageValue = message.getValue();
                    Person person = objectMapper.readValue(messageValue, Person.class);

                    // Calculate age
                    LocalDate dob = LocalDate.parse(person.getDob(), DateTimeFormatter.ofPattern("dd-MM-yyyy"));
                    int age = Period.between(dob, LocalDate.now()).getYears();
                    person.setAge(age);

                    // Output Person object
                    out.output(person);
                } catch (Exception e) {
                    System.err.println("Error processing message: " + message.getValue());
                    e.printStackTrace();
                }
            }
        })).setCoder(new PersonCoder());

        // Filtering Even Events from the Main Stream of data
        PCollection<KV<String, String>> evenStream = processedStream.apply("FilterEvenMessages",
                Filter.by(person -> (Boolean) (person.getAge() % 2 == 0)))
                .apply("ConvertPersonToJsonForEven",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                                .via((Person person) -> {
                                    try {
                                        ObjectMapper objectMapper = new ObjectMapper();
                                        String json = objectMapper.writeValueAsString(person);
                                        return KV.of(person.getUid(), json); // Create the KV pair
                                    } catch (Exception e) {
                                        throw new RuntimeException("Error serializing Person object to JSON: " + person, e);
                                    }
                                })
                );

        // Filtering Odd Events from the Main Stream of data
        PCollection<KV<String, String>> oddStream = processedStream.apply("FilterOddMessages",
                        Filter.by(person -> (Boolean) (person.getAge() % 2 != 0)))
                .apply("ConvertPersonToJsonForEven",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                                .via((Person person) -> {
                                    try {
                                        ObjectMapper objectMapper = new ObjectMapper();
                                        String json = objectMapper.writeValueAsString(person);
                                        return KV.of(person.getUid(), json); // Create the KV pair
                                    } catch (Exception e) {
                                        throw new RuntimeException("Error serializing Person object to JSON: " + person, e);
                                    }
                                })
                );
        // declaring Kafka Sink for Even Events
        evenStream.apply("kafkaEvenSink", KafkaIO.<String, String>write()
                .withBootstrapServers(bootstrapServers)
                .withTopic(evenTopic)
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class));

        // declaring Kafka Sink for Odd Events
        oddStream.apply("kafkaOddSink", KafkaIO.<String, String>write()
                .withBootstrapServers(bootstrapServers)
                .withTopic(oddTopic)
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class));

        //Writing File Sink for Even Events
        evenStream.apply("CastingEvenValues", MapElements.into(TypeDescriptors.strings())
                        .via(kv -> kv.getValue()))
                .apply("ApplyWindowing", Window.into(FixedWindows.of(Duration.standardMinutes(1)))) // Use 1-minute fixed windows
                .apply("WriteToTextFile", TextIO.write()
                        .to(evenFilePath)  // Specify the output directory
                        .withWindowedWrites() // Write windowed data
                        .withSuffix(".txt") // Add file extension for better readability
                        .withoutSharding()  // Optional: If you don't need sharding
                );

        //Writing File Sink for Odd Events
        oddStream.apply("CastingOddValues", MapElements.into(TypeDescriptors.strings())
                        .via(kv -> kv.getValue()))
                .apply("ApplyWindowing", Window.into(FixedWindows.of(Duration.standardMinutes(1)))) // Use 1-minute fixed windows
                .apply("WriteToTextFile", TextIO.write()
                        .to(oddFilePath)  // Specify the output directory
                        .withWindowedWrites() // Write windowed data
                        .withSuffix(".txt") // Add file extension for better readability
                        .withoutSharding()  // Optional: If you don't need sharding
                );

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }
}