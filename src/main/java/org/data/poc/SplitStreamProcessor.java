package org.data.poc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.data.encoder.PersonCoder;
import org.data.pojo.Person;
import org.joda.time.Duration;

import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;

public class SplitStreamProcessor {

    public static void main(String[] args) {

        ObjectMapper objectMapper = new ObjectMapper();
        // Define Kafka configurations
        String bootstrapServers = "localhost:9092";
        String inputTopic = "Test";
        String evenTopic = "even";
        String oddTopic = "odd";

        // Define pipeline options
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(FlinkRunner.class);

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        PCollection<KV<String, String>> kafkaStream = pipeline
                .apply("ReadFromKafka", KafkaIO.<String, String>read()
                        .withBootstrapServers(bootstrapServers)
                        .withTopic(inputTopic)
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withoutMetadata());

        PCollection<Person> processedStream = kafkaStream.apply("ProcessMessages", ParDo.of(new DoFn<KV<String, String>, Person>() {
            @ProcessElement
            public void processElement(@Element KV<String, String> message, OutputReceiver<Person> out) {
                try {
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

        final TupleTag<Person> evenTag = new TupleTag<Person>(){};
        final TupleTag<Person> oddTag = new TupleTag<Person>(){};

        // Apply ParDo with side outputs
        PCollectionTuple splitStream = processedStream.apply("SplitStreamByAge", ParDo
                .of(new DoFn<Person, Person>() {
                    @ProcessElement
                    public void processElement(@Element Person person, MultiOutputReceiver out) {
                        if (person.getAge() % 2 == 0) {
                            out.get(evenTag).output(person);  // Output to even stream (evenTag)
                        } else {
                            out.get(oddTag).output(person);   // Output to odd stream (oddTag)
                        }
                    }
                })
                .withOutputTags(evenTag, TupleTagList.of(oddTag)));  // Define evenTag as the primary output and oddTag as the side output

        // Get the even and odd streams from TupleTags
        PCollection<Person> evenStream = splitStream.get(evenTag);  // Fetch the even stream
        PCollection<Person> oddStream = splitStream.get(oddTag);    // Fetch the odd stream


        // Process evenStream and oddStream as needed
        evenStream.apply("ConvertPersonToJsonForEven",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                                .via((Person person) -> {
                                    try {
                                        String json = objectMapper.writeValueAsString(person);
                                        return KV.of(person.getUid(), json); // Create the KV pair
                                    } catch (Exception e) {
                                        throw new RuntimeException("Error serializing Person object to JSON: " + person, e);
                                    }
                                }))
                .apply("WriteToKafkaForEven", KafkaIO.<String, String>write()
                        .withBootstrapServers(bootstrapServers)
                        .withTopic(evenTopic)
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class));

        oddStream.apply("ConvertPersonToJsonForOdd",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                                .via((Person person) -> {
                                    try {
                                        String json = objectMapper.writeValueAsString(person);
                                        return KV.of(person.getUid(), json); // Create the KV pair
                                    } catch (Exception e) {
                                        throw new RuntimeException("Error serializing Person object to JSON: " + person, e);
                                    }
                                }))
                .apply("WriteToKafkaForOdd", KafkaIO.<String, String>write()
                        .withBootstrapServers(bootstrapServers)
                        .withTopic(oddTopic)
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class));

        // Write the evenStream and oddStream to separate files
        evenStream
                .apply("ConvertPersonToJsonForEven",
                        MapElements.into(TypeDescriptors.strings())
                                .via((Person person) -> {
                                    try {
                                        return objectMapper.writeValueAsString(person);
                                    } catch (Exception e) {
                                        throw new RuntimeException("Error serializing Person object to JSON: " + person, e);
                                    }
                                }))
                .apply("ApplyWindowing", Window.into(FixedWindows.of(Duration.standardMinutes(1)))) // Use 1-minute fixed windows
                .apply("WriteToTextFile", TextIO.write()
                        .to("/Users/shivaniarora/bigdata/beam/split/even")  // Specify the output directory
                        .withWindowedWrites() // Write windowed data
                        .withSuffix(".txt") // Add file extension for better readability
                        .withoutSharding()  // Optional: If you don't need sharding
                );

        oddStream
                .apply("ConvertPersonToJsonForEven",
                        MapElements.into(TypeDescriptors.strings())
                                .via((Person person) -> {
                                    try {
                                        return objectMapper.writeValueAsString(person);
                                    } catch (Exception e) {
                                        throw new RuntimeException("Error serializing Person object to JSON: " + person, e);
                                    }
                                }))
                .apply("ApplyWindowing", Window.into(FixedWindows.of(Duration.standardMinutes(1)))) // Use 1-minute fixed windows
                .apply("WriteToTextFile", TextIO.write()
                        .to("/Users/shivaniarora/bigdata/beam/split/odd")  // Specify the output directory
                        .withWindowedWrites() // Write windowed data
                        .withSuffix(".txt") // Add file extension for better readability
                        .withoutSharding()  // Optional: If you don't need sharding
                );

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }
}
