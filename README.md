**Apache Beam Kafka Stream Processing Project**

**Project Description**

This project is an Apache Beam pipeline that processes a Kafka stream of JSON data, performs transformations, and outputs the results to both Kafka topics and local files. The pipeline filters incoming messages based on the computed "age" field (even or odd), derived from the provided date of birth (DOB), and applies windowing for writing the results to file sinks.

The pipeline is configured to run using the Flink Runner.

**Features**

- Reads unbounded data streams from Kafka.
- Parses JSON data into Person objects using Jackson API.
- Calculates the age of each person based on their date of birth.
- Filters messages into even and odd age groups.
- Outputs:
  Even age messages to a Kafka topic and local files.
  Odd age messages to a Kafka topic and local files.
- Uses fixed-time windowing for local file sinks.

**Prerequisites**

- Java Development Kit (JDK) 8 or higher.
- Apache Kafka installed and running.
- Apache Flink cluster for pipeline execution.
- Maven for dependency management.
- Apache Beam SDK.

**Setup and Configuration**

- Kafka Configuration
  Ensure Kafka is running locally or provide the correct bootstrapServers address in the code.
  Topics:
    Input topic: Test
    Output topics: even and odd
- Local File Sinks
  Even messages are written to: /Users/shivaniarora/bigdata/beam/filter/even
  Odd messages are written to: /Users/shivaniarora/bigdata/beam/filter/odd

**How to Run**

Step 1: Clone the Repository
- git clone <repository-url>

Step 2: Build the Project
- Use Maven to build the project:
  mvn clean install

Step 3: Start Kafka
- Ensure Kafka is running and the required topics (Test, even, odd) are created:
- bin/kafka-topics.sh --create --topic Test --bootstrap-server localhost:9092
- bin/kafka-topics.sh --create --topic even --bootstrap-server localhost:9092
- bin/kafka-topics.sh --create --topic odd --bootstrap-server localhost:9092

Step 4: Run the Pipeline
- Execute the pipeline using the FlinkRunner:
- bin/flink run -class org.data.poc.FilterStreamProcessor /Users/shivaniarora/IdeaProjects/BeamStreamProcessor/target/BeamStreamProcessor-1.0-SNAPSHOT.jar
- bin/flink run -class org.data.poc.SplitStreamProcessor /Users/shivaniarora/IdeaProjects/BeamStreamProcessor/target/BeamStreamProcessor-1.0-SNAPSHOT.jar
