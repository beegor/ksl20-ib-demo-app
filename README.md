#Prerequisites

1. Java 8 installed on your computer.
2. Running Kafka cluster accessible from your computer
3. Permissions to create topics on Kafka cluster

#Prepare Kafka topic
1. Manually (using kafka CLI) create a topic named "ksl20-input-topic" with about 15 partitions

#Running the demo
1. Clone this project
2. Build it with gradle wrapper: from the project root run: "./gradlew build"  This will produce an executable jar ksl20-ib-demo-backend-1.0.0.jar located in {project_root}/build/libs
3. Navigate to {project_root}/build/libs
4. Run executable ksl20-ib-demo-backend-1.0.0.jar (might need to manually assign execute permission on some systems) 

#Accessing UI
Navigate to the following URL: http://localhost:8080/ui/index.html

NOTE: Frontend is an angular app built and placed under static resources off this app, so you don't have to run it separately.
The source code can be found at https://github.com/beegor/ksl20-ib-demo-frontend


