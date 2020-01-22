# Java Valve Consumer pattern demo


1. Build - mvn clean package
2. Adjust consumer and producer properties (security, bootstrap etc.)
Important settings :
Consumer:
    processing.time.ms - similated consumer handler latency (default 200ms)
    input.topics - input topic list (default INPUT_TOPIC)
    status.count - counter for status printout (default every 100 messages)
    error.count - counter for sending messages to error topic (every 10000th message)
Producer:
    acks - durability (default all)
    transactional.id - id for producer transactions (default valve)
    valve.topic - topic for the "valved" messages (default VALVE_TOPIC)
    error.topic - topic for the "errored" messages (default ERROR_TOPIC)
3. Run - java -jar <path to>/weirvalve-1.0-SNAPSHOT-jar-with-dependencies.jar consumer.properties producer.properties
4. Observe the console output (with the default processing time it takes 20 seconds to update)
    
 
    