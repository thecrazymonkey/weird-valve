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

3. Run - java -jar <path to>/weird-valve-1.0-SNAPSHOT-jar-with-dependencies.jar consumer.properties producer.properties
4. Observe the console output (with the default processing time it takes 20 seconds to update)

To test this you can do something like:

    kafka-producer-perf-test --topic INPUT_TOPIC --producer.config your_client.properties --throughput -1 --print-metrics --num-records 10000 --record-size 100    
 
And watch the log - it should process (slowly) ok until about 1500 or so messages and then start "valving" quickly the rest.
Once it finished the initial batch you can re-run the command to generate more messages. Again it will start doing normal 
processing until the messages in the topic become too old and "valving" ensues.

