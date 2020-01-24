package io.confluent.examples.clients.valve;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import javax.imageio.IIOException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;

public class ValveExample {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) {
        final Properties propsConsumer = new Properties(), propsProducer = new Properties();
        String propsFileConsumer = "", propsFileProducer = "";
        if (args.length > 0) {
            propsFileConsumer = args[0];
            propsFileProducer = args[1];
        } else {
            System.err.println("Missing consumer properties file.");
            System.exit(1);
        }
        try ( InputStream inputPropsConsumer = new FileInputStream(propsFileConsumer);
              InputStream inputPropsProducer = new FileInputStream(propsFileProducer)) {
            propsConsumer.load(inputPropsConsumer);
            propsProducer.load(inputPropsProducer);
        } catch (final IIOException | FileNotFoundException ex) {
            ex.printStackTrace();
            System.exit(1);
        } catch (final IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        // hardcode these as we do not plan to process the messages
        propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        propsProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        propsProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        // TODO - remove this after testing
        propsConsumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        propsConsumer.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (final KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(propsConsumer);
             final KafkaProducer<Object, Object> producer = new KafkaProducer<>(propsProducer)) {
            System.out.println("Consumer settings: " + propsConsumer.toString());
            System.out.println("Producer settings: " + propsProducer.toString());
            consumer.subscribe(Collections.singletonList(propsConsumer.getProperty("input.topics", "INPUT_TOPIC")));
            final String group_id = propsConsumer.getProperty("group.id");
            producer.initTransactions();
            final String valve_topic = propsProducer.getProperty("valve.topic", "VALVE_TOPIC");
            final String error_topic = propsProducer.getProperty("error.topic", "ERROR_TOPIC");
            // default processing time is 200 ms
            final Long sleep_time = Long.parseLong(propsConsumer.getProperty("processing.time.ms", "200"));
            // default backlog time is 5 minutes
            final Long backlog_time = Long.parseLong(propsConsumer.getProperty("backlog.time.ms", "300000"));
            System.out.println(backlog_time);
            final Long status_on_count = Long.parseLong(propsConsumer.getProperty("status.count", "100"));
            final Long error_on_count = Long.parseLong(propsConsumer.getProperty("error.count", "10000"));
            long record_count = 0, error_count = 0, valve_count = 0;
            while (true) {
                final ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofMillis(100));
                try {
                    if (!records.isEmpty()) {
                        final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
                        // store transactionally
                        producer.beginTransaction();
                        for (final ConsumerRecord<Object, Object> record : records) {
                            record_count++;
                            // get record create timestamp
                            final long current_time = System.currentTimeMillis();
                            final long delta = current_time - record.timestamp();
                            // check if time difference vs local time is over the configured threshold
                            if (delta > backlog_time) {
                                // too old - valve into valve topic
                                final ProducerRecord<Object, Object> recordInfo = new ProducerRecord<Object, Object>(valve_topic, record.key(), record.value());
                                producer.send(recordInfo);
                                valve_count++;
                            } else {
                                // else send to error if time is up - doing this only in non-valved
                                if (record_count % error_on_count == 0) {
                                    // send some % to fail topic
                                    final ProducerRecord<Object, Object> recordInfo = new ProducerRecord<Object, Object>(error_topic, record.key(), record.value());
                                    producer.send(recordInfo);
                                    error_count++;
                                } else // just simulate some processing
                                    Thread.sleep(sleep_time);
                            }
                            if (record_count % status_on_count == 0) {
                                // some sort of status print
                                final Date time_string = new Date();
                                System.out.println(time_string + " ********************************");
                                System.out.println("Processed: " + record_count);
                                System.out.println("Valved   : " + valve_count);
                                System.out.println("Errored  : " + error_count);
                                System.out.println(time_string + "---------------------------------");
                            }
                            currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                        }
                        // commit offsets as a part of the transaction
                        producer.sendOffsetsToTransaction(currentOffsets, group_id);
                        // final commit
                        producer.commitTransaction();
                    }
                } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                        e.printStackTrace();
                        // We can't recover from these exceptions, so our only option is to close the producer and exit.
                        producer.close();
                        break;
                }
                catch (final KafkaException e) {
                        e.printStackTrace();
                        // For all other exceptions, just abort the transaction and try again.
                        producer.abortTransaction();
                }
            }
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
    }
}
