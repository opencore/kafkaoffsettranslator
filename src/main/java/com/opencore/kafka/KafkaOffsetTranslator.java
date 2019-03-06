package com.opencore.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;


public class KafkaOffsetTranslator {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("At least source and target topic need to be specified!");
        }

        final String sourceTopic = args[0];
        final String targetTopic = args[1];
        final String consumerGroup = args.length == 3 ? args[2] : null;

        Properties adminProps = new Properties();
        adminProps.setProperty("bootstrap.servers", "127.0.0.1:9092");
        AdminClient adminClient = AdminClient.create(adminProps);

        ListConsumerGroupOffsetsResult groupOffsets = adminClient.listConsumerGroupOffsets(consumerGroup);

        try {
            // Retrieve all stored offsets for the group
            Map<TopicPartition, OffsetAndMetadata> offsetData = groupOffsets.partitionsToOffsetAndMetadata().get();

            // filter retrieved offsets down to in scope topic and subtract 1 from stored offset to get
            // last read message instead of first unread one
            Map<TopicPartition, OffsetAndMetadata> transformedOffsets = offsetData.keySet().stream()
                .filter(part -> part.topic().equals(sourceTopic))
                .collect(Collectors.toMap(part -> part, offset -> new OffsetAndMetadata(offsetData.get(offset).offset() - 1)));

            // Create Consumer to retrieve messages
            // We'll use a Deserializer that doesn't give the message but just the hash as we'll use that to compare
            adminProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "com.opencore.kafka.Murmur2Deserializer");
            adminProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.opencore.kafka.Murmur2Deserializer");
            KafkaConsumer<Integer, Integer> consumer = new KafkaConsumer<>(adminProps);

            // Seek to the transformed offsets
            consumer.assign(transformedOffsets.keySet());
            for (TopicPartition part : transformedOffsets.keySet()) {
                // Avoid seeking to negative offsets if committed offset was 0
                long offset = Math.max(transformedOffsets.get(part).offset() - 1, 0);
                consumer.seek(part, offset);
            }

            // poll for messages, this should return all messages that confirm to the committed offsets
            // fringe cases are always possible in situations with a high partition count or very large messages
            // this should be enclosed in a loop for any actual usage
            ConsumerRecords<Integer, Integer> records = consumer.poll(Duration.ofSeconds(1));

            Map<Integer, ConsumerRecord<Integer, Integer>> lastReadSourceRecords = new HashMap<>();

            // Iterate over all in scope partitions and look for matching messages
            for (TopicPartition part : transformedOffsets.keySet()) {
                List<ConsumerRecord<Integer, Integer>> partitionRecords = records.records(part);
                long offset = offsetData.get(part).offset() - 1;

                Set<ConsumerRecord<Integer, Integer>> matchedRecords = partitionRecords.stream()
                    .filter(record -> record.offset() == transformedOffsets.get(part).offset())
                    .collect(Collectors.toSet());

                if (matchedRecords.size() == 1) {
                    ConsumerRecord<Integer, Integer> match = matchedRecords.iterator().next();
                    lastReadSourceRecords.put(match.value(), match);
                }
            }
            // Close consumer to clean up
            consumer.close();

            // We now have the last read records stored in lastReadSourceRecords

            DescribeTopicsResult topicsResult = adminClient.describeTopics(Arrays.asList(targetTopic));
            TopicDescription topicDescription = topicsResult.all().get().get(targetTopic);

            adminProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "irrelevant");
            consumer = new KafkaConsumer<Integer, Integer>(adminProps);
            consumer.subscribe(Arrays.asList(targetTopic));
            consumer.poll(Duration.ofSeconds(1));
            consumer.seekToBeginning(consumer.assignment());

            // We are now at the beginning of the targettopic and can simply read all
            // messages while comparing to what we read before - any match is reported as a matched offset
            int emptyPolls = 0;

            while (emptyPolls < 10) {
                ConsumerRecords<Integer, Integer> targetRecords = consumer.poll(Duration.ofSeconds(1));
                if (targetRecords.isEmpty()) {
                    emptyPolls++;
                    continue;
                }
                emptyPolls = 0;
                Iterator<ConsumerRecord<Integer, Integer>> iter = targetRecords.iterator();
                while (iter.hasNext()) {
                    ConsumerRecord<Integer, Integer> targetRecord = iter.next();

                    if (lastReadSourceRecords.containsKey(targetRecord.value())) {
                        // we've committed the offset for a record with the same value (hash)
                        // -> potential match, compare key as well to be sure
                        ConsumerRecord<Integer, Integer> sourceRecord = lastReadSourceRecords.get(targetRecord.value());
                        if (targetRecord.key() == sourceRecord.key()) {
                            // match
                            System.out.println("Found match for source record " +
                                sourceRecord.topic() + "/" + sourceRecord.partition() + "/" + sourceRecord.offset() + ": " +
                                targetRecord.topic() + "/" + targetRecord.partition() + "/" + targetRecord.offset() + ": ");

                        }
                    }
                }
            }
            // Some additional checking should be performed to ensure that everything was found and we
            // have offsets for all partitions - this may not have worked out if replication wasn't done
            // with correct partitioning or the targetTopic has different partition count
            // Log warnings in that case and perhaps deny running at all if partition count doesn't match

            // Stick code in here to commit offsets to targetTopic for consumerGroup


        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


    }
}
