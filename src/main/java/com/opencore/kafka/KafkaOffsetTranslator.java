package com.opencore.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
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
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaOffsetTranslator {

  private static final Logger logger = LoggerFactory.getLogger(KafkaOffsetTranslator.class);

  public static void main(String[] args) {
    if (args.length < 3) {
      logger.error("At least source and target topic need to be specified!");
    }

    final String sourceTopic = args[0];
    final String targetTopic = args[1];
    final String consumerGroup = args.length >= 3 ? args[2] : null;
    final boolean seekByTimestamp = args.length == 4 ? Boolean.parseBoolean(args[3]) : false;

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

      Map<Integer, ConsumerRecord<Integer, Integer>> lastReadSourceRecords = new HashMap<>();

      // poll for messages, this should return all messages that confirm to the committed offsets
      // fringe cases are always possible in situations with a high partition count or very large messages
      // so we loop until we got all of them
      // should probably get a timeout for any real usage
      while (lastReadSourceRecords.size() < transformedOffsets.size()) {
        logger.debug("Polling for last read records..");
        ConsumerRecords<Integer, Integer> records = consumer.poll(Duration.ofSeconds(1));


        // Iterate over all in scope partitions and look for matching messages
        for (TopicPartition part : transformedOffsets.keySet()) {
          Optional<ConsumerRecord<Integer, Integer>> recordFound = records.records(part).stream()
              .filter(record -> record.offset() == transformedOffsets.get(part).offset())
              .findFirst();

          if (!recordFound.isPresent()) {
            // No record found for this partition
            // insert
            // handling here
          } else {
            lastReadSourceRecords.put(getRecordId(recordFound.get()), recordFound.get());
          }
        }
      }

      // We now have the last read records stored in lastReadSourceRecords

      // Describe targetTopic
      DescribeTopicsResult topicsResult = adminClient.describeTopics(Arrays.asList(targetTopic));
      TopicDescription topicDescription = topicsResult.all().get().get(targetTopic);

      // Convert to set of TopicPartition that can be used for consumer assigment
      Set<TopicPartition> targetPartitions = topicDescription.partitions()
          .stream()
          .map(part -> new TopicPartition(targetTopic, part.partition()))
          .collect(Collectors.toSet());

      consumer.assign(targetPartitions);

      // Either read topic from beginning or seek by known timestamps
      if (seekByTimestamp) {
        // Create input for seeking to timestamps - this will only work well if partitions have been preserved
        // during message replication
        Map<TopicPartition, Long> timestamps = lastReadSourceRecords.values()
            .stream()
            .collect(Collectors.toMap(part -> new TopicPartition(targetTopic, part.partition()), record -> record.timestamp()));

        Map<TopicPartition, OffsetAndTimestamp> offsetsFromTimestamps = consumer.offsetsForTimes(timestamps);

        for (TopicPartition part : offsetsFromTimestamps.keySet()) {
          Long offset = offsetsFromTimestamps.get(part).offset();
          if (offset == 0L) {
            // Potentially not found anything, maybe log a warning
          }
          logger.debug("Seeking to equivalent timestamps in target topic.");
          logger.trace("Seeking to timestamp " + offsetsFromTimestamps.get(part).offset() + " for partition " + part.toString());
          consumer.seek(part, offsetsFromTimestamps.get(part).offset());
        }

      } else {
        logger.debug("Seeking to beginning for all partitions");
        consumer.seekToBeginning(consumer.assignment());
      }

      // We are now either at the beginning of targetTopic or at the "correct" timestamp
      // in every partiton and can simply read all messages while comparing to what we read
      // before - any match is reported as a matched offset
      int emptyPolls = 0;

      Map<TopicPartition, OffsetAndMetadata> matchedOffsets = new HashMap<>();

      // Keep going until we either matched all record offsets or received no data for
      // the last 10 polls (probably at the end of the topic in that case)
      while (emptyPolls < 10 && matchedOffsets.size() < lastReadSourceRecords.size()) {
        ConsumerRecords<Integer, Integer> targetRecords = consumer.poll(Duration.ofSeconds(1));
        if (targetRecords.isEmpty()) {
          emptyPolls++;
          continue;
        }
        emptyPolls = 0;
        Iterator<ConsumerRecord<Integer, Integer>> iter = targetRecords.iterator();
        while (iter.hasNext()) {
          ConsumerRecord<Integer, Integer> targetRecord = iter.next();

          if (lastReadSourceRecords.containsKey(getRecordId(targetRecord))) {
            // we've committed the offset for a record with the same value (hash)
            // -> potential match, compare key as well to be sure
            ConsumerRecord<Integer, Integer> sourceRecord = lastReadSourceRecords.get(getRecordId(targetRecord));
            if (targetRecord.key() == sourceRecord.key() && targetRecord.timestamp() == sourceRecord.timestamp()) {
              // match based on same
              //   - key
              //   - value
              //   - timestamp
              logger.trace("Found match for source record " +
                  sourceRecord.topic() + "/" + sourceRecord.partition() + "/" + sourceRecord.offset() + ": " +
                  targetRecord.topic() + "/" + targetRecord.partition() + "/" + targetRecord.offset() + ": ");

              matchedOffsets.put(
                  new TopicPartition(targetRecord.topic(), targetRecord.partition()),
                  new OffsetAndMetadata(targetRecord.offset()));
            }
          }
        }
      }
      // Some additional checking should be performed to ensure that everything was found and we
      // have offsets for all partitions - this may not have worked out if replication wasn't done
      // with correct partitioning or the targetTopic has different partition count
      // Log warnings in that case and perhaps deny running at all if partition count doesn't match

      if (matchedOffsets.size() != lastReadSourceRecords.size()) {
        logger.warn("Couldn't find matching messages for " + (lastReadSourceRecords.size() - matchedOffsets.size()) + " partitions, will commit offsets, but please double check results!");
      }
      consumer.commitSync(matchedOffsets);
      logger.info("Successfully commited offsets for " + matchedOffsets.size() + " partitions on topic " + targetTopic + " for consumer group " + consumerGroup);
      consumer.close();

    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  public static int getRecordId(ConsumerRecord record) {
    StringBuilder baseId = new StringBuilder();
    baseId.append(record.key()).append(record.timestamp()).append(record.value());
    return Utils.murmur2(baseId.toString().getBytes());
  }

}
