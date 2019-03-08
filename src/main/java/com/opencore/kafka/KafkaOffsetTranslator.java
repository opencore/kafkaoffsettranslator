package com.opencore.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
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
import picocli.CommandLine;


@CommandLine.Command(description = "Migrates committed offsets for a consumer group from between clusters.",
    name = "offsettranslator", mixinStandardHelpOptions = true, version = "offsettranslator 1.0-SNAPSHOT")
public class KafkaOffsetTranslator implements Callable<Void> {

  private static final Logger logger = LoggerFactory.getLogger(KafkaOffsetTranslator.class);

  @CommandLine.Option(names = {"--source.config"}, required = true, description = "Properties file containing the configuration for the source cluster.")
  private File sourcePropertiesFile = null;

  @CommandLine.Option(names = {"--source-property"}, description = "Property for the source cluster")
  private Properties sourcePropertiesList = null;

  @CommandLine.Option(names = {"--target.config"}, required = true, description = "Properties file containing the configuration for the target cluster.")
  private File targetPropertiesFile = null;

  @CommandLine.Option(names = {"--target-property"}, description = "Individual property for the target cluster, these will override anything specificed in the target.config file.\n Can be specified multiple times.")
  private Properties targetPropertiesList = null;

  @CommandLine.Option(names = {"-c", "--consumer-group"}, required = true, description = "Name of the consumer group to translate offsets for.")
  private String consumerGroup = null;

  @CommandLine.Option(names = {"--source-topic"}, required = true, description = "Name of the consumer group to translate offsets for.")
  private String sourceTopic = null;

  @CommandLine.Option(names = {"--target-topic"}, required = true, description = "Name of the consumer group to translate offsets for.")
  private String targetTopic = null;

  @CommandLine.Option(names = {"--use-timestamps"}, description = "Name of the consumer group to translate offsets for.")
  private boolean seekByTimestamp = false;

  @CommandLine.Option(names = {"-d", "--dry-run"}, description = "Don't commit offsets to target cluster, just print them to the console.")
  private boolean dryRun = false;


  @Override
  public Void call() throws Exception {
    Properties sourceProperties = loadProperties(sourcePropertiesFile);
    Properties targetProperties = loadProperties(targetPropertiesFile);

    // Create Consumer to retrieve messages

    sourceProperties.putAll(sourcePropertiesList);
    // We'll use a Deserializer that doesn't give the message but just the hash as we'll use that to compare
    sourceProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Murmur2Deserializer.class.getCanonicalName());
    sourceProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Murmur2Deserializer.class.getCanonicalName());
    sourceProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

    // Overwrite any group that may accidentally have been placed in the config file
    targetProperties.putAll(targetPropertiesList);
    targetProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);

    try (KafkaConsumer<Integer, Integer> sourceConsumer = new KafkaConsumer<>(sourceProperties);
         KafkaConsumer<Integer, Integer> targetConsumer = new KafkaConsumer<>(targetProperties);
         AdminClient sourceAdminClient = AdminClient.create(sourceProperties);
         AdminClient targetAdminClient = AdminClient.create(targetProperties);) {

      ListConsumerGroupOffsetsResult groupOffsets = sourceAdminClient.listConsumerGroupOffsets(consumerGroup);

      // Retrieve all stored offsets for the group
      Map<TopicPartition, OffsetAndMetadata> offsetData = groupOffsets.partitionsToOffsetAndMetadata().get();

      // filter retrieved offsets down to in scope topic and subtract 1 from stored offset to get
      // last read message instead of first unread one
      Map<TopicPartition, OffsetAndMetadata> transformedOffsets = offsetData.keySet().stream()
          .filter(part -> part.topic().equals(sourceTopic))
          .collect(Collectors.toMap(part -> part, offset -> new OffsetAndMetadata(offsetData.get(offset).offset() - 1)));


      // Seek to the transformed offsets
      sourceConsumer.assign(transformedOffsets.keySet());
      for (TopicPartition part : transformedOffsets.keySet()) {
        // Avoid seeking to negative offsets if committed offset was 0
        long offset = Math.max(transformedOffsets.get(part).offset() - 1, 0);
        sourceConsumer.seek(part, offset);
      }

      Map<Integer, ConsumerRecord<Integer, Integer>> lastReadSourceRecords = new HashMap<>();

      // poll for messages, this should return all messages that confirm to the committed offsets
      // fringe cases are always possible in situations with a high partition count or very large messages
      // so we loop until we got all of them
      // should probably get a timeout for any real usage
      while (lastReadSourceRecords.size() < transformedOffsets.size()) {
        logger.debug("Polling for last read records..");
        ConsumerRecords<Integer, Integer> records = sourceConsumer.poll(Duration.ofSeconds(1));


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
      DescribeTopicsResult topicsResult = targetAdminClient.describeTopics(Arrays.asList(targetTopic));
      TopicDescription topicDescription = topicsResult.all().get().get(targetTopic);

      // Convert to set of TopicPartition that can be used for consumer assigment
      Set<TopicPartition> targetPartitions = topicDescription.partitions()
          .stream()
          .map(part -> new TopicPartition(targetTopic, part.partition()))
          .collect(Collectors.toSet());

      targetConsumer.assign(targetPartitions);

      // Either read topic from beginning or seek by known timestamps
      if (seekByTimestamp) {
        // Create input for seeking to timestamps - this will only work well if partitions have been preserved
        // during message replication
        Map<TopicPartition, Long> timestamps = lastReadSourceRecords.values()
            .stream()
            .collect(Collectors.toMap(part -> new TopicPartition(targetTopic, part.partition()), record -> record.timestamp()));

        Map<TopicPartition, OffsetAndTimestamp> offsetsFromTimestamps = targetConsumer.offsetsForTimes(timestamps);

        for (TopicPartition part : offsetsFromTimestamps.keySet()) {
          Long offset = offsetsFromTimestamps.get(part).offset();
          if (offset == 0L) {
            // Potentially not found anything, maybe log a warning
          }
          logger.debug("Seeking to equivalent timestamps in target topic.");
          logger.trace("Seeking to timestamp " + offsetsFromTimestamps.get(part).offset() + " for partition " + part.toString());
          targetConsumer.seek(part, offsetsFromTimestamps.get(part).offset());
        }

      } else {
        logger.debug("Seeking to beginning for all partitions");
        targetConsumer.seekToBeginning(targetConsumer.assignment());
      }

      // We are now either at the beginning of targetTopic or at the "correct" timestamp
      // in every partiton and can simply read all messages while comparing to what we read
      // before - any match is reported as a matched offset
      int emptyPolls = 0;

      Map<TopicPartition, OffsetAndMetadata> matchedOffsets = new HashMap<>();

      // Keep going until we either matched all record offsets or received no data for
      // the last 10 polls (probably at the end of the topic in that case)
      while (emptyPolls < 10 && matchedOffsets.size() < lastReadSourceRecords.size()) {
        ConsumerRecords<Integer, Integer> targetRecords = targetConsumer.poll(Duration.ofSeconds(1));
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
                  new OffsetAndMetadata(targetRecord.offset() + 1));
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
      targetConsumer.commitSync(matchedOffsets);
      logger.info("Successfully commited offsets for " + matchedOffsets.size() + " partitions on topic " + targetTopic + " for consumer group " + consumerGroup);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    return null;
  }

  private Properties loadProperties(File inputFile) {
    Properties result = new Properties();

    try {
      result.load(new FileInputStream(inputFile));
    } catch (IOException e) {
      logger.error("Error loading properties from file " + inputFile.getName() + ": " + e.getMessage());
      System.exit(-1);
    }
    return result;
  }

  public int getRecordId(ConsumerRecord record) {
    StringBuilder baseId = new StringBuilder();
    baseId.append(record.key()).append(record.timestamp()).append(record.value());
    return Utils.murmur2(baseId.toString().getBytes());
  }
}
