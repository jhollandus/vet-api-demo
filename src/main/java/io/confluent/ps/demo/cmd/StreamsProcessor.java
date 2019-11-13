package io.confluent.ps.demo.cmd;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.ps.KnownInstallation;
import io.confluent.ps.KnownInstallationKey;
import io.confluent.ps.SyncInstallationCmd;
import io.confluent.ps.SyncInstallationCompletion;
import io.confluent.ps.demo.cmd.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StreamsProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(StreamsProcessor.class);
    private static final List<String> ENTITIES =
            Arrays.asList("Clinic", "Prescription", "Patient", "Appointment", "Doctor");
    private static final Random RAND = new Random();

    private KafkaStreams streams;
    private final VetDemoApplication.Config config;

    @Autowired
    public StreamsProcessor(VetDemoApplication.Config config) {
        this.config = config;
    }


    @PostConstruct
    public void onStart() throws Exception {
        var topology = buildTopology();

        Properties streamProps = config.streamsConfig();
        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        streams = new KafkaStreams(buildTopology(), streamProps);
        LOG.info("Starting stream. | configuration={}", config);
        streams.start();
    }

    @PreDestroy
    public void onStop() throws Exception {
        if (streams != null) {
            streams.close();
        }
    }

    protected Topology buildTopology() {

        StreamsBuilder builder = new StreamsBuilder();
        KStream<KnownInstallationKey, KnownInstallation> vetDataStream = builder.stream(config.getVetDataUpdateTopic());


        KTable<KnownInstallationKey, KnownInstallation> knownInstallsTable = builder.table(config.getKnownInstallationTopic());


        vetDataStream = vetDataStream.leftJoin(knownInstallsTable, (vetData, knownInstall) -> {
            if (knownInstall == null) {
                vetData.setDiscoveredTime(System.currentTimeMillis());
                return vetData;
            }
            return knownInstall;
        })

                //Check for updates
                .mapValues(install -> {
                    long availableRange = fetchAvailableInstallRange(install.getInstallId(), install.getProcessedRange());
                    install.setAvailableRange(availableRange);
                    install.setLastCheckTime(System.currentTimeMillis());
                    LOG.info("Updating known installation: {}", install);
                    return install;
                });
        //update the table with the current available version
        vetDataStream.to(config.getKnownInstallationTopic());

        //filter out installations that don't have updates available then publish update commands for those that do
        var cmdStream = vetDataStream.filter((k, v) -> v.getProcessedRange() == null || v.getProcessedRange() < v.getAvailableRange())
                .flatMapValues(install -> {
                    String correlationKey = UUID.randomUUID().toString();
                    return ENTITIES.stream()
                            .map(ent -> SyncInstallationCmd.newBuilder()
                                    .setCorrelationId(correlationKey)
                                    .setEntityType(ent)
                                    .setKnownInstallation(install)
                                    .setRequestTime(System.currentTimeMillis())
                                    .build())
                            .collect(Collectors.toList());
                }).through(config.getInstallationCmdTopic());

        var cmdResultStream = cmdStream
                //filter out completed commands
                .filter((k, v) -> !v.getEntityCompletions().containsKey(v.getEntityType()))
                .mapValues(cmd -> {
           var results = fetchUpdates(cmd);
           cmd.getEntityCompletions().put(cmd.getEntityType(), SyncInstallationCompletion.newBuilder()
            .setCompletionTime(System.currentTimeMillis())
            .setRecordsProcessed(results.size())
            .setSuccessful(true)
            .build());
           return new CmdResults(cmd, results);
        });

        //publish data to entity topic
        cmdResultStream.flatMap((k, cmdResults) ->
               cmdResults.results.stream().map(v ->
                    KeyValue.pair(
                        //generate random key for entity, demo purpose only, also use string instead of avro because it's a demo
                        cmdResults.cmd.getKnownInstallation().getInstallId() + "-"
                        + cmdResults.cmd.getEntityType() + "-"
                        + v.split(":")[1], v))
                .collect(Collectors.toList()))
        .to(config.getInstallationEntityUpdateTopic(), Produced.with(Serdes.String(), Serdes.String()));


        //update command, use Exactly once semantics to make this transactional, requires 3 brokers though.
        cmdResultStream.mapValues(cmdRslt -> cmdRslt.cmd)
                .to(config.getInstallationCmdTopic());

        //aggregate the command results together and wait for them to complete based on correlation ID
        cmdStream.groupByKey()
                .reduce((prev, next) -> {
                    LOG.info("Received command: {}", next);
                    if (next.getCorrelationId() == null || next.getEntityCompletions().isEmpty()) {
                        //garbage, no correlationId or completions
                        return prev;
                    }

                    if (prev == null) {
                        //first time seeing install
                        next.setEntityType(null);
                        return next;
                    }

                    //check for errors and so on here if so desired
                    if (prev.getCorrelationId().equals(next.getCorrelationId())) {
                        next.getEntityCompletions().putAll(prev.getEntityCompletions());
                        Long nextRange = next.getKnownInstallation().getAvailableRange();
                        Long prevRange = prev.getKnownInstallation().getProcessedRange();
                        if(nextRange != null && (prevRange == null || nextRange > prevRange)) {
                            next.getKnownInstallation().setProcessedRange(nextRange);
                        }
                        return next;
                    }

                    if (prev.getRequestTime() <= next.getRequestTime()) {
                        //more recent request found
                        next.setEntityType(null);
                        return next;
                    } else {
                        //old request encountered... how?
                        return prev;
                    }

                }).filter((k, v) -> v.getEntityCompletions().keySet().containsAll(ENTITIES))
                .toStream()
                .join(knownInstallsTable, (cmd, known) -> {
                    known.setProcessedRange(cmd.getKnownInstallation().getAvailableRange());
                    known.setLastProcessedTime(System.currentTimeMillis());
                    return known;
                }).to(config.getKnownInstallationTopic());


        Properties props = new Properties();
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

        LOG.info("Unoptimized Topology: {}", builder.build().describe());

        Topology topology = builder.build(props);
        LOG.info("Optimized Toplogy: {}", topology.describe());

        return topology;
    }


    //methods to mock out external behavior
    public long fetchAvailableInstallRange(String installId, Long lastRange) {
        if (lastRange == null) {
            return RAND.nextInt(100) + 1;
        }

        if (RAND.nextInt(10) < 3) {
            return lastRange + RAND.nextInt(5);
        }
        return lastRange;
    }

    //system http endpoint call
    public List<String> fetchUpdates(SyncInstallationCmd cmd) {
        return IntStream.range(1, RAND.nextInt(5) + 1).mapToObj(i ->
                cmd.getKnownInstallation().getInstallId() + " "
                + cmd.getKnownInstallation().getAvailableRange() + " "
                + cmd.getEntityType() + ":" + i).collect(Collectors.toList());
    }

    public static class CmdResults {
        private SyncInstallationCmd cmd;
        private List<String> results;

        public CmdResults(SyncInstallationCmd cmd, List<String> results) {
            this.cmd = cmd;
            this.results = results;
        }
    }
}
