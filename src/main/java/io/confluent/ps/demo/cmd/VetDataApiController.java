package io.confluent.ps.demo.cmd;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.ps.KnownInstallation;
import io.confluent.ps.KnownInstallationKey;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RestController
public class VetDataApiController {
    private static final Logger LOG = LoggerFactory.getLogger(VetDataApiController.class);
    private static VetDemoApplication.Config config;
    private Producer<KnownInstallationKey, KnownInstallation> producer;

    @Autowired
    public VetDataApiController(VetDemoApplication.Config config) {
        this.config = config;
    }

    @PostConstruct
    public void onStart() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, config.getSchemaRegistryUrl());


        producer = new KafkaProducer<>(producerProps);
    }

    @PreDestroy
    public void onStop() {
        if(producer != null) {
            producer.close();
        }
    }

    @GetMapping(value = "/test")
    public void doTest() {
        InstallationList l = new InstallationList();
        l.setInstallations(IntStream.range(1, 10).mapToObj(Integer::toString).map(s ->
                new Installation().setInstallId(s)
                    .setPims("p" + s)
                    .setPracticeName("Practice " + s))
                .collect(Collectors.toList()));

        setInstalls(l);
    }

    @PostMapping(value = "/installs", consumes = "application/json")
    public void setInstalls(@RequestBody InstallationList installs) {
        Optional.of(installs.installations).ifPresent(installList ->{
            installList.forEach( install -> {
                var key = new KnownInstallationKey(install.installId, install.practiceName, install.pims);
                var value = KnownInstallation.newBuilder()
                        .setInstallId(install.installId)
                        .setPracticeName(install.practiceName)
                        .setPIMS(install.pims)
                        .build();

                producer.send(new ProducerRecord<>(config.getVetDataUpdateTopic(), key, value), (meta, err) -> {
                    if(err != null) {
                        LOG.error("Failed publishing vet data.", err);
                    }
                });
            });
        });
    }

    public static class Installation {
        private String installId;
        private String practiceName;
        private String pims;

        public String getInstallId() {
            return installId;
        }

        public Installation setInstallId(String installId) {
            this.installId = installId;
            return this;
        }

        public String getPracticeName() {
            return practiceName;
        }

        public Installation setPracticeName(String practiceName) {
            this.practiceName = practiceName;
            return this;
        }

        public String getPims() {
            return pims;
        }

        public Installation setPims(String pims) {
            this.pims = pims;
            return this;
        }
    }

    public static class InstallationList {
        private List<Installation> installations;

        public List<Installation> getInstallations() {
            return installations;
        }

        public InstallationList setInstallations(List<Installation> installations) {
            this.installations = installations;
            return this;
        }
    }
}
