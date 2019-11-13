package io.confluent.ps.demo.cmd;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@SpringBootApplication
public class VetDemoApplication {

    public static void main(String... args) {
        SpringApplication.run(VetDemoApplication.class, args);
    }

    @Bean
    public StreamsProcessor streamsProcessor(Config config) {

        return new StreamsProcessor(config);
    }

    @Bean
    @ConfigurationProperties(prefix="demo")
    public Config config() {
        return new Config();
    }

    public static class Config {
        private String appName;
        private String knownInstallationTopic;
        private String installationCmdTopic;
        private String installationEntityUpdateTopic;
        private String vetDataUpdateTopic;
        private String bootstrapServers;
        private String schemaRegistryUrl;
        private Map<String, String> streams = new HashMap<>();


        public Properties streamsConfig() {
            Properties streamsConfiguration = new Properties();

            streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
            streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, appName);
            streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            streamsConfiguration.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
            streamsConfiguration.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

            streams.forEach((k, v) -> streamsConfiguration.putIfAbsent(k, v));


            return streamsConfiguration;
        }

        public Map<String, String> getStreams() {
            return streams;
        }

        public Config setStreams(Map<String, String> streams) {
            this.streams = streams;
            return this;
        }

        public String getInstallationEntityUpdateTopic() {
            return installationEntityUpdateTopic;
        }

        public Config setInstallationEntityUpdateTopic(String installationEntityUpdateTopic) {
            this.installationEntityUpdateTopic = installationEntityUpdateTopic;
            return this;
        }

        public String getInstallationCmdTopic() {
            return installationCmdTopic;
        }

        public void setInstallationCmdTopic(String installationCmdTopic) {
            this.installationCmdTopic = installationCmdTopic;
        }

        public String getVetDataUpdateTopic() {
            return vetDataUpdateTopic;
        }

        public Config setVetDataUpdateTopic(String vetDataUpdateTopic) {
            this.vetDataUpdateTopic = vetDataUpdateTopic;
            return this;
        }

        public String getAppName() {
            return appName;
        }

        public void setAppName(String appName) {
            this.appName = appName;
        }

        public String getKnownInstallationTopic() {
            return knownInstallationTopic;
        }

        public void setKnownInstallationTopic(String knownInstallationTopic) {
            this.knownInstallationTopic = knownInstallationTopic;
        }

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getSchemaRegistryUrl() {
            return schemaRegistryUrl;
        }

        public void setSchemaRegistryUrl(String schemaRegistryUrl) {
            this.schemaRegistryUrl = schemaRegistryUrl;
        }
    }
}
