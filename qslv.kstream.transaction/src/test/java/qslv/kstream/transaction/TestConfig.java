package qslv.kstream.transaction;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

@Configuration
@EnableKafkaStreams
public class TestConfig {
	private static final Logger log = LoggerFactory.getLogger(SerdesConfig.class);
	
	@Autowired
	ConfigProperties configProperties;
	@Value("${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
	private String brokerAddresses;
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	@Profile("test")
	public KafkaStreamsConfiguration defaultKafkaStreamsConfig() throws Exception {
		Properties kafkaconfig = new Properties();
		try {
			kafkaconfig.load(new FileInputStream(configProperties.getKafkaStreamsPropertiesPath()));
		} catch (Exception fileEx) {
			try {
				kafkaconfig.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(configProperties.getKafkaStreamsPropertiesPath()));
			} catch (Exception resourceEx) {
				log.error("{} not found.", configProperties.getKafkaStreamsPropertiesPath());
				log.error("File Exception. {}", fileEx.toString());
				log.error("Resource Exception. {}", resourceEx.toString());
				throw resourceEx;
			}
		}
		kafkaconfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
		return new KafkaStreamsConfiguration(new HashMap(kafkaconfig));
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Bean
	@Profile("test")
	public Map<String,String> kafkaConsumerConfig() throws Exception {
		Properties kafkaconfig = new Properties();
		try {
			kafkaconfig.load(new FileInputStream(configProperties.getKafkaConsumerPropertiesPath()));
		} catch (Exception fileEx) {
			try {
				kafkaconfig.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(configProperties.getKafkaConsumerPropertiesPath()));
			} catch (Exception resourceEx) {
				log.error("{} not found.", configProperties.getKafkaConsumerPropertiesPath());
				log.error("File Exception. {}", fileEx.toString());
				log.error("Resource Exception. {}", resourceEx.toString());
				throw resourceEx;
			}
		}
		kafkaconfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
		return new HashMap(kafkaconfig);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Bean
	@Profile("test")
	public Map<String,String> kafkaProducerConfig() throws Exception {
		Properties kafkaconfig = new Properties();
		try {
			kafkaconfig.load(new FileInputStream(configProperties.getKafkaProducerPropertiesPath()));
		} catch (Exception fileEx) {
			try {
				kafkaconfig.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(configProperties.getKafkaProducerPropertiesPath()));
			} catch (Exception resourceEx) {
				log.error("{} not found.", configProperties.getKafkaProducerPropertiesPath());
				log.error("File Exception. {}", fileEx.toString());
				log.error("Resource Exception. {}", resourceEx.toString());
				throw resourceEx;
			}
		}
		kafkaconfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
		return new HashMap(kafkaconfig);
	}
}
