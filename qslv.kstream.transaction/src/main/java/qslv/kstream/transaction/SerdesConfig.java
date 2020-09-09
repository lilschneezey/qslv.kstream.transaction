package qslv.kstream.transaction;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import com.fasterxml.jackson.databind.JavaType;

import qslv.common.kafka.JacksonAvroDeserializer;
import qslv.common.kafka.JacksonAvroSerializer;
import qslv.common.kafka.ResponseMessage;
import qslv.common.kafka.TraceableMessage;
import qslv.data.BalanceLog;
import qslv.kstream.LoggedTransaction;
import qslv.kstream.PostingResponse;
import qslv.kstream.workflow.WorkflowMessage;
import qslv.kstream.PostingRequest;

@Configuration
@EnableKafkaStreams
public class SerdesConfig {
	private static final Logger log = LoggerFactory.getLogger(SerdesConfig.class);
	
	@Autowired
	ConfigProperties configProperties;
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	@Profile("!test")
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
		return new KafkaStreamsConfiguration(new HashMap(kafkaconfig));
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Bean
	@Profile("!test")
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
		return new HashMap(kafkaconfig);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Bean
	@Profile("!test")
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
		return new HashMap(kafkaconfig);
	}
	
	@Bean
	Serde<TraceableMessage<WorkflowMessage>> enhancedPostingRequestSerde() throws Exception {
		JacksonAvroSerializer<TraceableMessage<WorkflowMessage>> serializer = new JacksonAvroSerializer<>();
		JacksonAvroDeserializer<TraceableMessage<WorkflowMessage>> deserializer = new JacksonAvroDeserializer<>();
		JavaType type = serializer.getTypeFactory().constructParametricType(TraceableMessage.class, WorkflowMessage.class);
		serializer.configure(kafkaProducerConfig(), false, type);
		deserializer.configure(kafkaConsumerConfig(), false);
		return Serdes.serdeFrom(serializer, deserializer);
	}	

	@Bean
	Serde<TraceableMessage<LoggedTransaction>> loggedTransactionSerde() throws Exception {
		JacksonAvroSerializer<TraceableMessage<LoggedTransaction>> serializer = new JacksonAvroSerializer<>();
		JacksonAvroDeserializer<TraceableMessage<LoggedTransaction>> deserializer = new JacksonAvroDeserializer<>();
		JavaType type = serializer.getTypeFactory().constructParametricType(TraceableMessage.class, LoggedTransaction.class);
		serializer.configure(kafkaProducerConfig(), false, type);
		deserializer.configure(kafkaConsumerConfig(), false);
		return Serdes.serdeFrom(serializer, deserializer);
	}	

	@Bean
	Serde<ResponseMessage<PostingRequest, PostingResponse>> responseSerde() throws Exception {
		JacksonAvroSerializer<ResponseMessage<PostingRequest, PostingResponse>> serializer = new JacksonAvroSerializer<>();
		JacksonAvroDeserializer<ResponseMessage<PostingRequest, PostingResponse>> deserializer = new JacksonAvroDeserializer<>();
		JavaType type = serializer.getTypeFactory().constructParametricType(ResponseMessage.class, PostingRequest.class, PostingResponse.class);
		serializer.configure(kafkaProducerConfig(), false, type);
		deserializer.configure(kafkaConsumerConfig(), false);
		return Serdes.serdeFrom(serializer, deserializer);
	}	

	@Bean
	Serde<BalanceLog> balanceLogSerde() throws Exception {
		JacksonAvroSerializer<BalanceLog> serializer = new JacksonAvroSerializer<>();
		JacksonAvroDeserializer<BalanceLog> deserializer = new JacksonAvroDeserializer<>();
		serializer.configure(kafkaProducerConfig(), false);
		deserializer.configure(kafkaConsumerConfig(), false);
		return Serdes.serdeFrom(serializer, deserializer);
	}	
}
