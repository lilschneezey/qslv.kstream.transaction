package qslv.kstream.transaction;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import qslv.util.EnableQuickSilver;

@Configuration
@ConfigurationProperties(prefix = "qslv")
@EnableQuickSilver
public class ConfigProperties {

	private String aitid;
	
	private String enhancedRequestTopic = null;
	private String responseTopic = null;
	private String transactionLogTopic = null;
	private String balanceLogTopic = null;
	private String balanceStoreName = null;
	private String kafkaConsumerPropertiesPath = null;
	private String kafkaProducerPropertiesPath = null;
	private String kafkaStreamsPropertiesPath = null;
	
	public String getAitid() {
		return aitid;
	}
	public void setAitid(String aitid) {
		this.aitid = aitid;
	}
	public String getEnhancedRequestTopic() {
		return enhancedRequestTopic;
	}
	public void setEnhancedRequestTopic(String enhancedRequestTopic) {
		this.enhancedRequestTopic = enhancedRequestTopic;
	}
	public String getResponseTopic() {
		return responseTopic;
	}
	public void setResponseTopic(String responseTopic) {
		this.responseTopic = responseTopic;
	}
	public String getTransactionLogTopic() {
		return transactionLogTopic;
	}
	public void setTransactionLogTopic(String transactionLogTopic) {
		this.transactionLogTopic = transactionLogTopic;
	}
	public String getBalanceLogTopic() {
		return balanceLogTopic;
	}
	public void setBalanceLogTopic(String balanceLogTopic) {
		this.balanceLogTopic = balanceLogTopic;
	}
	public String getBalanceStoreName() {
		return balanceStoreName;
	}
	public void setBalanceStoreName(String balanceStoreName) {
		this.balanceStoreName = balanceStoreName;
	}
	public String getKafkaConsumerPropertiesPath() {
		return kafkaConsumerPropertiesPath;
	}
	public void setKafkaConsumerPropertiesPath(String kafkaConsumerPropertiesPath) {
		this.kafkaConsumerPropertiesPath = kafkaConsumerPropertiesPath;
	}
	public String getKafkaProducerPropertiesPath() {
		return kafkaProducerPropertiesPath;
	}
	public void setKafkaProducerPropertiesPath(String kafkaProducerPropertiesPath) {
		this.kafkaProducerPropertiesPath = kafkaProducerPropertiesPath;
	}
	public String getKafkaStreamsPropertiesPath() {
		return kafkaStreamsPropertiesPath;
	}
	public void setKafkaStreamsPropertiesPath(String kafkaStreamsPropertiesPath) {
		this.kafkaStreamsPropertiesPath = kafkaStreamsPropertiesPath;
	}

}
