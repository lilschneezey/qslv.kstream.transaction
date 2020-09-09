package qslv.kstream.transaction;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

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

public class TestSetup {

	static public final String SCHEMA_REGISTRY = "http://localhost:8081";

	static public final String BALANCE_LOG_TOPIC = "balance.log";
	static public final String BALANCE_STORE = "balance.store";
	static public final String ENHANCED_REQUEST_TOPIC = "enhanced.request";
	static public final String RESPONSE_TOPIC = "posting.response";
	static public final String TRANSACTION_LOG_TOPIC = "transaction.log";

	private ConfigProperties configProperties = new ConfigProperties();

	private Serde<TraceableMessage<WorkflowMessage>> enhancedPostingRequestSerde;
	private Serde<TraceableMessage<LoggedTransaction>> loggedTransactionSerde;
	private Serde<ResponseMessage<PostingRequest, PostingResponse>> responseSerde;
	private Serde<BalanceLog> balanceLogSerde;
	private ProcesserTopology processerTopology = new ProcesserTopology();

	private TopologyTestDriver testDriver;

	private TestInputTopic<String, BalanceLog> balanceLogTopic;
	private TestInputTopic<String, TraceableMessage<WorkflowMessage>> requestTopic;
	private TestOutputTopic<String, ResponseMessage<PostingRequest, PostingResponse>> responseTopic;
	private TestOutputTopic<String, TraceableMessage<LoggedTransaction>> transactionLogTopic;

	private KeyValueStore<String, ValueAndTimestamp<BalanceLog>> balanceStore;

	public TestSetup() throws Exception {
		configProperties.setAitid("12345");
		configProperties.setBalanceLogTopic(BALANCE_LOG_TOPIC);
		configProperties.setBalanceStoreName(BALANCE_STORE);
		configProperties.setEnhancedRequestTopic(ENHANCED_REQUEST_TOPIC);
		configProperties.setResponseTopic(RESPONSE_TOPIC);
		configProperties.setTransactionLogTopic(TRANSACTION_LOG_TOPIC);

		Map<String, String> config = new HashMap<>();
		config.put("schema.registry.url", SCHEMA_REGISTRY);
		enhancedPostingRequestSerde = enhancedPostingRequestSerde(config);
		loggedTransactionSerde = loggedTransactionSerde(config);
		responseSerde = responseSerde(config);
		balanceLogSerde = balanceLogSerde(config);

		processerTopology.setBalanceLogSerde(balanceLogSerde);
		processerTopology.setConfigProperties(configProperties);
		processerTopology.setEnhancedPostingRequestSerde(enhancedPostingRequestSerde);
		processerTopology.setLoggedTransactionSerde(loggedTransactionSerde);
		processerTopology.setResponseSerde(responseSerde);

		StreamsBuilder builder = new StreamsBuilder();
		processerTopology.kStream(builder);

		Topology topology = builder.build();

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "unit.reservation");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
		testDriver = new TopologyTestDriver(topology, props);

		balanceLogTopic = testDriver.createInputTopic(BALANCE_LOG_TOPIC, Serdes.String().serializer(),
				balanceLogSerde.serializer());
		requestTopic = testDriver.createInputTopic(ENHANCED_REQUEST_TOPIC, Serdes.String().serializer(),
				enhancedPostingRequestSerde.serializer());
		responseTopic = testDriver.createOutputTopic(RESPONSE_TOPIC, Serdes.String().deserializer(),
				responseSerde.deserializer());
		transactionLogTopic = testDriver.createOutputTopic(TRANSACTION_LOG_TOPIC, Serdes.String().deserializer(),
				loggedTransactionSerde.deserializer());

		balanceStore = testDriver.getTimestampedKeyValueStore(BALANCE_STORE);
	}

	static public Serde<TraceableMessage<WorkflowMessage>> enhancedPostingRequestSerde(Map<String, ?> config)
			throws Exception {
		JacksonAvroSerializer<TraceableMessage<WorkflowMessage>> serializer = new JacksonAvroSerializer<>();
		JacksonAvroDeserializer<TraceableMessage<WorkflowMessage>> deserializer = new JacksonAvroDeserializer<>();
		JavaType type = serializer.getTypeFactory().constructParametricType(TraceableMessage.class,
				WorkflowMessage.class);
		serializer.configure(config, false, type);
		deserializer.configure(config, false);
		return Serdes.serdeFrom(serializer, deserializer);
	}

	static public Serde<TraceableMessage<LoggedTransaction>> loggedTransactionSerde(Map<String, ?> config)
			throws Exception {
		JacksonAvroSerializer<TraceableMessage<LoggedTransaction>> serializer = new JacksonAvroSerializer<>();
		JacksonAvroDeserializer<TraceableMessage<LoggedTransaction>> deserializer = new JacksonAvroDeserializer<>();
		JavaType type = serializer.getTypeFactory().constructParametricType(TraceableMessage.class,
				LoggedTransaction.class);
		serializer.configure(config, false, type);
		deserializer.configure(config, false);
		return Serdes.serdeFrom(serializer, deserializer);
	}

	static public Serde<ResponseMessage<PostingRequest, PostingResponse>> responseSerde(Map<String, ?> config)
			throws Exception {
		JacksonAvroSerializer<ResponseMessage<PostingRequest, PostingResponse>> serializer = new JacksonAvroSerializer<>();
		JacksonAvroDeserializer<ResponseMessage<PostingRequest, PostingResponse>> deserializer = new JacksonAvroDeserializer<>();
		JavaType type = serializer.getTypeFactory().constructParametricType(ResponseMessage.class, PostingRequest.class,
				PostingResponse.class);
		serializer.configure(config, false, type);
		deserializer.configure(config, false);
		return Serdes.serdeFrom(serializer, deserializer);
	}

	static public Serde<BalanceLog> balanceLogSerde(Map<String, ?> config) throws Exception {
		JacksonAvroSerializer<BalanceLog> serializer = new JacksonAvroSerializer<>();
		JacksonAvroDeserializer<BalanceLog> deserializer = new JacksonAvroDeserializer<>();
		serializer.configure(config, false);
		deserializer.configure(config, false);
		return Serdes.serdeFrom(serializer, deserializer);
	}

	public TopologyTestDriver getTestDriver() {
		return testDriver;
	}

	public TestInputTopic<String, BalanceLog> getBalanceLogTopic() {
		return balanceLogTopic;
	}

	public TestInputTopic<String, TraceableMessage<WorkflowMessage>> getRequestTopic() {
		return requestTopic;
	}

	public TestOutputTopic<String, ResponseMessage<PostingRequest, PostingResponse>> getResponseTopic() {
		return responseTopic;
	}

	public TestOutputTopic<String, TraceableMessage<LoggedTransaction>> getTransactionLogTopic() {
		return transactionLogTopic;
	}

	public KeyValueStore<String, ValueAndTimestamp<BalanceLog>> getBalanceStore() {
		return balanceStore;
	}

}
