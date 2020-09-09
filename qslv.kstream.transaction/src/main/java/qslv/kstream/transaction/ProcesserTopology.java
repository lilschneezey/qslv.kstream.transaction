package qslv.kstream.transaction;

import java.time.LocalDateTime;
import java.util.ArrayList;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import qslv.common.kafka.ResponseMessage;
import qslv.common.kafka.TraceableMessage;
import qslv.data.BalanceLog;
import qslv.kstream.BaseTransactionRequest;
import qslv.kstream.LoggedTransaction;
import qslv.kstream.PostingResponse;
import qslv.kstream.PostingRequest;
import qslv.kstream.workflow.CancelReservationWorkflow;
import qslv.kstream.workflow.CommitReservationWorkflow;
import qslv.kstream.workflow.ReservationWorkflow;
import qslv.kstream.workflow.TransactionWorkflow;
import qslv.kstream.workflow.TransferWorkflow;
import qslv.kstream.workflow.WorkflowMessage;
import qslv.util.ServiceLevelIndicator;

@Component
public class ProcesserTopology {
	private static final Logger log = LoggerFactory.getLogger(ProcesserTopology.class);

	@Autowired
	ConfigProperties configProperties;
	@Autowired
	Serde<TraceableMessage<WorkflowMessage>> enhancedPostingRequestSerde;
	@Autowired
	Serde<TraceableMessage<LoggedTransaction>> loggedTransactionSerde;
	@Autowired
	Serde<ResponseMessage<PostingRequest, PostingResponse>> responseSerde;
	@Autowired
	Serde<BalanceLog> balanceLogSerde;
	
	public void setConfigProperties(ConfigProperties configProperties) {
		this.configProperties = configProperties;
	}

	public void setEnhancedPostingRequestSerde(
			Serde<TraceableMessage<WorkflowMessage>> enhancedPostingRequestSerde) {
		this.enhancedPostingRequestSerde = enhancedPostingRequestSerde;
	}

	public void setLoggedTransactionSerde(Serde<TraceableMessage<LoggedTransaction>> loggedTransactionSerde) {
		this.loggedTransactionSerde = loggedTransactionSerde;
	}

	public void setResponseSerde(Serde<ResponseMessage<PostingRequest, PostingResponse>> responseSerde) {
		this.responseSerde = responseSerde;
	}

	public void setBalanceLogSerde(Serde<BalanceLog> balanceLogSerde) {
		this.balanceLogSerde = balanceLogSerde;
	}
	
	@Bean
	public KStream<?, ?> kStream(StreamsBuilder builder) {
		KStream<String, TraceableMessage<WorkflowMessage>> enhancedRequests = builder
				.stream(configProperties.getEnhancedRequestTopic(), Consumed.with(Serdes.String(), enhancedPostingRequestSerde));

		KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(configProperties.getBalanceStoreName());

		builder.table(configProperties.getBalanceLogTopic(), 
				Consumed.with(Serdes.String(), balanceLogSerde),
				Materialized.<String, BalanceLog>as(storeSupplier)
				.withKeySerde(Serdes.String())
				.withValueSerde(balanceLogSerde)
				.withCachingEnabled()
				.withLoggingDisabled()
			);
		
		//------------------------------------
		// Process Request
		//------------------------------------
		@SuppressWarnings("unchecked")
		KStream<String, TraceableMessage<WorkflowMessage>>[] processingResults = enhancedRequests
				.mapValues((v) -> resetRequest(v))
				.transformValues(()->new ProcessingTransformer(configProperties), configProperties.getBalanceStoreName())
				.branch(
						(k,v) -> isComplete(k,v),
						(k,v) -> true
				);
		// -- Completed Stream -----
		KStream<String, TraceableMessage<WorkflowMessage>> completedStream = processingResults[0]
				.mapValues( (v) -> mapCompletionTime(v) )
				.peek((k,v) -> ServiceLevelIndicator.logAsyncServiceElapsedTime(log, "qslv.kstream.transaction", configProperties.getAitid(), v.getMessageCreationTime()) );
		// send the reply back to the requester 
		completedStream
				.map( (k,v) -> mapResponse(k,v) )
				.to(configProperties.getResponseTopic(), Produced.with(Serdes.String(), responseSerde));
		// record a log of the transaction(s)
		completedStream
				.flatMap((k,v) -> mapTransactions(k,v))
				.to(configProperties.getTransactionLogTopic(), Produced.with(Serdes.String(), loggedTransactionSerde));

		// --- Stream for the next Account Processor ---
		processingResults[1]
				.map((k,v) -> rekeyRequest(k,v) )
				.to(configProperties.getEnhancedRequestTopic(), Produced.with(Serdes.String(), enhancedPostingRequestSerde) );
		// --- Log the Transactions complete so far ---
		processingResults[1]
			.flatMap((k,v) -> mapTransactions(k,v))
			.to(configProperties.getTransactionLogTopic(), Produced.with(Serdes.String(), loggedTransactionSerde));
		
		return enhancedRequests;
	}
	
	private KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> mapResponse( String key, TraceableMessage<WorkflowMessage> message ) {
		WorkflowMessage workflowMessage = message.getPayload();
		BaseTransactionRequest request = null;
		int status = ResponseMessage.INTERNAL_ERROR;

		if ( workflowMessage.hasReservationWorkflow() ) {
			request = workflowMessage.getReservationWorkflow().getRequest();
			switch (workflowMessage.getWorkflow().getState()) {
				case ReservationWorkflow.INSUFFICIENT_FUNDS:
					status = ResponseMessage.INSUFFICIENT_FUNDS; break;
				case ReservationWorkflow.SUCCESS:
					status = ResponseMessage.SUCCESS; break;
				case ReservationWorkflow.FAILURE:
				default:
					status = ResponseMessage.INTERNAL_ERROR; break;
			}
			
		} else if (workflowMessage.hasCancelReservationWorkflow() ) {
			request = workflowMessage.getCancelReservationWorkflow().getRequest();
			switch (workflowMessage.getWorkflow().getState()) {
				case CancelReservationWorkflow.NO_MATCH:
					status = ResponseMessage.CONFLICT; break;
				case CancelReservationWorkflow.SUCCESS:
					status = ResponseMessage.SUCCESS; break;
				case CancelReservationWorkflow.FAILURE:
				default:
					status = ResponseMessage.INTERNAL_ERROR; break;
			}

		} else if (workflowMessage.hasCommitReservationWorkflow()) {
			request = workflowMessage.getCommitReservationWorkflow().getRequest();
			switch (workflowMessage.getWorkflow().getState()) {
				case CommitReservationWorkflow.NO_MATCH:
					status = ResponseMessage.CONFLICT; break;
				case CommitReservationWorkflow.SUCCESS:
					status = ResponseMessage.SUCCESS; break;
				case CommitReservationWorkflow.FAILURE:
				default:
					status = ResponseMessage.INTERNAL_ERROR; break;
			}

		} else if (workflowMessage.hasTransferWorkflow() ) {
			request = workflowMessage.getTransferWorkflow().getRequest();
			switch (workflowMessage.getWorkflow().getState()) {
				case TransferWorkflow.INSUFFICIENT_FUNDS:
					status = ResponseMessage.INSUFFICIENT_FUNDS; break;
				case TransferWorkflow.SUCCESS:
					status = ResponseMessage.SUCCESS; break;
				case TransferWorkflow.FAILURE:
				default:
					status = ResponseMessage.INTERNAL_ERROR; break;
			}

		} else {
			request = workflowMessage.getTransactionWorkflow().getRequest();
			switch (workflowMessage.getWorkflow().getState()) {
				case TransactionWorkflow.INSUFFICIENT_FUNDS:
					status = ResponseMessage.INSUFFICIENT_FUNDS; break;
				case TransactionWorkflow.SUCCESS:
					status = ResponseMessage.SUCCESS; break;
				case TransactionWorkflow.FAILURE:
				default:
					status = ResponseMessage.INTERNAL_ERROR; break;
			}

		}
		
		ResponseMessage<PostingRequest, PostingResponse> response = new ResponseMessage<>(
				message, new PostingRequest(request), new PostingResponse( workflowMessage.getWorkflow().getAccumulatedResults() ) );
		response.setStatus(status);
		response.setErrorMessage(workflowMessage.getWorkflow().getErrorMessage());
		return new KeyValue<>(workflowMessage.getResponseKey(), response);
	}
	
	private Iterable<KeyValue<String,TraceableMessage<LoggedTransaction>>> mapTransactions( String key, TraceableMessage<WorkflowMessage> message ) {
		ArrayList<LoggedTransaction> results = message.getPayload().getWorkflow().getResults();
		ArrayList<KeyValue<String, TraceableMessage<LoggedTransaction>>> list = new ArrayList<>();
		for(LoggedTransaction transaction: results ) {
			TraceableMessage<LoggedTransaction> tlt = new TraceableMessage<>(message, transaction);
			if (tlt.getMessageCompletionTime() == null) tlt.setMessageCompletionTime(LocalDateTime.now());
			list.add(new KeyValue<>(transaction.getAccountNumber(), tlt));
			log.debug("Log Transaction UUID {}", transaction.getTransactionUuid());
		}
		return list;
	}
	
	private TraceableMessage<WorkflowMessage> mapCompletionTime(TraceableMessage<WorkflowMessage> message) {
		message.setMessageCompletionTime(LocalDateTime.now());
		return message;
	}
	
	private TraceableMessage<WorkflowMessage> resetRequest(TraceableMessage<WorkflowMessage> message) {
		message.getPayload().getWorkflow().setResults(null);
		if (message.getPayload().getWorkflow().getAccumulatedResults() == null) 
			message.getPayload().getWorkflow().setAccumulatedResults(new ArrayList<LoggedTransaction>());
		if (message.getPayload().getWorkflow().getResults() == null) 
			message.getPayload().getWorkflow().setResults(new ArrayList<LoggedTransaction>());
		return message;
	}
	
	private KeyValue<String, TraceableMessage<WorkflowMessage>> rekeyRequest(String key, TraceableMessage<WorkflowMessage> message) {
		WorkflowMessage workflowMessage = message.getPayload();
		String nextAccountNumber = null;
		
		if ( workflowMessage.hasReservationWorkflow() ) {
			nextAccountNumber = workflowMessage.getReservationWorkflow().getProcessingAccountNumber();
		} else if (workflowMessage.hasCommitReservationWorkflow()) {
			nextAccountNumber = workflowMessage.getCommitReservationWorkflow().getProcessingAccountNumber();
		} else if (workflowMessage.hasTransferWorkflow() ) {
			nextAccountNumber = workflowMessage.getTransferWorkflow().getTransferToAccount().getAccountNumber();
		} else {
			nextAccountNumber = workflowMessage.getTransactionWorkflow().getProcessingAccountNumber();
		}

		log.debug("Advancing to the next OD account {}", nextAccountNumber);
		return new KeyValue<>(nextAccountNumber, message);
	}
	
	boolean isComplete(String key, TraceableMessage<WorkflowMessage> message) {
		WorkflowMessage workflowMessage = message.getPayload();
		int state = workflowMessage.getWorkflow().getState();
		boolean result = true;
		
		if ( workflowMessage.hasReservationWorkflow() ) {
			result = ( ReservationWorkflow.FAILURE == state 
					|| ReservationWorkflow.SUCCESS == state
					|| ReservationWorkflow.INSUFFICIENT_FUNDS == state ); 
			
		} else if (workflowMessage.hasCancelReservationWorkflow() ) {
			result = ( CancelReservationWorkflow.FAILURE == state
					|| CancelReservationWorkflow.NO_MATCH == state
					|| CancelReservationWorkflow.SUCCESS == state);			
			
		} else if (workflowMessage.hasCommitReservationWorkflow()) {
			result = ( CommitReservationWorkflow.FAILURE == state
					|| CommitReservationWorkflow.NO_MATCH == state
					|| CommitReservationWorkflow.SUCCESS == state);
			
		} else if (workflowMessage.hasTransferWorkflow() ) {
			result = ( TransferWorkflow.FAILURE == state
					|| TransferWorkflow.INSUFFICIENT_FUNDS == state
					|| TransferWorkflow.SUCCESS == state);
			
		} else {
			result = ( TransactionWorkflow.FAILURE == state
					|| TransactionWorkflow.INSUFFICIENT_FUNDS == state
					|| TransactionWorkflow.SUCCESS == state);
		}
		return result;
	}
}
