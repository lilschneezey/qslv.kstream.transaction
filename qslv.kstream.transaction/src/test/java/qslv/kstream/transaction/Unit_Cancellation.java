package qslv.kstream.transaction;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.UUID;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import qslv.common.kafka.ResponseMessage;
import qslv.common.kafka.TraceableMessage;
import qslv.data.Account;
import qslv.data.BalanceLog;
import qslv.kstream.CancelReservationRequest;
import qslv.kstream.LoggedTransaction;
import qslv.kstream.PostingResponse;
import qslv.kstream.PostingRequest;
import qslv.kstream.workflow.CancelReservationWorkflow;
import qslv.kstream.workflow.WorkflowMessage;
import qslv.util.Random;

@ExtendWith(MockitoExtension.class)
class Unit_Cancellation {

	public final static String AIT = "237482"; 
	public final static String TEST_TAXONOMY_ID = "9.9.9.9.9";
	public final static String CORRELATION_ID = UUID.randomUUID().toString();
	public final static String VALID_STATUS = "EF";
	public final static String INVALID_STATUS = "CL";
	public final static String JSON_DATA = "{\"value\": 234934}";

	static TestSetup context;
	
	@BeforeAll
	static void beforeAl() throws Exception {
		context = new TestSetup();
	}
	
	@AfterAll
	static void afterAll() {
		context.getTestDriver().close();
	}

    @Test
	void test_cancellation_success() {
    	drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());
    	
		long startingBalance = 1111L;
		long transactionAmount = -4444L;
		long expectedBalance = startingBalance - transactionAmount;
		
		// - setup --------------------
		Account account = randomAccount(true, true);	
		resetBalance(account.getAccountNumber(), startingBalance);
		
		// - prepare --------------------
		CancelReservationRequest cancelRequest = new CancelReservationRequest();
		cancelRequest.setRequestUuid(UUID.randomUUID());
		cancelRequest.setAccountNumber(account.getAccountNumber());
		cancelRequest.setReservationUuid(UUID.randomUUID());
		cancelRequest.setJsonMetaData(JSON_DATA);
		
		LoggedTransaction reservation = new LoggedTransaction();
		reservation.setAccountNumber(cancelRequest.getAccountNumber());
		reservation.setDebitCardNumber(Random.randomDigits(12));
		reservation.setRequestUuid(UUID.randomUUID());
		reservation.setReservationUuid(null);
		reservation.setRunningBalanceAmount(startingBalance);
		reservation.setTransactionAmount(transactionAmount);
		reservation.setTransactionMetaDataJson(JSON_DATA);
		reservation.setTransactionTime(LocalDateTime.now());
		reservation.setTransactionTypeCode(LoggedTransaction.RESERVATION);
		reservation.setTransactionUuid(cancelRequest.getReservationUuid());

		CancelReservationWorkflow workflow = new CancelReservationWorkflow(CancelReservationWorkflow.CANCEL_RESERVATION, cancelRequest);
		workflow.setProcessingAccountNumber(account.getAccountNumber());
		workflow.setReservation(reservation);
		
		TraceableMessage<WorkflowMessage> traceable = new TraceableMessage<>();
		traceable.setProducerAit(AIT);
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId(CORRELATION_ID);
		traceable.setPayload(new WorkflowMessage(workflow, Random.randomString(20)));
		traceable.setMessageCreationTime(LocalDateTime.now());

		// - execute ------------------------
		context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
		
		// - verify -------------------------
		KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue = context.getResponseTopic().readKeyValue();
		assertNotNull(keyvalue);
		assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
		assertNotNull(keyvalue.value);
		assertNotNull(keyvalue.value.getRequest());
		
		verifyTraceData(traceable, keyvalue);
		assertNull(keyvalue.value.getErrorMessage());
		assertEquals(ResponseMessage.SUCCESS, keyvalue.value.getStatus());

		CancelReservationRequest rRequest = keyvalue.value.getRequest().getCancelReservationRequest();
		assertEquals( cancelRequest.getAccountNumber(), rRequest.getAccountNumber());
		assertEquals( cancelRequest.getJsonMetaData(), rRequest.getJsonMetaData());
		assertEquals( cancelRequest.getRequestUuid(), rRequest.getRequestUuid());
		assertEquals( cancelRequest.getReservationUuid(), rRequest.getReservationUuid());

		PostingResponse response = keyvalue.value.getResponse();
		assertNotNull(response);
		assertNotNull(response.getTransactions());
		assertEquals(1, response.getTransactions().size());
		
		LoggedTransaction transaction = response.getTransactions().get(0);
		assertEquals( cancelRequest.getAccountNumber(), transaction.getAccountNumber());
		assertEquals( reservation.getDebitCardNumber(), transaction.getDebitCardNumber());
		assertEquals( cancelRequest.getRequestUuid(), transaction.getRequestUuid());
		assertEquals( reservation.getTransactionUuid(), transaction.getReservationUuid());
		assertEquals( expectedBalance, transaction.getRunningBalanceAmount());
		assertEquals( Math.abs(transactionAmount), transaction.getTransactionAmount());
		assertEquals( cancelRequest.getJsonMetaData(), transaction.getTransactionMetaDataJson());
		assertNotNull( transaction.getTransactionTime());
		assertEquals( LoggedTransaction.RESERVATION_CANCEL, transaction.getTransactionTypeCode());
		assertNotNull( transaction.getTransactionUuid());

		// Get Transaction from Log Topic
		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue);
		assertEquals(account.getAccountNumber(), logKeyValue.key);
		
		TraceableMessage<LoggedTransaction> tresult = logKeyValue.value;
		verifyLoggedTraceData(traceable, tresult, keyvalue.value.getMessageCompletionTime());
		verifyTransactions(transaction, tresult.getPayload());

		// Get Balance State Store
		BalanceLog balanceLog = context.getBalanceStore().get(account.getAccountNumber()).value();
		assertNotNull( balanceLog );
		assertEquals( expectedBalance, balanceLog.getBalance() );
		assertEquals( transaction.getTransactionUuid(), balanceLog.getLastTransaction() );
	}

    @Test
	void test_cancellation_noReservation() {
    	drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());
    	
		long startingBalance = 1111L;
		long expectedBalance = startingBalance;
		
		// - setup --------------------
		Account account = randomAccount(true, true);	
		UUID starting_UUID = resetBalance(account.getAccountNumber(), startingBalance);
		
		// - prepare --------------------
		CancelReservationRequest cancelRequest = new CancelReservationRequest();
		cancelRequest.setRequestUuid(UUID.randomUUID());
		cancelRequest.setAccountNumber(account.getAccountNumber());
		cancelRequest.setReservationUuid(UUID.randomUUID());
		cancelRequest.setJsonMetaData(JSON_DATA);

		CancelReservationWorkflow workflow = new CancelReservationWorkflow(CancelReservationWorkflow.CANCEL_RESERVATION, cancelRequest);
		workflow.setProcessingAccountNumber(account.getAccountNumber());
		workflow.setReservation(null);
		
		TraceableMessage<WorkflowMessage> traceable = new TraceableMessage<>();
		traceable.setProducerAit(AIT);
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId(CORRELATION_ID);
		traceable.setPayload(new WorkflowMessage(workflow, Random.randomString(20)));
		traceable.setMessageCreationTime(LocalDateTime.now());

		// - execute ------------------------
		context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
		
		// - verify -------------------------
		KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue = context.getResponseTopic().readKeyValue();
		assertNotNull(keyvalue);
		assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
		assertNotNull(keyvalue.value);
		assertNotNull(keyvalue.value.getRequest());
		
		verifyTraceData(traceable, keyvalue);
		assertTrue(keyvalue.value.getErrorMessage().contains("Reservation not found"));
		assertEquals(ResponseMessage.CONFLICT, keyvalue.value.getStatus());

		CancelReservationRequest rRequest = keyvalue.value.getRequest().getCancelReservationRequest();
		assertEquals( cancelRequest.getAccountNumber(), rRequest.getAccountNumber());
		assertEquals( cancelRequest.getJsonMetaData(), rRequest.getJsonMetaData());
		assertEquals( cancelRequest.getRequestUuid(), rRequest.getRequestUuid());
		assertEquals( cancelRequest.getReservationUuid(), rRequest.getReservationUuid());

		PostingResponse response = keyvalue.value.getResponse();
		assertNotNull(response);
		assertNotNull(response.getTransactions());
		assertEquals(0, response.getTransactions().size());
		
		// Get Balance State Store
		BalanceLog balanceLog = context.getBalanceStore().get(account.getAccountNumber()).value();
		assertNotNull( balanceLog );
		assertEquals( expectedBalance, balanceLog.getBalance() );
		assertEquals( starting_UUID, balanceLog.getLastTransaction() );
	}
    
    @Test
	void test_cancellation_noMatch() {
    	drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());
    	
		long startingBalance = 1111L;
		long expectedBalance = startingBalance;
		
		// - setup --------------------
		Account account = randomAccount(true, true);	
		UUID starting_UUID = resetBalance(account.getAccountNumber(), startingBalance);
		
		// - prepare --------------------
		CancelReservationRequest cancelRequest = new CancelReservationRequest();
		cancelRequest.setRequestUuid(UUID.randomUUID());
		cancelRequest.setAccountNumber(account.getAccountNumber());
		cancelRequest.setReservationUuid(UUID.randomUUID());
		cancelRequest.setJsonMetaData(JSON_DATA);

		CancelReservationWorkflow workflow = new CancelReservationWorkflow(CancelReservationWorkflow.NO_MATCH, cancelRequest);
		workflow.setProcessingAccountNumber(account.getAccountNumber());
		workflow.setReservation(null);
		
		TraceableMessage<WorkflowMessage> traceable = new TraceableMessage<>();
		traceable.setProducerAit(AIT);
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId(CORRELATION_ID);
		traceable.setPayload(new WorkflowMessage(workflow, Random.randomString(20)));
		traceable.setMessageCreationTime(LocalDateTime.now());

		// - execute ------------------------
		context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
		
		// - verify -------------------------
		KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue = context.getResponseTopic().readKeyValue();
		assertNotNull(keyvalue);
		assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
		assertNotNull(keyvalue.value);
		assertNotNull(keyvalue.value.getRequest());
		
		verifyTraceData(traceable, keyvalue);
		assertTrue(keyvalue.value.getErrorMessage().contains("No match found for reservation"));
		assertEquals(ResponseMessage.CONFLICT, keyvalue.value.getStatus());

		CancelReservationRequest rRequest = keyvalue.value.getRequest().getCancelReservationRequest();
		assertEquals( cancelRequest.getAccountNumber(), rRequest.getAccountNumber());
		assertEquals( cancelRequest.getJsonMetaData(), rRequest.getJsonMetaData());
		assertEquals( cancelRequest.getRequestUuid(), rRequest.getRequestUuid());
		assertEquals( cancelRequest.getReservationUuid(), rRequest.getReservationUuid());

		PostingResponse response = keyvalue.value.getResponse();
		assertNotNull(response);
		assertNotNull(response.getTransactions());
		assertEquals(0, response.getTransactions().size());
		
		// Get Balance State Store
		BalanceLog balanceLog = context.getBalanceStore().get(account.getAccountNumber()).value();
		assertNotNull( balanceLog );
		assertEquals( expectedBalance, balanceLog.getBalance() );
		assertEquals( starting_UUID, balanceLog.getLastTransaction() );
	}
	private void verifyLoggedTraceData(TraceableMessage<?> traceable,
			TraceableMessage<LoggedTransaction> tresult, 
			LocalDateTime completionTime) {
		assertEquals(traceable.getBusinessTaxonomyId(), tresult.getBusinessTaxonomyId());
		assertEquals(traceable.getCorrelationId(), tresult.getCorrelationId());
		assertEquals(traceable.getMessageCreationTime(), tresult.getMessageCreationTime());
		assertEquals(traceable.getProducerAit(), tresult.getProducerAit());
		if ( completionTime != null)
			assertEquals(completionTime, tresult.getMessageCompletionTime());
		else 
			assertNotNull(tresult.getMessageCompletionTime() );
	}

	private void verifyTransactions(LoggedTransaction reservation, LoggedTransaction ttran) {
		assertEquals( reservation.getAccountNumber(), ttran.getAccountNumber());
		assertEquals( reservation.getDebitCardNumber(), ttran.getDebitCardNumber());
		assertEquals( reservation.getRequestUuid(), ttran.getRequestUuid());
		assertEquals( reservation.getReservationUuid(), ttran.getReservationUuid());
		assertEquals( reservation.getRunningBalanceAmount(), ttran.getRunningBalanceAmount());
		assertEquals( reservation.getTransactionAmount(), ttran.getTransactionAmount());
		assertEquals( reservation.getTransactionMetaDataJson(), ttran.getTransactionMetaDataJson());
		assertEquals( reservation.getTransactionTime(), ttran.getTransactionTime());
		assertEquals( reservation.getTransactionTypeCode(), ttran.getTransactionTypeCode());
		assertEquals( reservation.getTransactionUuid(), ttran.getTransactionUuid());
	}

	private void verifyTraceData(TraceableMessage<WorkflowMessage> traceable,
			KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue) {
		assertEquals(traceable.getBusinessTaxonomyId(), keyvalue.value.getBusinessTaxonomyId());
		assertEquals(traceable.getCorrelationId(), keyvalue.value.getCorrelationId());
		assertEquals(traceable.getMessageCreationTime(), keyvalue.value.getMessageCreationTime());
		assertEquals(traceable.getProducerAit(), keyvalue.value.getProducerAit());
		assertNotNull(keyvalue.value.getMessageCompletionTime());
	}
    
    private UUID resetBalance(String accountNumber, long balance) {
		BalanceLog log = new BalanceLog();
		log.setAccountNumber(accountNumber);
		log.setLastTransaction(UUID.randomUUID());
		log.setBalance(balance);
		context.getBalanceStore().put(accountNumber, ValueAndTimestamp.make(log, System.currentTimeMillis()));
		return log.getLastTransaction();
	}
	
	Account randomAccount(boolean valid, boolean protectAgainstOverdraft) {
		Account account = new Account();
		account.setAccountLifeCycleStatus(valid ? "EF" : "CL");
		account.setAccountNumber(Random.randomDigits(12));
		account.setProtectAgainstOverdraft(protectAgainstOverdraft);
		return account;
	}

	private void drain(TestOutputTopic<?, ?> topic) {
		while ( topic.getQueueSize() > 0) {
			topic.readKeyValue();
		}
	}
    

}
