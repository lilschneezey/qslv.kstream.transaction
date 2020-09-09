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
import qslv.kstream.LoggedTransaction;
import qslv.kstream.PostingResponse;
import qslv.kstream.PostingRequest;
import qslv.kstream.TransferRequest;
import qslv.kstream.workflow.TransferWorkflow;
import qslv.kstream.workflow.WorkflowMessage;
import qslv.util.Random;

@ExtendWith(MockitoExtension.class)
class Unit_Transfer {

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
	void test_transfer_success() {
    	drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());

		long startingFromBalance = 9999L;
		long startingToBalance = 1111L;
		long transactionAmount = 4444L;
		long expectedFromBalance = startingFromBalance - transactionAmount;
		long expectedToBalance = startingToBalance + transactionAmount;

		// - setup --------------------
		Account fromAccount = randomAccount(true, true);
		Account toAccount = randomAccount(true, true);
		resetBalance(fromAccount.getAccountNumber(), startingFromBalance);
		resetBalance(toAccount.getAccountNumber(), startingToBalance);
		
		// - prepare --------------------
		TransferRequest transferRequest = new TransferRequest();
		transferRequest.setRequestUuid(UUID.randomUUID());
		transferRequest.setTransferFromAccountNumber(fromAccount.getAccountNumber());
		transferRequest.setTransferToAccountNumber(toAccount.getAccountNumber());
		transferRequest.setTransferAmount(transactionAmount);
		transferRequest.setJsonMetaData(JSON_DATA);

		TransferWorkflow workflow = new TransferWorkflow(TransferWorkflow.START_TRANSFER_FUNDS, transferRequest);
		workflow.setTransferFromAccount(fromAccount);
		workflow.setTransferToAccount(toAccount);
		workflow.setProcessingAccountNumber(fromAccount.getAccountNumber());
		
		TraceableMessage<WorkflowMessage> traceable = new TraceableMessage<>();
		traceable.setProducerAit(AIT);
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId(CORRELATION_ID);
		traceable.setPayload(new WorkflowMessage(workflow, Random.randomString(20)));
		traceable.setMessageCreationTime(LocalDateTime.now());
		
		// - execute ------------------------
		context.getRequestTopic().pipeInput(fromAccount.getAccountNumber(), traceable);
		
		// - verify -------------------------
		KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue = context.getResponseTopic().readKeyValue();
		assertNotNull(keyvalue);
		assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
		assertNotNull(keyvalue.value);
		assertNotNull(keyvalue.value.getRequest());
		
		verifyTraceData(traceable, keyvalue);
		assertNull(keyvalue.value.getErrorMessage());
		assertEquals(ResponseMessage.SUCCESS, keyvalue.value.getStatus());

		TransferRequest rrequest = keyvalue.value.getRequest().getTransferRequest();
		assertEquals(transferRequest.getJsonMetaData(), rrequest.getJsonMetaData() );
		assertEquals(transferRequest.getRequestUuid(), rrequest.getRequestUuid() );
		assertEquals(transferRequest.getTransferAmount(), rrequest.getTransferAmount() );
		assertEquals(transferRequest.getTransferFromAccountNumber(), rrequest.getTransferFromAccountNumber() );
		assertEquals(transferRequest.getTransferToAccountNumber(), rrequest.getTransferToAccountNumber() );
		
		PostingResponse response = keyvalue.value.getResponse();
		assertNotNull(response);
		assertNotNull(response.getTransactions());
		assertEquals(2, response.getTransactions().size());
		
		LoggedTransaction transferFrom = response.getTransactions().get(0);
		assertEquals( transferRequest.getTransferFromAccountNumber(), transferFrom.getAccountNumber());
		assertNull( transferFrom.getDebitCardNumber());
		assertEquals( transferRequest.getRequestUuid(), transferFrom.getRequestUuid());
		assertNull( transferFrom.getReservationUuid());
		assertEquals( expectedFromBalance, transferFrom.getRunningBalanceAmount());
		assertEquals( (0L-transactionAmount), transferFrom.getTransactionAmount());
		assertEquals( JSON_DATA, transferFrom.getTransactionMetaDataJson());
		assertNotNull( transferFrom.getTransactionTime());
		assertEquals( LoggedTransaction.TRANSFER_FROM, transferFrom.getTransactionTypeCode());
		assertNotNull( transferFrom.getTransactionUuid());

		LoggedTransaction transferTo = response.getTransactions().get(1);
		assertEquals( transferRequest.getTransferToAccountNumber(), transferTo.getAccountNumber());
		assertNull( transferTo.getDebitCardNumber());
		assertEquals( transferRequest.getRequestUuid(), transferTo.getRequestUuid());
		assertNull( transferTo.getReservationUuid());
		assertEquals( expectedToBalance, transferTo.getRunningBalanceAmount());
		assertEquals( transactionAmount, transferTo.getTransactionAmount());
		assertEquals( JSON_DATA, transferTo.getTransactionMetaDataJson());
		assertNotNull( transferTo.getTransactionTime());
		assertEquals( LoggedTransaction.TRANSFER_TO, transferTo.getTransactionTypeCode());
		assertNotNull( transferTo.getTransactionUuid());

		// Transaction in the transaction log
		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue);
		assertEquals(transferRequest.getTransferFromAccountNumber(), logKeyValue.key);
		verifyLoggedTraceData(traceable, logKeyValue.value, null);
		verifyTransactions(transferFrom, logKeyValue.value.getPayload());
		
		// Transaction in the transaction log
		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue2 = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue2);
		assertEquals(transferRequest.getTransferToAccountNumber(), logKeyValue2.key);
		verifyLoggedTraceData(traceable, logKeyValue2.value, keyvalue.value.getMessageCompletionTime());
		verifyTransactions(transferTo, logKeyValue2.value.getPayload());

		// Balance State Store
		BalanceLog balanceLog = context.getBalanceStore().get(transferRequest.getTransferFromAccountNumber()).value();
		assertNotNull( balanceLog );
		assertEquals( expectedFromBalance, balanceLog.getBalance() );
		assertEquals( transferFrom.getTransactionUuid(), balanceLog.getLastTransaction() );

		// Balance State Store
		BalanceLog balanceLog2 = context.getBalanceStore().get(transferRequest.getTransferToAccountNumber()).value();
		assertNotNull( balanceLog2 );
		assertEquals( expectedToBalance, balanceLog2.getBalance() );
		assertEquals( transferTo.getTransactionUuid(), balanceLog2.getLastTransaction() );
	}

    @Test
	void test_transfer_NSF() {
    	drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());

		long startingFromBalance = 2222L;
		long startingToBalance = 1111L;
		long transactionAmount = 4444L;
		long expectedFromBalance = startingFromBalance ;
		long expectedToBalance = startingToBalance;

		// - setup --------------------
		Account fromAccount = randomAccount(true, true);
		Account toAccount = randomAccount(true, true);
		resetBalance(fromAccount.getAccountNumber(), startingFromBalance);
		UUID toUUID = resetBalance(toAccount.getAccountNumber(), startingToBalance);
		
		// - prepare --------------------
		TransferRequest transferRequest = new TransferRequest();
		transferRequest.setRequestUuid(UUID.randomUUID());
		transferRequest.setTransferFromAccountNumber(fromAccount.getAccountNumber());
		transferRequest.setTransferToAccountNumber(toAccount.getAccountNumber());
		transferRequest.setTransferAmount(transactionAmount);
		transferRequest.setJsonMetaData(JSON_DATA);

		TransferWorkflow workflow = new TransferWorkflow(TransferWorkflow.START_TRANSFER_FUNDS, transferRequest);
		workflow.setTransferFromAccount(fromAccount);
		workflow.setTransferToAccount(toAccount);
		workflow.setProcessingAccountNumber(fromAccount.getAccountNumber());
		
		TraceableMessage<WorkflowMessage> traceable = new TraceableMessage<>();
		traceable.setProducerAit(AIT);
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId(CORRELATION_ID);
		traceable.setPayload(new WorkflowMessage(workflow, Random.randomString(20)));
		traceable.setMessageCreationTime(LocalDateTime.now());
		
		// - execute ------------------------
		context.getRequestTopic().pipeInput(fromAccount.getAccountNumber(), traceable);
		
		// - verify -------------------------
		KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue = context.getResponseTopic().readKeyValue();
		assertNotNull(keyvalue);
		assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
		assertNotNull(keyvalue.value);
		assertNotNull(keyvalue.value.getRequest());
		
		verifyTraceData(traceable, keyvalue);
		assertTrue(keyvalue.value.getErrorMessage().contains("Insufficient funds"));
		assertEquals(ResponseMessage.INSUFFICIENT_FUNDS, keyvalue.value.getStatus());

		TransferRequest rrequest = keyvalue.value.getRequest().getTransferRequest();
		assertEquals(transferRequest.getJsonMetaData(), rrequest.getJsonMetaData() );
		assertEquals(transferRequest.getRequestUuid(), rrequest.getRequestUuid() );
		assertEquals(transferRequest.getTransferAmount(), rrequest.getTransferAmount() );
		assertEquals(transferRequest.getTransferFromAccountNumber(), rrequest.getTransferFromAccountNumber() );
		assertEquals(transferRequest.getTransferToAccountNumber(), rrequest.getTransferToAccountNumber() );
		
		PostingResponse response = keyvalue.value.getResponse();
		assertNotNull(response);
		assertNotNull(response.getTransactions());
		assertEquals(1, response.getTransactions().size());
		
		LoggedTransaction transferFrom = response.getTransactions().get(0);
		assertEquals( transferRequest.getTransferFromAccountNumber(), transferFrom.getAccountNumber());
		assertNull( transferFrom.getDebitCardNumber());
		assertEquals( transferRequest.getRequestUuid(), transferFrom.getRequestUuid());
		assertNull( transferFrom.getReservationUuid());
		assertEquals( expectedFromBalance, transferFrom.getRunningBalanceAmount());
		assertEquals( (0L-transactionAmount), transferFrom.getTransactionAmount());
		assertEquals( JSON_DATA, transferFrom.getTransactionMetaDataJson());
		assertNotNull( transferFrom.getTransactionTime());
		assertEquals( LoggedTransaction.REJECTED_TRANSACTION, transferFrom.getTransactionTypeCode());
		assertNotNull( transferFrom.getTransactionUuid());

		// Transaction in the transaction log
		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue);
		assertEquals(transferRequest.getTransferFromAccountNumber(), logKeyValue.key);
		verifyLoggedTraceData(traceable, logKeyValue.value, null);
		verifyTransactions(transferFrom, logKeyValue.value.getPayload());
		
		// Balance State Store
		BalanceLog balanceLog = context.getBalanceStore().get(transferRequest.getTransferFromAccountNumber()).value();
		assertNotNull( balanceLog );
		assertEquals( expectedFromBalance, balanceLog.getBalance() );
		assertEquals( transferFrom.getTransactionUuid(), balanceLog.getLastTransaction() );

		// Balance State Store
		BalanceLog balanceLog2 = context.getBalanceStore().get(transferRequest.getTransferToAccountNumber()).value();
		assertNotNull( balanceLog2 );
		assertEquals( expectedToBalance, balanceLog2.getBalance() );
		assertEquals( toUUID, balanceLog2.getLastTransaction() );
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
