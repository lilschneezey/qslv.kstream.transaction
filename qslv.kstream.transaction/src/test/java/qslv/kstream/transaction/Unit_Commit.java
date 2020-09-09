package qslv.kstream.transaction;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
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
import qslv.data.OverdraftInstruction;
import qslv.kstream.CommitReservationRequest;
import qslv.kstream.LoggedTransaction;
import qslv.kstream.PostingResponse;
import qslv.kstream.PostingRequest;
import qslv.kstream.workflow.CommitReservationWorkflow;
import qslv.kstream.workflow.WorkflowMessage;
import qslv.util.Random;

@ExtendWith(MockitoExtension.class)
class Unit_Commit {

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
    void test_commit_additionalRemaining_success() {
    	test_commit_amount(9999L, -3333L, -5555L, true, true);
    }
   
    @Test
    void test_commit_returnFunds_success() {
    	test_commit_amount(9999L, -3333L, -1234L, true, true);
    }

    @Test
    void test_commit_noneRemaining_success() {
    	test_commit_amount(9999L, -3333L, -3333L, true, true);
    }

    @Test
    void test_commit_additionalRemaining_ODdisallowedOnRequest_success() {
    	test_commit_amount(9999L, -3333L, -3333L, false, true);
    }
    
    @Test
    void test_commit_additionalRemaining_ODdisallowedOnAccount_success() {
    	test_commit_amount(9999L, -3333L, -3333L, true, false);
    }

    @Test
    void test_commit_additionalRemaining_ODdisallowedOnBoth_success() {
    	test_commit_amount(9999L, -3333L, -3333L, true, false);
    }

    void test_commit_amount(long startingBalance, long originalAmount, long transactionAmount, boolean requestAllowOD, boolean accountAllowOD) {
    	drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());
    	
    	long remainingAmount = transactionAmount - originalAmount;
    	long expectedBalance = startingBalance + remainingAmount;
		
		TraceableMessage<WorkflowMessage> traceable = setupBasicInput(startingBalance, originalAmount, transactionAmount, 0);
		Account account = traceable.getPayload().getCommitReservationWorkflow().getAccount();
		CommitReservationRequest request = traceable.getPayload().getCommitReservationWorkflow().getRequest();
		LoggedTransaction reservation = traceable.getPayload().getCommitReservationWorkflow().getReservation();
		request.setProtectAgainstOverdraft(requestAllowOD);
		account.setProtectAgainstOverdraft(accountAllowOD);
		
		// - execute ------------------------
		context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
		
		// - verify -------------------------
		KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue = context.getResponseTopic().readKeyValue();
		assertNotNull(keyvalue);
		assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
		assertNotNull(keyvalue.value);
		assertNotNull(keyvalue.value.getRequest());
		assertNotNull(keyvalue.value.getResponse());
		
		verifyTraceData(traceable, keyvalue);
		assertNull(keyvalue.value.getErrorMessage());
		assertEquals(ResponseMessage.SUCCESS, keyvalue.value.getStatus());

		verifyRequestData(request, keyvalue);

		PostingResponse response = keyvalue.value.getResponse();
		assertNotNull(response.getTransactions());
		assertEquals(1, response.getTransactions().size());
		
		LoggedTransaction commit = response.getTransactions().get(0);
		assertEquals( request.getAccountNumber(), commit.getAccountNumber());
		assertNull( request.getDebitCardNumber(), commit.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), commit.getRequestUuid());
		assertEquals( reservation.getTransactionUuid(), commit.getReservationUuid());
		assertEquals( expectedBalance, commit.getRunningBalanceAmount());
		assertEquals( remainingAmount, commit.getTransactionAmount());
		assertEquals( request.getJsonMetaData(), commit.getTransactionMetaDataJson());
		assertNotNull( commit.getTransactionTime());
		assertEquals( LoggedTransaction.RESERVATION_COMMIT, commit.getTransactionTypeCode());
		assertNotNull( commit.getTransactionUuid());

		// get transaction from transaction log topic
		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue);
		assertEquals(account.getAccountNumber(), logKeyValue.key);
		verifyLoggedTraceData(traceable, logKeyValue.value, keyvalue.value.getMessageCompletionTime());
		verifyTransactions(commit, logKeyValue.value.getPayload());
		
		// -- Verify Balance Log State Store --
		BalanceLog balanceLog = context.getBalanceStore().get(account.getAccountNumber()).value();
		assertNotNull( balanceLog );
		assertEquals( expectedBalance, balanceLog.getBalance() );
		assertEquals(commit.getTransactionUuid(), balanceLog.getLastTransaction() );
	}

    @Test
    void test_commit_reservationNotFound_null() {
    	test_commit_reservationNotFound(9999L, -3333L, -3333L, null);
    }

    @Test
    void test_commit_reservationNotFound_different() {
    	test_commit_reservationNotFound(9999L, -3333L, -3333L, UUID.randomUUID());
    }

    void test_commit_reservationNotFound(long startingBalance, long originalAmount, long transactionAmount, UUID reservationUuid) {
    	drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());
    	
		TraceableMessage<WorkflowMessage> traceable = setupBasicInput(startingBalance, originalAmount, transactionAmount, 0);
		Account account = traceable.getPayload().getCommitReservationWorkflow().getAccount();
		CommitReservationRequest request = traceable.getPayload().getCommitReservationWorkflow().getRequest();
		request.setReservationUuid(reservationUuid);
		UUID oldTransaction = resetBalance(account.getAccountNumber(), startingBalance);
		
		// - execute ------------------------
		context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
		
		// - verify -------------------------
		KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue = context.getResponseTopic().readKeyValue();
		assertNotNull(keyvalue);
		assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
		assertNotNull(keyvalue.value);
		assertNotNull(keyvalue.value.getRequest());
		assertNotNull(keyvalue.value.getResponse());
		
		verifyTraceData(traceable, keyvalue);
		assertTrue(keyvalue.value.getErrorMessage().contains("Reservation not found"));
		assertEquals(ResponseMessage.CONFLICT, keyvalue.value.getStatus());

		verifyRequestData(request, keyvalue);

		PostingResponse response = keyvalue.value.getResponse();
		assertNotNull(response.getTransactions());
		assertEquals(0, response.getTransactions().size());
				
		// -- Verify Balance Log State Store --
		BalanceLog balanceLog = context.getBalanceStore().get(account.getAccountNumber()).value();
		assertNotNull( balanceLog );
		assertEquals( startingBalance, balanceLog.getBalance() );
		assertEquals( oldTransaction, balanceLog.getLastTransaction() );
	}
    
    @Test
    void test_commit_NoMatch() {
    	drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());
    	
    	long startingBalance = 99999L;
    	long originalAmount = -999L;
    	long transactionAmount = -1111L;
    	
		TraceableMessage<WorkflowMessage> traceable = setupBasicInput(99999L, originalAmount, transactionAmount, 0);
		Account account = traceable.getPayload().getCommitReservationWorkflow().getAccount();
		CommitReservationRequest request = traceable.getPayload().getCommitReservationWorkflow().getRequest();
		request.setReservationUuid(null);
		UUID oldTransaction = resetBalance(account.getAccountNumber(), startingBalance);
		traceable.getPayload().getCommitReservationWorkflow().setState(CommitReservationWorkflow.NO_MATCH);
		
		// - execute ------------------------
		context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
		
		// - verify -------------------------
		KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue = context.getResponseTopic().readKeyValue();
		assertNotNull(keyvalue);
		assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
		assertNotNull(keyvalue.value);
		assertNotNull(keyvalue.value.getRequest());
		assertNotNull(keyvalue.value.getResponse());
		
		verifyTraceData(traceable, keyvalue);
		assertTrue(keyvalue.value.getErrorMessage().contains("No match found for reservation"));
		assertEquals(ResponseMessage.CONFLICT, keyvalue.value.getStatus());

		verifyRequestData(request, keyvalue);

		PostingResponse response = keyvalue.value.getResponse();
		assertNotNull(response.getTransactions());
		assertEquals(0, response.getTransactions().size());
				
		// -- Verify Balance Log State Store --
		BalanceLog balanceLog = context.getBalanceStore().get(account.getAccountNumber()).value();
		assertNotNull( balanceLog );
		assertEquals( startingBalance, balanceLog.getBalance() );
		assertEquals( oldTransaction, balanceLog.getLastTransaction() );
	}
    @Test
    void test_commit_OD_success() {
  
    	drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());
    	
      	long startingBalance=4L;
    	long originalAmount= -3333L;
    	long transactionAmount= -4444L;
    	long remainingAmount = transactionAmount - originalAmount;
    	long expectedBalance = startingBalance;
    	long expectedIntermediaryBalance = startingBalance + Math.abs(remainingAmount);
    	
    	long startingODBalance = 9999L;
    	long expectedODBalance = startingODBalance - Math.abs(remainingAmount);
		
		TraceableMessage<WorkflowMessage> traceable = setupBasicInput(startingBalance, originalAmount, transactionAmount, 1);
		Account account = traceable.getPayload().getCommitReservationWorkflow().getAccount();
		CommitReservationRequest request = traceable.getPayload().getCommitReservationWorkflow().getRequest();
		LoggedTransaction reservation = traceable.getPayload().getCommitReservationWorkflow().getReservation();
		OverdraftInstruction instruction = traceable.getPayload().getCommitReservationWorkflow().getUnprocessedInstructions().get(0);
		request.setProtectAgainstOverdraft(true);
		account.setProtectAgainstOverdraft(true);
		resetBalance(instruction.getOverdraftAccount().getAccountNumber(), startingODBalance);
		
		// - execute ------------------------
		context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
		
		// - verify -------------------------
		KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue = context.getResponseTopic().readKeyValue();
		assertNotNull(keyvalue);
		assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
		assertNotNull(keyvalue.value);
		assertNotNull(keyvalue.value.getRequest());
		assertNotNull(keyvalue.value.getResponse());
		
		verifyTraceData(traceable, keyvalue);
		assertNull(keyvalue.value.getErrorMessage());
		assertEquals(ResponseMessage.SUCCESS, keyvalue.value.getStatus());

		verifyRequestData(request, keyvalue);

		PostingResponse response = keyvalue.value.getResponse();
		assertNotNull(response.getTransactions());
		assertEquals(4, response.getTransactions().size());
		
		LoggedTransaction rejection = response.getTransactions().get(0);
		assertEquals( request.getAccountNumber(), rejection.getAccountNumber());
		assertEquals( request.getDebitCardNumber(), rejection.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), rejection.getRequestUuid());
		assertEquals( reservation.getTransactionUuid(), rejection.getReservationUuid());
		assertEquals( startingBalance, rejection.getRunningBalanceAmount());
		assertEquals( remainingAmount, rejection.getTransactionAmount());
		assertEquals( request.getJsonMetaData(), rejection.getTransactionMetaDataJson());
		assertNotNull( rejection.getTransactionTime());
		assertEquals( LoggedTransaction.REJECTED_TRANSACTION, rejection.getTransactionTypeCode());
		assertNotNull( rejection.getTransactionUuid());

		LoggedTransaction transferFrom = response.getTransactions().get(1);
		assertEquals( instruction.getOverdraftAccount().getAccountNumber(), transferFrom.getAccountNumber());
		assertNull( transferFrom.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), transferFrom.getRequestUuid());
		assertNull( transferFrom.getReservationUuid());
		assertEquals( expectedODBalance, transferFrom.getRunningBalanceAmount());
		assertEquals( 0L-Math.abs(remainingAmount), transferFrom.getTransactionAmount());
		assertEquals( request.getJsonMetaData(), transferFrom.getTransactionMetaDataJson());
		assertNotNull( transferFrom.getTransactionTime());
		assertEquals( LoggedTransaction.TRANSFER_FROM, transferFrom.getTransactionTypeCode());
		assertNotNull( transferFrom.getTransactionUuid());

		LoggedTransaction transferTo = response.getTransactions().get(2);
		assertEquals( request.getAccountNumber(), transferTo.getAccountNumber());
		assertNull( transferTo.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), transferTo.getRequestUuid());
		assertNull( transferTo.getReservationUuid());
		assertEquals( expectedIntermediaryBalance, transferTo.getRunningBalanceAmount());
		assertEquals( Math.abs(remainingAmount), transferTo.getTransactionAmount());
		assertEquals( request.getJsonMetaData(), transferTo.getTransactionMetaDataJson());
		assertNotNull( transferTo.getTransactionTime());
		assertEquals( LoggedTransaction.TRANSFER_TO, transferTo.getTransactionTypeCode());
		assertNotNull( transferTo.getTransactionUuid());

		LoggedTransaction transaction = response.getTransactions().get(3);
		assertEquals( request.getAccountNumber(), transaction.getAccountNumber());
		assertEquals( request.getDebitCardNumber(), transaction.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), transaction.getRequestUuid());
		assertEquals( reservation.getTransactionUuid(), transaction.getReservationUuid());
		assertEquals( expectedBalance, transaction.getRunningBalanceAmount());
		assertEquals( remainingAmount, transaction.getTransactionAmount());
		assertEquals( request.getJsonMetaData(), transaction.getTransactionMetaDataJson());
		assertNotNull( transaction.getTransactionTime());
		assertEquals( LoggedTransaction.RESERVATION_COMMIT, transaction.getTransactionTypeCode());
		assertNotNull( transaction.getTransactionUuid());

		// get transaction from topic
		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue);
		assertEquals(account.getAccountNumber(), logKeyValue.key);
		verifyLoggedTraceData(traceable, logKeyValue.value, null);
		verifyTransactions(rejection, logKeyValue.value.getPayload());
		
		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue2 = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue2);
		assertEquals(instruction.getOverdraftAccount().getAccountNumber(), logKeyValue2.key);
		verifyLoggedTraceData(traceable, logKeyValue2.value, null);
		verifyTransactions(transferFrom, logKeyValue2.value.getPayload());

		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue3 = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue3);
		assertEquals(account.getAccountNumber(), logKeyValue3.key);
		verifyLoggedTraceData(traceable, logKeyValue3.value, null);
		verifyTransactions(transferTo, logKeyValue3.value.getPayload());

		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue4 = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue4);
		assertEquals(account.getAccountNumber(), logKeyValue4.key);
		verifyLoggedTraceData(traceable, logKeyValue4.value, keyvalue.value.getMessageCompletionTime());
		verifyTransactions(transaction, logKeyValue4.value.getPayload());

		// get State Store
		BalanceLog balanceLog = context.getBalanceStore().get(account.getAccountNumber()).value();
		assertNotNull( balanceLog );
		assertEquals( expectedBalance, balanceLog.getBalance() );
		assertEquals( transaction.getTransactionUuid(), balanceLog.getLastTransaction() );

		BalanceLog balanceLog2 = context.getBalanceStore().get(instruction.getOverdraftAccount().getAccountNumber()).value();
		assertNotNull( balanceLog2 );
		assertEquals( expectedODBalance, balanceLog2.getBalance() );
		assertEquals( transferFrom.getTransactionUuid(), balanceLog2.getLastTransaction() );
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

	private void verifyRequestData(CommitReservationRequest request,
			KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue) {
		CommitReservationRequest rRequest = keyvalue.value.getRequest().getCommitReservationRequest();
		assertEquals( request.getAccountNumber(), rRequest.getAccountNumber());
		assertEquals( request.getDebitCardNumber(), rRequest.getDebitCardNumber());
		assertEquals( request.getJsonMetaData(), rRequest.getJsonMetaData());
		assertEquals( request.getRequestUuid(), rRequest.getRequestUuid());
		assertEquals( request.getTransactionAmount(), rRequest.getTransactionAmount());
		assertEquals( request.getReservationUuid(), rRequest.getReservationUuid());
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
	OverdraftInstruction randomOverdraft(String accountNumber, boolean valid, boolean accountValid) {
		OverdraftInstruction od = new OverdraftInstruction();
		od.setAccountNumber(accountNumber);
		od.setEffectiveStart(LocalDateTime.now().minusMonths(12));
		od.setEffectiveEnd(LocalDateTime.now().plusMonths(12));
		od.setInstructionLifecycleStatus(valid ? "EF" : "CL");
		od.setOverdraftAccount(randomAccount(accountValid, true));
		return od;
	}
	
	public TraceableMessage<WorkflowMessage> setupBasicInput(long startingBalance, long originalAmount, long transactionAmount, int overdrafts) {
		// - setup --------------------
		Account account = randomAccount(true, true);
		ArrayList<OverdraftInstruction> instructions = new ArrayList<>();
		for (int ii=0; ii< overdrafts; ii++) {
			OverdraftInstruction overdraft = randomOverdraft(account.getAccountNumber(), true, true);
			instructions.add(overdraft);			
		}
		
		resetBalance(account.getAccountNumber(), startingBalance);
		
		// - prepare --------------------
		CommitReservationRequest commitRequest = new CommitReservationRequest();
		commitRequest.setRequestUuid(UUID.randomUUID());
		commitRequest.setAccountNumber(account.getAccountNumber());
		commitRequest.setDebitCardNumber(null);
		commitRequest.setTransactionAmount(transactionAmount);
		commitRequest.setJsonMetaData(JSON_DATA);
		commitRequest.setReservationUuid(UUID.randomUUID());

		LoggedTransaction reservation = new LoggedTransaction();
		reservation.setAccountNumber(account.getAccountNumber());
		reservation.setDebitCardNumber(Random.randomDigits(12));
		reservation.setRequestUuid(UUID.randomUUID());
		reservation.setReservationUuid(null);
		reservation.setRunningBalanceAmount(34L);
		reservation.setTransactionAmount(originalAmount);
		reservation.setTransactionMetaDataJson(JSON_DATA);
		reservation.setTransactionTime(LocalDateTime.now());
		reservation.setTransactionTypeCode(LoggedTransaction.RESERVATION);
		reservation.setTransactionUuid(commitRequest.getReservationUuid());
		
		CommitReservationWorkflow workflow = new CommitReservationWorkflow(CommitReservationWorkflow.COMMIT_START, commitRequest);
		workflow.setAccount(account);
		workflow.setProcessingAccountNumber(account.getAccountNumber());
		workflow.setUnprocessedInstructions(overdrafts != 0 ? instructions : null);
		workflow.setReservation(reservation);
		
		TraceableMessage<WorkflowMessage> traceable = new TraceableMessage<>();
		traceable.setProducerAit(AIT);
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId(CORRELATION_ID);
		traceable.setPayload(new WorkflowMessage(workflow, Random.randomString(20)));
		traceable.setMessageCreationTime(LocalDateTime.now());

		return traceable;
	}
	
	private void drain(TestOutputTopic<?, ?> topic) {
		while ( topic.getQueueSize() > 0) {
			topic.readKeyValue();
		}
	}
    

}
