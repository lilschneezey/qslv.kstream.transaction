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
import qslv.kstream.LoggedTransaction;
import qslv.kstream.PostingResponse;
import qslv.kstream.PostingRequest;
import qslv.kstream.ReservationRequest;
import qslv.kstream.workflow.ReservationWorkflow;
import qslv.kstream.workflow.WorkflowMessage;
import qslv.util.Random;

@ExtendWith(MockitoExtension.class)
class Unit_Reservation {

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
	void test_reservation_success() {
    	drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());
    	
		long startingBalance = 9999L;
		long transactionAmount = -4444L;
		long expectedBalance = startingBalance + transactionAmount;
		
		TraceableMessage<WorkflowMessage> traceable = setupBasicInput(startingBalance, transactionAmount,0);
		Account account = traceable.getPayload().getReservationWorkflow().getAccount();
		ReservationRequest request = traceable.getPayload().getReservationWorkflow().getRequest();
		
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

		verifyRequestData(request, keyvalue);

		PostingResponse response = keyvalue.value.getResponse();
		assertNotNull(response);
		assertNotNull(response.getTransactions());
		assertEquals(1, response.getTransactions().size());
		
		LoggedTransaction transaction = response.getTransactions().get(0);
		assertEquals( request.getAccountNumber(), transaction.getAccountNumber());
		assertNull( transaction.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), transaction.getRequestUuid());
		assertNull( transaction.getReservationUuid());
		assertEquals( expectedBalance, transaction.getRunningBalanceAmount());
		assertEquals( transactionAmount, transaction.getTransactionAmount());
		assertEquals( JSON_DATA, transaction.getTransactionMetaDataJson());
		assertNotNull( transaction.getTransactionTime());
		assertEquals( LoggedTransaction.RESERVATION, transaction.getTransactionTypeCode());
		assertNotNull( transaction.getTransactionUuid());

		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue);
		assertEquals(account.getAccountNumber(), logKeyValue.key);
		
		TraceableMessage<LoggedTransaction> tresult = logKeyValue.value;
		verifyLoggedTraceData(traceable, tresult, keyvalue.value.getMessageCompletionTime());
		
		LoggedTransaction ttran = tresult.getPayload();
		assertEquals( transaction.getAccountNumber(), ttran.getAccountNumber());
		assertEquals( transaction.getDebitCardNumber(), ttran.getDebitCardNumber());
		assertEquals( transaction.getRequestUuid(), ttran.getRequestUuid());
		assertEquals( transaction.getReservationUuid(), ttran.getReservationUuid());
		assertEquals( transaction.getRunningBalanceAmount(), ttran.getRunningBalanceAmount());
		assertEquals( transaction.getTransactionAmount(), ttran.getTransactionAmount());
		assertEquals( transaction.getTransactionMetaDataJson(), ttran.getTransactionMetaDataJson());
		assertEquals( transaction.getTransactionTime(), ttran.getTransactionTime());
		assertEquals( LoggedTransaction.RESERVATION, ttran.getTransactionTypeCode());
		assertEquals( transaction.getTransactionUuid(), ttran.getTransactionUuid());

		BalanceLog balanceLog = context.getBalanceStore().get(account.getAccountNumber()).value();
		assertNotNull( balanceLog );
		assertEquals( expectedBalance, balanceLog.getBalance() );
		assertEquals(transaction.getTransactionUuid(), balanceLog.getLastTransaction() );
	}

    @Test
	void test_reservation_transactionDisallowOD_NSF() {
    	drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());

		long startingBalance = 3333L;
		long transactionAmount = -4444L;
		long expectedBalance = startingBalance;
		
		TraceableMessage<WorkflowMessage> traceable = setupBasicInput(startingBalance, transactionAmount,0);
		Account account = traceable.getPayload().getReservationWorkflow().getAccount();
		ReservationRequest request = traceable.getPayload().getReservationWorkflow().getRequest();
		
		request.setProtectAgainstOverdraft(false);
		
		// - execute ------------------------
		context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
		
		// - verify -------------------------
		KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue = context.getResponseTopic().readKeyValue();
		assertNotNull(keyvalue);
		assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
		assertNotNull(keyvalue.value);
		assertNotNull(keyvalue.value.getRequest());
		
		verifyTraceData(traceable, keyvalue);
		assertTrue(keyvalue.value.getErrorMessage().contains("false/true"));
		assertEquals(ResponseMessage.INSUFFICIENT_FUNDS, keyvalue.value.getStatus());

		verifyRequestData(request, keyvalue);

		PostingResponse response = keyvalue.value.getResponse();
		assertNotNull(response);
		assertNotNull(response.getTransactions());
		assertEquals(1, response.getTransactions().size());
		
		LoggedTransaction transaction = response.getTransactions().get(0);
		assertEquals( request.getAccountNumber(), transaction.getAccountNumber());
		assertNull( transaction.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), transaction.getRequestUuid());
		assertNull( transaction.getReservationUuid());
		assertEquals( expectedBalance, transaction.getRunningBalanceAmount());
		assertEquals( transactionAmount, transaction.getTransactionAmount());
		assertEquals( JSON_DATA, transaction.getTransactionMetaDataJson());
		assertNotNull( transaction.getTransactionTime());
		assertEquals( LoggedTransaction.REJECTED_TRANSACTION, transaction.getTransactionTypeCode());
		assertNotNull( transaction.getTransactionUuid());

		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue);
		assertEquals(account.getAccountNumber(), logKeyValue.key);
		
		TraceableMessage<LoggedTransaction> tresult = logKeyValue.value;
		verifyLoggedTraceData(traceable, tresult, keyvalue.value.getMessageCompletionTime());
		
		LoggedTransaction ttran = tresult.getPayload();
		assertEquals( transaction.getAccountNumber(), ttran.getAccountNumber());
		assertEquals( transaction.getDebitCardNumber(), ttran.getDebitCardNumber());
		assertEquals( transaction.getRequestUuid(), ttran.getRequestUuid());
		assertEquals( transaction.getReservationUuid(), ttran.getReservationUuid());
		assertEquals( transaction.getRunningBalanceAmount(), ttran.getRunningBalanceAmount());
		assertEquals( transaction.getTransactionAmount(), ttran.getTransactionAmount());
		assertEquals( transaction.getTransactionMetaDataJson(), ttran.getTransactionMetaDataJson());
		assertEquals( transaction.getTransactionTime(), ttran.getTransactionTime());
		assertEquals( LoggedTransaction.REJECTED_TRANSACTION, ttran.getTransactionTypeCode());
		assertEquals( transaction.getTransactionUuid(), ttran.getTransactionUuid());

		BalanceLog balanceLog = context.getBalanceStore().get(account.getAccountNumber()).value();
		assertNotNull( balanceLog );
		assertEquals( expectedBalance, balanceLog.getBalance() );
		assertEquals(transaction.getTransactionUuid(), balanceLog.getLastTransaction() );

	}
 
    @Test
	void test_reservation_accountDisallowOD_NSF() {
    	drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());

		long startingBalance = 3333L;
		long transactionAmount = -4444L;
		
		TraceableMessage<WorkflowMessage> traceable = setupBasicInput(startingBalance, transactionAmount,0);
		Account account = traceable.getPayload().getReservationWorkflow().getAccount();
		
		account.setProtectAgainstOverdraft(false);
		
		// - execute ------------------------
		context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
		
		// - verify -------------------------
		KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue = context.getResponseTopic().readKeyValue();
		assertNotNull(keyvalue);
		assertNotNull(keyvalue.value);
		assertEquals(ResponseMessage.INSUFFICIENT_FUNDS, keyvalue.value.getStatus());
		assertTrue(keyvalue.value.getErrorMessage().contains("true/false"));

	}
    
    @Test
	void test_reservation_noODrecords_NSF() {
    	drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());

    	long startingBalance = 3333L;
		long transactionAmount = -4444L;
		
		TraceableMessage<WorkflowMessage> traceable = setupBasicInput(startingBalance, transactionAmount,0);
		Account account = traceable.getPayload().getReservationWorkflow().getAccount();
		
		// - execute ------------------------
		context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
		
		// - verify -------------------------
		KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue = context.getResponseTopic().readKeyValue();
		assertNotNull(keyvalue);
		assertNotNull(keyvalue.value);
		assertEquals(ResponseMessage.INSUFFICIENT_FUNDS, keyvalue.value.getStatus());
		assertTrue(keyvalue.value.getErrorMessage().contains("true/true"));
	}
    
    @Test
	void test_reservation_oneAccountOverdraft_success() {
    	drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());

    	long startingBalance = 3333L;
		long odStartingBalance = 9999L;
		long transactionAmount = -4444L;
		long expectedBalance = startingBalance;
		long expectedODBalance = odStartingBalance + transactionAmount;
		
		TraceableMessage<WorkflowMessage> traceable = setupBasicInput(startingBalance, transactionAmount, 1);
		Account account = traceable.getPayload().getReservationWorkflow().getAccount();
		ReservationRequest request = traceable.getPayload().getReservationWorkflow().getRequest();

		OverdraftInstruction instruction = traceable.getPayload().getReservationWorkflow().getUnprocessedInstructions().get(0);
		resetBalance(instruction.getOverdraftAccount().getAccountNumber(), odStartingBalance);
		
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

		verifyRequestData(request, keyvalue);

		PostingResponse response = keyvalue.value.getResponse();
		assertNotNull(response);
		assertNotNull(response.getTransactions());
		assertEquals(2, response.getTransactions().size());
		
		// --- First Transaction in Response
		LoggedTransaction rejection = response.getTransactions().get(0);
		verifyReservation(request, rejection, LoggedTransaction.REJECTED_TRANSACTION, request.getAccountNumber());
		assertEquals( expectedBalance, rejection.getRunningBalanceAmount());
		assertEquals( transactionAmount, rejection.getTransactionAmount());

		// --- Second Transaction in Response
		LoggedTransaction reservation = response.getTransactions().get(1);
		verifyReservation(request, reservation, LoggedTransaction.RESERVATION, instruction.getOverdraftAccount().getAccountNumber());
		assertEquals( expectedODBalance, reservation.getRunningBalanceAmount());
		assertEquals( transactionAmount, reservation.getTransactionAmount());

		// --- First LoggedTransaction
		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue);
		assertEquals(account.getAccountNumber(), logKeyValue.key);
		verifyLoggedTraceData(traceable, logKeyValue.value, null);
		verifyTransactions(rejection, logKeyValue.value.getPayload());

		//-- Second Logged Transaction
		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue2 = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue2);
		assertEquals(instruction.getOverdraftAccount().getAccountNumber(), logKeyValue2.key);
		verifyLoggedTraceData(traceable, logKeyValue2.value, keyvalue.value.getMessageCompletionTime());
		verifyTransactions(reservation, logKeyValue2.value.getPayload());

		// ---First Account Balance
		BalanceLog balanceLog = context.getBalanceStore().get(account.getAccountNumber()).value();
		assertNotNull( balanceLog );
		assertEquals( expectedBalance, balanceLog.getBalance() );
		assertEquals( rejection.getTransactionUuid(), balanceLog.getLastTransaction() );
		
		// ---Second Account Balance
		BalanceLog balanceLog2 = context.getBalanceStore().get(instruction.getOverdraftAccount().getAccountNumber()).value();
		assertNotNull( balanceLog2 );
		assertEquals( expectedODBalance, balanceLog2.getBalance() );
		assertEquals( reservation.getTransactionUuid(), balanceLog2.getLastTransaction() );
	}
    
    @Test
	void test_reservation_twoAccountOverdraft_success() {
    	drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());

    	long startingBalance = 3333L;
		long odStartingBalance = 3333L;
		long od2StartingBalance = 9999L;
		long transactionAmount = -4444L;
		long expectedBalance = startingBalance;
		long expectedODBalance = odStartingBalance;
		long expectedOD2Balance = od2StartingBalance + transactionAmount;
		
		TraceableMessage<WorkflowMessage> traceable = setupBasicInput(startingBalance, transactionAmount, 1);
		Account account = traceable.getPayload().getReservationWorkflow().getAccount();
		ReservationRequest request = traceable.getPayload().getReservationWorkflow().getRequest();

		OverdraftInstruction instruction = traceable.getPayload().getReservationWorkflow().getUnprocessedInstructions().get(0);
		resetBalance(instruction.getOverdraftAccount().getAccountNumber(), odStartingBalance);
		
		OverdraftInstruction instruction2 = randomOverdraft(account.getAccountNumber(), true, true);
		traceable.getPayload().getReservationWorkflow().getUnprocessedInstructions().add(instruction2);
		resetBalance(instruction2.getOverdraftAccount().getAccountNumber(), od2StartingBalance);
		
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

		verifyRequestData(request, keyvalue);

		PostingResponse response = keyvalue.value.getResponse();
		assertNotNull(response);
		assertNotNull(response.getTransactions());
		assertEquals(3, response.getTransactions().size());
		
		// --- First Transaction in Response
		LoggedTransaction rejection = response.getTransactions().get(0);
		verifyReservation(request, rejection, LoggedTransaction.REJECTED_TRANSACTION, request.getAccountNumber());
		assertEquals( expectedBalance, rejection.getRunningBalanceAmount());
		assertEquals( transactionAmount, rejection.getTransactionAmount());

		// --- Second Transaction in Response
		LoggedTransaction rejection2 = response.getTransactions().get(1);
		verifyReservation(request, rejection2, LoggedTransaction.REJECTED_TRANSACTION, instruction.getOverdraftAccount().getAccountNumber());
		assertEquals( expectedODBalance, rejection2.getRunningBalanceAmount());
		assertEquals( transactionAmount, rejection2.getTransactionAmount());

		// --- Third Transaction in Response
		LoggedTransaction reservation = response.getTransactions().get(2);
		verifyReservation(request, reservation, LoggedTransaction.RESERVATION, instruction2.getOverdraftAccount().getAccountNumber());
		assertEquals( expectedOD2Balance, reservation.getRunningBalanceAmount());
		assertEquals( transactionAmount, reservation.getTransactionAmount());

		// --- First LoggedTransaction
		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue);
		assertEquals(account.getAccountNumber(), logKeyValue.key);
		verifyLoggedTraceData(traceable, logKeyValue.value, null);
		verifyTransactions(rejection, logKeyValue.value.getPayload());

		//-- Second Logged Transaction
		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue2 = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue2);
		assertEquals(instruction.getOverdraftAccount().getAccountNumber(), logKeyValue2.key);
		verifyLoggedTraceData(traceable, logKeyValue2.value, null);
		verifyTransactions(rejection2, logKeyValue2.value.getPayload());

		//-- Third Logged Transaction
		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue3 = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue3);
		assertEquals(instruction2.getOverdraftAccount().getAccountNumber(), logKeyValue3.key);
		verifyLoggedTraceData(traceable, logKeyValue3.value, keyvalue.value.getMessageCompletionTime());
		verifyTransactions(reservation, logKeyValue3.value.getPayload());

		// ---First Account Balance
		BalanceLog balanceLog = context.getBalanceStore().get(account.getAccountNumber()).value();
		assertNotNull( balanceLog );
		assertEquals( expectedBalance, balanceLog.getBalance() );
		assertEquals( rejection.getTransactionUuid(), balanceLog.getLastTransaction() );
		
		// ---Second Account Balance
		BalanceLog balanceLog2 = context.getBalanceStore().get(instruction.getOverdraftAccount().getAccountNumber()).value();
		assertNotNull( balanceLog2 );
		assertEquals( expectedODBalance, balanceLog2.getBalance() );
		assertEquals( rejection2.getTransactionUuid(), balanceLog2.getLastTransaction() );
		
		// ---Third Account Balance
		BalanceLog balanceLog3 = context.getBalanceStore().get(instruction2.getOverdraftAccount().getAccountNumber()).value();
		assertNotNull( balanceLog3 );
		assertEquals( expectedOD2Balance, balanceLog3.getBalance() );
		assertEquals( reservation.getTransactionUuid(), balanceLog3.getLastTransaction() );
	}
    
    @Test
	void test_reservation_noMoneyOverdraft_NSF() {
    	drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());

    	long startingBalance = 3333L;
		long odStartingBalance = 3333L;
		long od2StartingBalance = 3333L;
		long transactionAmount = -4444L;
		long expectedBalance = startingBalance;
		long expectedODBalance = odStartingBalance;
		long expectedOD2Balance = od2StartingBalance;
		
		TraceableMessage<WorkflowMessage> traceable = setupBasicInput(startingBalance, transactionAmount, 1);
		Account account = traceable.getPayload().getReservationWorkflow().getAccount();
		ReservationRequest request = traceable.getPayload().getReservationWorkflow().getRequest();

		OverdraftInstruction instruction = traceable.getPayload().getReservationWorkflow().getUnprocessedInstructions().get(0);
		resetBalance(instruction.getOverdraftAccount().getAccountNumber(), odStartingBalance);
		
		OverdraftInstruction instruction2 = randomOverdraft(account.getAccountNumber(), true, true);
		traceable.getPayload().getReservationWorkflow().getUnprocessedInstructions().add(instruction2);
		resetBalance(instruction2.getOverdraftAccount().getAccountNumber(), od2StartingBalance);
		
		// - execute ------------------------
		context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
		
		// - verify -------------------------
		KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue = context.getResponseTopic().readKeyValue();
		assertNotNull(keyvalue);
		assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
		assertNotNull(keyvalue.value);
		assertNotNull(keyvalue.value.getRequest());
		
		verifyTraceData(traceable, keyvalue);
		assertTrue(keyvalue.value.getErrorMessage().contains("Insufficient funds"));
		assertEquals(ResponseMessage.INSUFFICIENT_FUNDS, keyvalue.value.getStatus());

		verifyRequestData(request, keyvalue);

		PostingResponse response = keyvalue.value.getResponse();
		assertNotNull(response);
		assertNotNull(response.getTransactions());
		assertEquals(3, response.getTransactions().size());
		
		// --- First Transaction in Response
		LoggedTransaction rejection = response.getTransactions().get(0);
		verifyReservation(request, rejection, LoggedTransaction.REJECTED_TRANSACTION, request.getAccountNumber());
		assertEquals( expectedBalance, rejection.getRunningBalanceAmount());
		assertEquals( transactionAmount, rejection.getTransactionAmount());

		// --- Second Transaction in Response
		LoggedTransaction rejection2 = response.getTransactions().get(1);
		verifyReservation(request, rejection2, LoggedTransaction.REJECTED_TRANSACTION, instruction.getOverdraftAccount().getAccountNumber());
		assertEquals( expectedODBalance, rejection2.getRunningBalanceAmount());
		assertEquals( transactionAmount, rejection2.getTransactionAmount());

		// --- Third Transaction in Response
		LoggedTransaction reservation = response.getTransactions().get(2);
		verifyReservation(request, reservation, LoggedTransaction.REJECTED_TRANSACTION, instruction2.getOverdraftAccount().getAccountNumber());
		assertEquals( expectedOD2Balance, reservation.getRunningBalanceAmount());
		assertEquals( transactionAmount, reservation.getTransactionAmount());

		// --- First LoggedTransaction
		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue);
		assertEquals(account.getAccountNumber(), logKeyValue.key);
		verifyLoggedTraceData(traceable, logKeyValue.value, null);
		verifyTransactions(rejection, logKeyValue.value.getPayload());

		//-- Second Logged Transaction
		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue2 = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue2);
		assertEquals(instruction.getOverdraftAccount().getAccountNumber(), logKeyValue2.key);
		verifyLoggedTraceData(traceable, logKeyValue2.value, null);
		verifyTransactions(rejection2, logKeyValue2.value.getPayload());

		//-- Third Logged Transaction
		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue3 = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue3);
		assertEquals(instruction2.getOverdraftAccount().getAccountNumber(), logKeyValue3.key);
		verifyLoggedTraceData(traceable, logKeyValue3.value, keyvalue.value.getMessageCompletionTime());
		verifyTransactions(reservation, logKeyValue3.value.getPayload());

		// ---First Account Balance
		BalanceLog balanceLog = context.getBalanceStore().get(account.getAccountNumber()).value();
		assertNotNull( balanceLog );
		assertEquals( expectedBalance, balanceLog.getBalance() );
		assertEquals( rejection.getTransactionUuid(), balanceLog.getLastTransaction() );
		
		// ---Second Account Balance
		BalanceLog balanceLog2 = context.getBalanceStore().get(instruction.getOverdraftAccount().getAccountNumber()).value();
		assertNotNull( balanceLog2 );
		assertEquals( expectedODBalance, balanceLog2.getBalance() );
		assertEquals( rejection2.getTransactionUuid(), balanceLog2.getLastTransaction() );
		
		// ---Third Account Balance
		BalanceLog balanceLog3 = context.getBalanceStore().get(instruction2.getOverdraftAccount().getAccountNumber()).value();
		assertNotNull( balanceLog3 );
		assertEquals( expectedOD2Balance, balanceLog3.getBalance() );
		assertEquals( reservation.getTransactionUuid(), balanceLog3.getLastTransaction() );
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

	private void verifyReservation(ReservationRequest request, LoggedTransaction reservation, String tranType, String accountNumber) {
		assertEquals( accountNumber, reservation.getAccountNumber());
		assertEquals( request.getDebitCardNumber(), reservation.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), reservation.getRequestUuid());
		assertEquals( request.getJsonMetaData(), reservation.getTransactionMetaDataJson());
		assertEquals( tranType, reservation.getTransactionTypeCode());
		assertNotNull( reservation.getTransactionTime());
		assertNotNull( reservation.getTransactionUuid());
		assertNull( reservation.getReservationUuid());
	}

	private void verifyRequestData(ReservationRequest request,
			KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue) {
		ReservationRequest rRequest = keyvalue.value.getRequest().getReservationRequest();
		assertEquals( request.getAccountNumber(), rRequest.getAccountNumber());
		assertEquals( request.getDebitCardNumber(), rRequest.getDebitCardNumber());
		assertEquals( request.getJsonMetaData(), rRequest.getJsonMetaData());
		assertEquals( request.getRequestUuid(), rRequest.getRequestUuid());
		assertEquals( request.getTransactionAmount(), rRequest.getTransactionAmount());
	}

	private void verifyTraceData(TraceableMessage<WorkflowMessage> traceable,
			KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue) {
		assertEquals(traceable.getBusinessTaxonomyId(), keyvalue.value.getBusinessTaxonomyId());
		assertEquals(traceable.getCorrelationId(), keyvalue.value.getCorrelationId());
		assertEquals(traceable.getMessageCreationTime(), keyvalue.value.getMessageCreationTime());
		assertEquals(traceable.getProducerAit(), keyvalue.value.getProducerAit());
		assertNotNull(keyvalue.value.getMessageCompletionTime());
	}
    
    private void resetBalance(String accountNumber, long balance) {
		BalanceLog log = new BalanceLog();
		log.setAccountNumber(accountNumber);
		log.setLastTransaction(UUID.randomUUID());
		log.setBalance(balance);
		context.getBalanceStore().put(accountNumber, ValueAndTimestamp.make(log, System.currentTimeMillis()));
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
	
	public TraceableMessage<WorkflowMessage> setupBasicInput(long startingBalance, long transactionAmount, int overdrafts) {
		// - setup --------------------
		Account account = randomAccount(true, true);
		ArrayList<OverdraftInstruction> instructions = new ArrayList<>();
		for (int ii=0; ii< overdrafts; ii++) {
			OverdraftInstruction overdraft = randomOverdraft(account.getAccountNumber(), true, true);
			instructions.add(overdraft);			
		}
		
		resetBalance(account.getAccountNumber(), startingBalance);
		
		// - prepare --------------------
		ReservationRequest reservationRequest = new ReservationRequest();
		reservationRequest.setRequestUuid(UUID.randomUUID());
		reservationRequest.setAccountNumber(account.getAccountNumber());
		reservationRequest.setDebitCardNumber(null);
		reservationRequest.setTransactionAmount(transactionAmount);
		reservationRequest.setJsonMetaData(JSON_DATA);

		ReservationWorkflow workflow = new ReservationWorkflow(ReservationWorkflow.ACQUIRE_RESERVATION, reservationRequest);
		workflow.setAccount(account);
		workflow.setProcessingAccountNumber(account.getAccountNumber());
		workflow.setUnprocessedInstructions(overdrafts != 0 ? instructions : null);
		
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
