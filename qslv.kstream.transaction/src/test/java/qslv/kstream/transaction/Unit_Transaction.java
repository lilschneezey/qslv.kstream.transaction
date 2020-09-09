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
import qslv.kstream.TransactionRequest;
import qslv.kstream.workflow.TransactionWorkflow;
import qslv.kstream.workflow.WorkflowMessage;
import qslv.util.Random;

@ExtendWith(MockitoExtension.class)
class Unit_Transaction {

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
    void test_transaction_positiveAmount_success() {
    	test_transaction_amount(9999L, 3333L);
    }
   
    @Test
    void test_transaction_negativeAmount_success() {
    	test_transaction_amount(9999L, -3333L);
    }

    void test_transaction_amount(long startingBalance, long transactionAmount) {
    	drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());
    	long expectedBalance = startingBalance + transactionAmount;
		
		TraceableMessage<WorkflowMessage> traceable = setupBasicInput(startingBalance, transactionAmount,0);
		Account account = traceable.getPayload().getTransactionWorkflow().getAccount();
		TransactionRequest request = traceable.getPayload().getTransactionWorkflow().getRequest();
		
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
		
		LoggedTransaction transaction = response.getTransactions().get(0);
		assertEquals( request.getAccountNumber(), transaction.getAccountNumber());
		assertNull( request.getDebitCardNumber(), transaction.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), transaction.getRequestUuid());
		assertNull( transaction.getReservationUuid());
		assertEquals( expectedBalance, transaction.getRunningBalanceAmount());
		assertEquals( transactionAmount, transaction.getTransactionAmount());
		assertEquals( request.getJsonMetaData(), transaction.getTransactionMetaDataJson());
		assertNotNull( transaction.getTransactionTime());
		assertEquals( LoggedTransaction.NORMAL, transaction.getTransactionTypeCode());
		assertNotNull( transaction.getTransactionUuid());

		// get transaction from transaction log topic
		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue);
		assertEquals(account.getAccountNumber(), logKeyValue.key);
		verifyLoggedTraceData(traceable, logKeyValue.value, keyvalue.value.getMessageCompletionTime());
		verifyTransactions(transaction, logKeyValue.value.getPayload());
		
		// -- Verify Balance Log State Store --
		BalanceLog balanceLog = context.getBalanceStore().get(account.getAccountNumber()).value();
		assertNotNull( balanceLog );
		assertEquals( expectedBalance, balanceLog.getBalance() );
		assertEquals(transaction.getTransactionUuid(), balanceLog.getLastTransaction() );
	}

    @Test
    void test_transaction_OD_requestDisallow() {
    	test_transaction_ODdisallow_fail(3333L, -4444L, 9999L, false, true);
    }
    
    @Test
    void test_transaction_OD_accountDisallow() {
    	test_transaction_ODdisallow_fail(3333L, -4444L, 9999L, true, false);
    }

    @Test
    void test_transaction_OD_bothDisallow() {
    	test_transaction_ODdisallow_fail(3333L, -4444L, 9999L, false, false);
    }

    void test_transaction_ODdisallow_fail(long startingBalance, 
			long transactionAmount, 
			long odStartingBalance,
			boolean requestProtect, 
			boolean accountAllow) {
		long expectedBalance = startingBalance;

		drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());
		
		TraceableMessage<WorkflowMessage> traceable = setupBasicInput(startingBalance, transactionAmount,1);
		Account account = traceable.getPayload().getTransactionWorkflow().getAccount();
		TransactionRequest request = traceable.getPayload().getTransactionWorkflow().getRequest();
		OverdraftInstruction instruction = traceable.getPayload().getTransactionWorkflow().getUnprocessedInstructions().get(0);
		
		request.setProtectAgainstOverdraft(requestProtect);
		account.setProtectAgainstOverdraft(accountAllow);
		resetBalance(instruction.getOverdraftAccount().getAccountNumber(), odStartingBalance);
		
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
		assertTrue(keyvalue.value.getErrorMessage().contains("Insufficient funds"));
		assertEquals(ResponseMessage.INSUFFICIENT_FUNDS, keyvalue.value.getStatus());

		verifyRequestData(request, keyvalue);

		PostingResponse response = keyvalue.value.getResponse();
		assertNotNull(response.getTransactions());
		assertEquals(1, response.getTransactions().size());
		
		LoggedTransaction transaction = response.getTransactions().get(0);
		assertEquals( request.getAccountNumber(), transaction.getAccountNumber());
		assertEquals( request.getDebitCardNumber(), transaction.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), transaction.getRequestUuid());
		assertNull( transaction.getReservationUuid());
		assertEquals( expectedBalance, transaction.getRunningBalanceAmount());
		assertEquals( transactionAmount, transaction.getTransactionAmount());
		assertEquals( request.getJsonMetaData(), transaction.getTransactionMetaDataJson());
		assertNotNull( transaction.getTransactionTime());
		assertEquals( LoggedTransaction.REJECTED_TRANSACTION, transaction.getTransactionTypeCode());
		assertNotNull( transaction.getTransactionUuid());

		// get transaction from topic
		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue);
		assertEquals(account.getAccountNumber(), logKeyValue.key);
		verifyLoggedTraceData(traceable, logKeyValue.value, keyvalue.value.getMessageCompletionTime());
		verifyTransactions(transaction, logKeyValue.value.getPayload());
		
		// get State Store
		BalanceLog balanceLog = context.getBalanceStore().get(account.getAccountNumber()).value();
		assertNotNull( balanceLog );
		assertEquals( expectedBalance, balanceLog.getBalance() );
		assertEquals( transaction.getTransactionUuid(), balanceLog.getLastTransaction() );
	}
 
    @Test
    void test_transaction_OD_success() {
    	long startingBalance = 3333L;
		long transactionAmount = -4444L;
		long odStartingBalance = 9999L;
		boolean requestProtect = true;
		boolean accountAllow = true;
		long expectedIntermediaryBalance = startingBalance - transactionAmount;
		long expectedODBalance = odStartingBalance + transactionAmount;

		drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());
		
		TraceableMessage<WorkflowMessage> traceable = setupBasicInput(startingBalance, transactionAmount,1);
		Account account = traceable.getPayload().getTransactionWorkflow().getAccount();
		TransactionRequest request = traceable.getPayload().getTransactionWorkflow().getRequest();
		OverdraftInstruction instruction = traceable.getPayload().getTransactionWorkflow().getUnprocessedInstructions().get(0);
		
		request.setProtectAgainstOverdraft(requestProtect);
		account.setProtectAgainstOverdraft(accountAllow);
		resetBalance(instruction.getOverdraftAccount().getAccountNumber(), odStartingBalance);
		
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
		assertNull( rejection.getReservationUuid());
		assertEquals( startingBalance, rejection.getRunningBalanceAmount());
		assertEquals( transactionAmount, rejection.getTransactionAmount());
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
		assertEquals( transactionAmount, transferFrom.getTransactionAmount());
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
		assertEquals( (0L-transactionAmount), transferTo.getTransactionAmount());
		assertEquals( request.getJsonMetaData(), transferTo.getTransactionMetaDataJson());
		assertNotNull( transferTo.getTransactionTime());
		assertEquals( LoggedTransaction.TRANSFER_TO, transferTo.getTransactionTypeCode());
		assertNotNull( transferTo.getTransactionUuid());

		LoggedTransaction transaction = response.getTransactions().get(3);
		assertEquals( request.getAccountNumber(), transaction.getAccountNumber());
		assertEquals( request.getDebitCardNumber(), transaction.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), transaction.getRequestUuid());
		assertNull( transaction.getReservationUuid());
		assertEquals( startingBalance, transaction.getRunningBalanceAmount());
		assertEquals( transactionAmount, transaction.getTransactionAmount());
		assertEquals( request.getJsonMetaData(), transaction.getTransactionMetaDataJson());
		assertNotNull( transaction.getTransactionTime());
		assertEquals( LoggedTransaction.NORMAL, transaction.getTransactionTypeCode());
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
		assertEquals( startingBalance, balanceLog.getBalance() );
		assertEquals( transaction.getTransactionUuid(), balanceLog.getLastTransaction() );

		BalanceLog balanceLog2 = context.getBalanceStore().get(instruction.getOverdraftAccount().getAccountNumber()).value();
		assertNotNull( balanceLog2 );
		assertEquals( expectedODBalance, balanceLog2.getBalance() );
		assertEquals( transferFrom.getTransactionUuid(), balanceLog2.getLastTransaction() );
	}

    @Test
    void test_transaction_OD_isauthorized_NSF() {
    	long startingBalance = 3333L;
		long transactionAmount = -4444L;
		long odStartingBalance = 2222L;
		boolean requestProtect = true;
		boolean accountAllow = true;

		drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());
		
		TraceableMessage<WorkflowMessage> traceable = setupBasicInput(startingBalance, transactionAmount,1);
		Account account = traceable.getPayload().getTransactionWorkflow().getAccount();
		TransactionRequest request = traceable.getPayload().getTransactionWorkflow().getRequest();
		OverdraftInstruction instruction = traceable.getPayload().getTransactionWorkflow().getUnprocessedInstructions().get(0);
		
		request.setAuthorizeAgainstBalance(true);
		request.setProtectAgainstOverdraft(requestProtect);
		account.setProtectAgainstOverdraft(accountAllow);
		resetBalance(instruction.getOverdraftAccount().getAccountNumber(), odStartingBalance);
		
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
		assertTrue(keyvalue.value.getErrorMessage().contains("Insufficient funds"));
		assertEquals(ResponseMessage.INSUFFICIENT_FUNDS, keyvalue.value.getStatus());

		verifyRequestData(request, keyvalue);

		PostingResponse response = keyvalue.value.getResponse();
		assertNotNull(response.getTransactions());
		assertEquals(2, response.getTransactions().size());
		
		LoggedTransaction rejection = response.getTransactions().get(0);
		assertEquals( request.getAccountNumber(), rejection.getAccountNumber());
		assertEquals( request.getDebitCardNumber(), rejection.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), rejection.getRequestUuid());
		assertNull( rejection.getReservationUuid());
		assertEquals( startingBalance, rejection.getRunningBalanceAmount());
		assertEquals( transactionAmount, rejection.getTransactionAmount());
		assertEquals( request.getJsonMetaData(), rejection.getTransactionMetaDataJson());
		assertNotNull( rejection.getTransactionTime());
		assertEquals( LoggedTransaction.REJECTED_TRANSACTION, rejection.getTransactionTypeCode());
		assertNotNull( rejection.getTransactionUuid());

		LoggedTransaction transferFrom = response.getTransactions().get(1);
		assertEquals( instruction.getOverdraftAccount().getAccountNumber(), transferFrom.getAccountNumber());
		assertNull( transferFrom.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), transferFrom.getRequestUuid());
		assertNull( transferFrom.getReservationUuid());
		assertEquals( odStartingBalance, transferFrom.getRunningBalanceAmount());
		assertEquals( transactionAmount, transferFrom.getTransactionAmount());
		assertEquals( request.getJsonMetaData(), transferFrom.getTransactionMetaDataJson());
		assertNotNull( transferFrom.getTransactionTime());
		assertEquals( LoggedTransaction.REJECTED_TRANSACTION, transferFrom.getTransactionTypeCode());
		assertNotNull( transferFrom.getTransactionUuid());

		// get transaction from topic
		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue);
		assertEquals(account.getAccountNumber(), logKeyValue.key);
		verifyLoggedTraceData(traceable, logKeyValue.value, null);
		verifyTransactions(rejection, logKeyValue.value.getPayload());
		
		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue2 = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue2);
		assertEquals(instruction.getOverdraftAccount().getAccountNumber(), logKeyValue2.key);
		verifyLoggedTraceData(traceable, logKeyValue2.value, keyvalue.value.getMessageCompletionTime());
		verifyTransactions(transferFrom, logKeyValue2.value.getPayload());

		// get State Store
		BalanceLog balanceLog = context.getBalanceStore().get(account.getAccountNumber()).value();
		assertNotNull( balanceLog );
		assertEquals( startingBalance, balanceLog.getBalance() );
		assertEquals( rejection.getTransactionUuid(), balanceLog.getLastTransaction() );

		BalanceLog balanceLog2 = context.getBalanceStore().get(instruction.getOverdraftAccount().getAccountNumber()).value();
		assertNotNull( balanceLog2 );
		assertEquals( odStartingBalance, balanceLog2.getBalance() );
		assertEquals( transferFrom.getTransactionUuid(), balanceLog2.getLastTransaction() );
	}
    
    @Test
    void test_transaction_OD_isNOTauthorized_NSF() {
    	long startingBalance = 3333L;
		long transactionAmount = -4444L;
		long odStartingBalance = 2222L;
		long expectedBalance = startingBalance + transactionAmount;
		boolean requestProtect = true;
		boolean accountAllow = true;

		drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());
		
		TraceableMessage<WorkflowMessage> traceable = setupBasicInput(startingBalance, transactionAmount,1);
		Account account = traceable.getPayload().getTransactionWorkflow().getAccount();
		TransactionRequest request = traceable.getPayload().getTransactionWorkflow().getRequest();
		OverdraftInstruction instruction = traceable.getPayload().getTransactionWorkflow().getUnprocessedInstructions().get(0);
		
		request.setAuthorizeAgainstBalance(false);
		request.setProtectAgainstOverdraft(requestProtect);
		account.setProtectAgainstOverdraft(accountAllow);
		resetBalance(instruction.getOverdraftAccount().getAccountNumber(), odStartingBalance);
		
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
		assertEquals(3, response.getTransactions().size());
		
		LoggedTransaction rejection = response.getTransactions().get(0);
		assertEquals( request.getAccountNumber(), rejection.getAccountNumber());
		assertEquals( request.getDebitCardNumber(), rejection.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), rejection.getRequestUuid());
		assertNull( rejection.getReservationUuid());
		assertEquals( startingBalance, rejection.getRunningBalanceAmount());
		assertEquals( transactionAmount, rejection.getTransactionAmount());
		assertEquals( request.getJsonMetaData(), rejection.getTransactionMetaDataJson());
		assertNotNull( rejection.getTransactionTime());
		assertEquals( LoggedTransaction.REJECTED_TRANSACTION, rejection.getTransactionTypeCode());
		assertNotNull( rejection.getTransactionUuid());

		LoggedTransaction transferFrom = response.getTransactions().get(1);
		assertEquals( instruction.getOverdraftAccount().getAccountNumber(), transferFrom.getAccountNumber());
		assertNull( transferFrom.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), transferFrom.getRequestUuid());
		assertNull( transferFrom.getReservationUuid());
		assertEquals( odStartingBalance, transferFrom.getRunningBalanceAmount());
		assertEquals( transactionAmount, transferFrom.getTransactionAmount());
		assertEquals( request.getJsonMetaData(), transferFrom.getTransactionMetaDataJson());
		assertNotNull( transferFrom.getTransactionTime());
		assertEquals( LoggedTransaction.REJECTED_TRANSACTION, transferFrom.getTransactionTypeCode());
		assertNotNull( transferFrom.getTransactionUuid());

		LoggedTransaction transaction = response.getTransactions().get(2);
		assertEquals( request.getAccountNumber(), transaction.getAccountNumber());
		assertEquals( request.getDebitCardNumber(), transaction.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), transaction.getRequestUuid());
		assertNull( transaction.getReservationUuid());
		assertEquals( expectedBalance, transaction.getRunningBalanceAmount());
		assertEquals( transactionAmount, transaction.getTransactionAmount());
		assertEquals( request.getJsonMetaData(), transaction.getTransactionMetaDataJson());
		assertNotNull( transaction.getTransactionTime());
		assertEquals( LoggedTransaction.NORMAL, transaction.getTransactionTypeCode());
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
		verifyLoggedTraceData(traceable, logKeyValue3.value, keyvalue.value.getMessageCompletionTime());
		verifyTransactions(transaction, logKeyValue3.value.getPayload());

		// get State Store
		BalanceLog balanceLog = context.getBalanceStore().get(account.getAccountNumber()).value();
		assertNotNull( balanceLog );
		assertEquals( expectedBalance, balanceLog.getBalance() );
		assertEquals( transaction.getTransactionUuid(), balanceLog.getLastTransaction() );

		BalanceLog balanceLog2 = context.getBalanceStore().get(instruction.getOverdraftAccount().getAccountNumber()).value();
		assertNotNull( balanceLog2 );
		assertEquals( odStartingBalance, balanceLog2.getBalance() );
		assertEquals( transferFrom.getTransactionUuid(), balanceLog2.getLastTransaction() );
	}
    
    @Test
    void test_transaction_two_OD_success() {
    	long startingBalance = 3333L;
		long transactionAmount = -4444L;
		long odStartingBalance = 3333L;
		long od2StartingBalance = 9999L;
		boolean requestProtect = true;
		boolean accountAllow = true;
		long expectedIntermediaryBalance = startingBalance - transactionAmount;
		long expectedODBalance = odStartingBalance;
		long expectedOD2Balance = od2StartingBalance + transactionAmount;

		drain(context.getResponseTopic());
    	drain(context.getTransactionLogTopic());
		
		TraceableMessage<WorkflowMessage> traceable = setupBasicInput(startingBalance, transactionAmount,1);
		Account account = traceable.getPayload().getTransactionWorkflow().getAccount();
		TransactionRequest request = traceable.getPayload().getTransactionWorkflow().getRequest();
		OverdraftInstruction instruction = traceable.getPayload().getTransactionWorkflow().getUnprocessedInstructions().get(0);
		OverdraftInstruction instruction2 = randomOverdraft(account.getAccountNumber(), true, true);
		traceable.getPayload().getTransactionWorkflow().getUnprocessedInstructions().add(instruction2);
		
		request.setProtectAgainstOverdraft(requestProtect);
		account.setProtectAgainstOverdraft(accountAllow);
		resetBalance(instruction.getOverdraftAccount().getAccountNumber(), odStartingBalance);
		resetBalance(instruction2.getOverdraftAccount().getAccountNumber(), od2StartingBalance);
		
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
		assertEquals(5, response.getTransactions().size());
		
		LoggedTransaction rejection = response.getTransactions().get(0);
		assertEquals( request.getAccountNumber(), rejection.getAccountNumber());
		assertEquals( request.getDebitCardNumber(), rejection.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), rejection.getRequestUuid());
		assertNull( rejection.getReservationUuid());
		assertEquals( startingBalance, rejection.getRunningBalanceAmount());
		assertEquals( transactionAmount, rejection.getTransactionAmount());
		assertEquals( request.getJsonMetaData(), rejection.getTransactionMetaDataJson());
		assertNotNull( rejection.getTransactionTime());
		assertEquals( LoggedTransaction.REJECTED_TRANSACTION, rejection.getTransactionTypeCode());
		assertNotNull( rejection.getTransactionUuid());

		LoggedTransaction transfer1From = response.getTransactions().get(1);
		assertEquals( instruction.getOverdraftAccount().getAccountNumber(), transfer1From.getAccountNumber());
		assertNull( transfer1From.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), transfer1From.getRequestUuid());
		assertNull( transfer1From.getReservationUuid());
		assertEquals( expectedODBalance, transfer1From.getRunningBalanceAmount());
		assertEquals( transactionAmount, transfer1From.getTransactionAmount());
		assertEquals( request.getJsonMetaData(), transfer1From.getTransactionMetaDataJson());
		assertNotNull( transfer1From.getTransactionTime());
		assertEquals( LoggedTransaction.REJECTED_TRANSACTION, transfer1From.getTransactionTypeCode());
		assertNotNull( transfer1From.getTransactionUuid());

		LoggedTransaction transfer2From = response.getTransactions().get(2);
		assertEquals( instruction2.getOverdraftAccount().getAccountNumber(), transfer2From.getAccountNumber());
		assertNull( transfer2From.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), transfer2From.getRequestUuid());
		assertNull( transfer2From.getReservationUuid());
		assertEquals( expectedOD2Balance, transfer2From.getRunningBalanceAmount());
		assertEquals( transactionAmount, transfer2From.getTransactionAmount());
		assertEquals( request.getJsonMetaData(), transfer2From.getTransactionMetaDataJson());
		assertNotNull( transfer2From.getTransactionTime());
		assertEquals( LoggedTransaction.TRANSFER_FROM, transfer2From.getTransactionTypeCode());
		assertNotNull( transfer2From.getTransactionUuid());

		LoggedTransaction transferTo = response.getTransactions().get(3);
		assertEquals( request.getAccountNumber(), transferTo.getAccountNumber());
		assertNull( transferTo.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), transferTo.getRequestUuid());
		assertNull( transferTo.getReservationUuid());
		assertEquals( expectedIntermediaryBalance, transferTo.getRunningBalanceAmount());
		assertEquals( (0L-transactionAmount), transferTo.getTransactionAmount());
		assertEquals( request.getJsonMetaData(), transferTo.getTransactionMetaDataJson());
		assertNotNull( transferTo.getTransactionTime());
		assertEquals( LoggedTransaction.TRANSFER_TO, transferTo.getTransactionTypeCode());
		assertNotNull( transferTo.getTransactionUuid());

		LoggedTransaction transaction = response.getTransactions().get(4);
		assertEquals( request.getAccountNumber(), transaction.getAccountNumber());
		assertEquals( request.getDebitCardNumber(), transaction.getDebitCardNumber());
		assertEquals( request.getRequestUuid(), transaction.getRequestUuid());
		assertNull( transaction.getReservationUuid());
		assertEquals( startingBalance, transaction.getRunningBalanceAmount());
		assertEquals( transactionAmount, transaction.getTransactionAmount());
		assertEquals( request.getJsonMetaData(), transaction.getTransactionMetaDataJson());
		assertNotNull( transaction.getTransactionTime());
		assertEquals( LoggedTransaction.NORMAL, transaction.getTransactionTypeCode());
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
		verifyTransactions(transfer1From, logKeyValue2.value.getPayload());

		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue3 = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue3);
		assertEquals(instruction2.getOverdraftAccount().getAccountNumber(), logKeyValue3.key);
		verifyLoggedTraceData(traceable, logKeyValue3.value, null);
		verifyTransactions(transfer2From, logKeyValue3.value.getPayload());

		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue4 = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue4);
		assertEquals(account.getAccountNumber(), logKeyValue4.key);
		verifyLoggedTraceData(traceable, logKeyValue4.value, null);
		verifyTransactions(transferTo, logKeyValue4.value.getPayload());

		KeyValue<String, TraceableMessage<LoggedTransaction>> logKeyValue5 = context.getTransactionLogTopic().readKeyValue();
		assertNotNull(logKeyValue5);
		assertEquals(account.getAccountNumber(), logKeyValue5.key);
		verifyLoggedTraceData(traceable, logKeyValue5.value, keyvalue.value.getMessageCompletionTime());
		verifyTransactions(transaction, logKeyValue5.value.getPayload());

		// get State Store
		BalanceLog balanceLog = context.getBalanceStore().get(account.getAccountNumber()).value();
		assertNotNull( balanceLog );
		assertEquals( startingBalance, balanceLog.getBalance() );
		assertEquals( transaction.getTransactionUuid(), balanceLog.getLastTransaction() );

		BalanceLog balanceLog2 = context.getBalanceStore().get(instruction.getOverdraftAccount().getAccountNumber()).value();
		assertNotNull( balanceLog2 );
		assertEquals( expectedODBalance, balanceLog2.getBalance() );
		assertEquals( transfer1From.getTransactionUuid(), balanceLog2.getLastTransaction() );

		BalanceLog balanceLog3 = context.getBalanceStore().get(instruction2.getOverdraftAccount().getAccountNumber()).value();
		assertNotNull( balanceLog3 );
		assertEquals( expectedOD2Balance, balanceLog3.getBalance() );
		assertEquals( transfer2From.getTransactionUuid(), balanceLog3.getLastTransaction() );
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

	private void verifyRequestData(TransactionRequest request,
			KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue) {
		TransactionRequest rRequest = keyvalue.value.getRequest().getTransactionRequest();
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
		TransactionRequest transactionRequest = new TransactionRequest();
		transactionRequest.setRequestUuid(UUID.randomUUID());
		transactionRequest.setAccountNumber(account.getAccountNumber());
		transactionRequest.setDebitCardNumber(null);
		transactionRequest.setTransactionAmount(transactionAmount);
		transactionRequest.setJsonMetaData(JSON_DATA);

		TransactionWorkflow workflow = new TransactionWorkflow(TransactionWorkflow.TRANSACT_START, transactionRequest);
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
