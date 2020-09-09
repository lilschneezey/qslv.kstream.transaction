package qslv.kstream.transaction;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import qslv.data.BalanceLog;
import qslv.data.OverdraftInstruction;
import qslv.kstream.LoggedTransaction;
import qslv.kstream.TransactionRequest;
import qslv.kstream.workflow.TransactionWorkflow;

public class TransactionProcessor {
	private static final Logger log = LoggerFactory.getLogger(TransactionProcessor.class);

	public static BalanceLog processTransactStart(TransactionWorkflow workflow, final BalanceLog currentBalance ) {
		
		TransactionRequest request = workflow.getRequest();
		List<OverdraftInstruction> overdrafts = workflow.getUnprocessedInstructions();
		boolean insufficientFunds = request.getTransactionAmount() < 0L 
				&&	Math.abs(request.getTransactionAmount()) > currentBalance.getBalance();
		boolean canOverdraft = request.isProtectAgainstOverdraft()
				&& workflow.getAccount().isProtectAgainstOverdraft()
				&& overdrafts != null
				&& overdrafts.size() > 0;
				
		BalanceLog newBalance = null;

		// - setup common fields for log
		LoggedTransaction transaction = new LoggedTransaction();
		transaction.setAccountNumber(request.getAccountNumber());
		transaction.setDebitCardNumber(request.getDebitCardNumber());
		transaction.setRequestUuid(request.getRequestUuid());
		transaction.setReservationUuid(null);
		transaction.setTransactionAmount(request.getTransactionAmount());
		transaction.setTransactionMetaDataJson(request.getJsonMetaData());
		transaction.setTransactionTime(LocalDateTime.now());
		transaction.setTransactionUuid(UUID.randomUUID());

		if (insufficientFunds && canOverdraft) {
			// Reject Transaction and send to other accounts to start a transfer
			transaction.setRunningBalanceAmount(currentBalance.getBalance());
			transaction.setTransactionTypeCode(LoggedTransaction.REJECTED_TRANSACTION);
			
			workflow.getResults().add(transaction);
			workflow.getAccumulatedResults().add(transaction);
			workflow.setProcessingAccountNumber(overdrafts.get(0).getOverdraftAccount().getAccountNumber());
			if (workflow.getProcessedInstructions() == null ) {
				workflow.setProcessedInstructions( new ArrayList<>());
			}
			workflow.getProcessedInstructions().add(overdrafts.get(0));
			workflow.getUnprocessedInstructions().remove(0);
			workflow.setState(TransactionWorkflow.TRANSFER_FROM_OVERDRAFT_PROTECTION);

			if (log.isDebugEnabled()) {
				log.debug("Insufficient funds. {} requested. {} balance. Protect against overdraft {}. {} Overdraft records processed {}",
						request.getTransactionAmount(), currentBalance.getBalance(), request.isProtectAgainstOverdraft(), 
						workflow.getAccount().isProtectAgainstOverdraft(), overdrafts.size() );
			}
			newBalance = new BalanceLog( currentBalance.getAccountNumber(), transaction.getTransactionUuid(), currentBalance.getBalance());

		} else if (insufficientFunds && request.isAuthorizeAgainstBalance() ) {
			// Reject Transaction and Response with Failure
			
			transaction.setRunningBalanceAmount(currentBalance.getBalance());
			transaction.setTransactionTypeCode(LoggedTransaction.REJECTED_TRANSACTION);
			
			workflow.getResults().add(transaction);
			workflow.getAccumulatedResults().add(transaction);
			workflow.setState(TransactionWorkflow.INSUFFICIENT_FUNDS);
			String errorMessage = String.format("Insufficient funds. %d requested. %d balance. "
					+ "Protect against overdraft %s/%s. No Overdraft records processed out of %d.",
					request.getTransactionAmount(), currentBalance.getBalance(), request.isProtectAgainstOverdraft(),  
					workflow.getAccount().isProtectAgainstOverdraft(), overdrafts==null ? 0 : overdrafts.size() );
			workflow.setErrorMessage(errorMessage);
			log.debug(errorMessage);

			newBalance = new BalanceLog( currentBalance.getAccountNumber(), transaction.getTransactionUuid(), currentBalance.getBalance());

		} else {
			
			// Transaction Approved, regardless of current balance
			long runningBalance = currentBalance.getBalance() + request.getTransactionAmount();

			transaction.setRunningBalanceAmount(runningBalance);
			transaction.setTransactionTypeCode(LoggedTransaction.NORMAL);

			workflow.getResults().add(transaction);
			workflow.getAccumulatedResults().add(transaction);
			workflow.setState(TransactionWorkflow.SUCCESS);
			workflow.setErrorMessage(null);

			log.debug("Transaction Approved. Account ={}, New Balance = {}", currentBalance.getAccountNumber(), runningBalance);

			newBalance = new BalanceLog( currentBalance.getAccountNumber(), transaction.getTransactionUuid(), runningBalance);	
		}
		return newBalance;
	}
	
	public static BalanceLog processTransferFromOverdraft(TransactionWorkflow workflow, final BalanceLog currentBalance ) {
		
		TransactionRequest request = workflow.getRequest();
		List<OverdraftInstruction> overdrafts = workflow.getUnprocessedInstructions();
		boolean insufficientFunds = request.getTransactionAmount() < 0L 
				&&	Math.abs(request.getTransactionAmount()) > currentBalance.getBalance();
		boolean canOverdraft = overdrafts.size() > 0;
				
		BalanceLog newBalance = null;

		// - setup common fields for log
		LoggedTransaction transaction = new LoggedTransaction();
		transaction.setAccountNumber(workflow.getProcessingAccountNumber());
		transaction.setDebitCardNumber(null);
		transaction.setRequestUuid(request.getRequestUuid());
		transaction.setReservationUuid(null);
		transaction.setTransactionAmount(request.getTransactionAmount());
		transaction.setTransactionMetaDataJson(request.getJsonMetaData());
		transaction.setTransactionTime(LocalDateTime.now());
		transaction.setTransactionUuid(UUID.randomUUID());

		if (insufficientFunds && canOverdraft) {
			// Reject Transaction and send to other accounts to start a transfer
			transaction.setRunningBalanceAmount(currentBalance.getBalance());
			transaction.setTransactionTypeCode(LoggedTransaction.REJECTED_TRANSACTION);
			
			workflow.getResults().add(transaction);
			workflow.getAccumulatedResults().add(transaction);
			workflow.setProcessingAccountNumber(overdrafts.get(0).getOverdraftAccount().getAccountNumber());
			workflow.getProcessedInstructions().add(overdrafts.get(0));
			workflow.getUnprocessedInstructions().remove(0);
			workflow.setState(TransactionWorkflow.TRANSFER_FROM_OVERDRAFT_PROTECTION);

			if (log.isDebugEnabled()) {
				log.debug("Insufficient funds. {} requested. {} balance. Protect against overdraft {}. {} Overdraft records processed {}",
						request.getTransactionAmount(), currentBalance.getBalance(), request.isProtectAgainstOverdraft(), 
						workflow.getAccount().isProtectAgainstOverdraft(), overdrafts.size() );
			}
			newBalance = new BalanceLog( currentBalance.getAccountNumber(), transaction.getTransactionUuid(), currentBalance.getBalance());

		} else if ( insufficientFunds && request.isAuthorizeAgainstBalance() ) {
			// Reject Transaction and Response with Failure
			
			transaction.setRunningBalanceAmount(currentBalance.getBalance());
			transaction.setTransactionTypeCode(LoggedTransaction.REJECTED_TRANSACTION);
			
			workflow.getResults().add(transaction);
			workflow.getAccumulatedResults().add(transaction);
			workflow.setState(TransactionWorkflow.INSUFFICIENT_FUNDS);
			String errorMessage = String.format("Insufficient funds. %d requested. %d balance. "
					+ "Protect against overdraft %s/%s. Overdraft records processed %d",
					request.getTransactionAmount(), currentBalance.getBalance(), request.isProtectAgainstOverdraft(),  
					workflow.getAccount().isProtectAgainstOverdraft(), overdrafts.size() );
			workflow.setErrorMessage(errorMessage);
			log.debug(errorMessage);

			newBalance = new BalanceLog( currentBalance.getAccountNumber(), transaction.getTransactionUuid(), currentBalance.getBalance());

		} else if ( insufficientFunds ) {
			// Reject Transaction and send back to original account
			
			transaction.setRunningBalanceAmount(currentBalance.getBalance());
			transaction.setTransactionTypeCode(LoggedTransaction.REJECTED_TRANSACTION);
			
			workflow.getResults().add(transaction);
			workflow.getAccumulatedResults().add(transaction);
			workflow.setState(TransactionWorkflow.OVERDRAFT_ACCOUNT);
			workflow.setProcessingAccountNumber(workflow.getRequest().getAccountNumber());
			
			if (log.isDebugEnabled()) {
				log.debug("Insufficient funds. {} requested. {} balance. Protect against overdraft {}/{}",
						request.getTransactionAmount(), currentBalance.getBalance(), request.isProtectAgainstOverdraft(), 
						workflow.getAccount().isProtectAgainstOverdraft() );
			}

			newBalance = new BalanceLog( currentBalance.getAccountNumber(), transaction.getTransactionUuid(), currentBalance.getBalance());

		} else {
			// Transaction Approved, return to the original account to complete transfer
			long runningBalance = currentBalance.getBalance() + request.getTransactionAmount();

			transaction.setRunningBalanceAmount(runningBalance);
			transaction.setTransactionTypeCode(LoggedTransaction.TRANSFER_FROM);

			workflow.getResults().add(transaction);
			workflow.getAccumulatedResults().add(transaction);
			workflow.setState(TransactionWorkflow.TRANSFER_AND_TRANSACT);
			workflow.setProcessingAccountNumber(workflow.getRequest().getAccountNumber());
			workflow.setErrorMessage(null);

			log.debug("Transaction Approved. Account ={}, New Balance = {}", currentBalance.getAccountNumber(), runningBalance);

			newBalance = new BalanceLog( currentBalance.getAccountNumber(), transaction.getTransactionUuid(), runningBalance);	
		}
		return newBalance;
	}
	
	public static BalanceLog processTransferAndTransact(TransactionWorkflow workflow, final BalanceLog currentBalance ) {
		
		TransactionRequest request = workflow.getRequest();	
		long transferRunningBalance = currentBalance.getBalance() + Math.abs(request.getTransactionAmount());
		long transactRunningBalance = currentBalance.getBalance() + Math.abs(request.getTransactionAmount()) - Math.abs(request.getTransactionAmount());

		LoggedTransaction transfer = new LoggedTransaction();
		transfer.setAccountNumber(request.getAccountNumber());
		transfer.setDebitCardNumber(null);
		transfer.setRequestUuid(request.getRequestUuid());
		transfer.setReservationUuid(null);
		transfer.setTransactionAmount( Math.abs(request.getTransactionAmount()) );
		transfer.setTransactionMetaDataJson(request.getJsonMetaData());
		transfer.setTransactionTime(LocalDateTime.now());
		transfer.setTransactionUuid(UUID.randomUUID());
		transfer.setRunningBalanceAmount( transferRunningBalance );
		transfer.setTransactionTypeCode(LoggedTransaction.TRANSFER_TO);

		LoggedTransaction transaction = new LoggedTransaction();
		transaction.setAccountNumber(request.getAccountNumber());
		transaction.setDebitCardNumber(request.getDebitCardNumber());
		transaction.setRequestUuid(request.getRequestUuid());
		transaction.setReservationUuid(null);
		transaction.setTransactionAmount(request.getTransactionAmount());
		transaction.setTransactionMetaDataJson(request.getJsonMetaData());
		transaction.setTransactionTime(LocalDateTime.now());
		transaction.setTransactionUuid(UUID.randomUUID());
		transaction.setRunningBalanceAmount( transactRunningBalance );
		transaction.setTransactionTypeCode(LoggedTransaction.NORMAL);

		workflow.getResults().add(transfer);
		workflow.getAccumulatedResults().add(transfer);
		workflow.getResults().add(transaction);
		workflow.getAccumulatedResults().add(transaction);
		workflow.setState(TransactionWorkflow.SUCCESS);
		workflow.setErrorMessage(null);

		return new BalanceLog( currentBalance.getAccountNumber(), transaction.getTransactionUuid(), transactRunningBalance);
	}

	public static BalanceLog processOverDraftAccount(TransactionWorkflow workflow, final BalanceLog currentBalance ) {
		
		TransactionRequest request = workflow.getRequest();
		long runningBalance = currentBalance.getBalance() + request.getTransactionAmount();
				
		LoggedTransaction transaction = new LoggedTransaction();
		transaction.setAccountNumber(request.getAccountNumber());
		transaction.setDebitCardNumber(request.getDebitCardNumber());
		transaction.setRequestUuid(request.getRequestUuid());
		transaction.setReservationUuid(null);
		transaction.setTransactionAmount(request.getTransactionAmount());
		transaction.setTransactionMetaDataJson(request.getJsonMetaData());
		transaction.setTransactionTime(LocalDateTime.now());
		transaction.setTransactionUuid(UUID.randomUUID());
		transaction.setRunningBalanceAmount( runningBalance );
		transaction.setTransactionTypeCode(LoggedTransaction.NORMAL);

		workflow.getResults().add(transaction);
		workflow.getAccumulatedResults().add(transaction);
		workflow.setState(TransactionWorkflow.SUCCESS);
		workflow.setErrorMessage(null);

		log.debug("Transaction Approved with Overdraft. Account ={}, New Balance = {}", currentBalance.getAccountNumber(), runningBalance);

		return new BalanceLog( currentBalance.getAccountNumber(), transaction.getTransactionUuid(), runningBalance);
	}

}
