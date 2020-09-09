package qslv.kstream.transaction;

import java.time.LocalDateTime;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import qslv.data.BalanceLog;
import qslv.kstream.LoggedTransaction;
import qslv.kstream.TransferRequest;
import qslv.kstream.workflow.TransferWorkflow;

public class TransferProcessor {
	private static final Logger log = LoggerFactory.getLogger(TransferProcessor.class);
	
	public static BalanceLog processStartTransfer(TransferWorkflow workflow, final BalanceLog currentBalance ) {
		
		TransferRequest request = workflow.getRequest();	
		boolean insufficientFunds = Math.abs(request.getTransferAmount()) > currentBalance.getBalance();
		BalanceLog newBalance = null;

		LoggedTransaction transfer = new LoggedTransaction();
		transfer.setAccountNumber(request.getTransferFromAccountNumber());
		transfer.setDebitCardNumber(null);
		transfer.setRequestUuid(request.getRequestUuid());
		transfer.setReservationUuid(null);
		transfer.setTransactionAmount( 0L - Math.abs(request.getTransferAmount()) );
		transfer.setTransactionMetaDataJson(request.getJsonMetaData());
		transfer.setTransactionTime(LocalDateTime.now());
		transfer.setTransactionUuid(UUID.randomUUID());

		if ( insufficientFunds ) {
			transfer.setRunningBalanceAmount(currentBalance.getBalance());
			transfer.setTransactionTypeCode(LoggedTransaction.REJECTED_TRANSACTION);

			workflow.getResults().add(transfer);
			workflow.getAccumulatedResults().add(transfer);
			workflow.setState(TransferWorkflow.INSUFFICIENT_FUNDS);
			String errorMessage = String.format("Insufficient funds. %d requested. %d balance. ",
					request.getTransferAmount(), currentBalance.getBalance() );
			workflow.setErrorMessage(errorMessage);
			log.debug(errorMessage);

			newBalance = new BalanceLog( currentBalance.getAccountNumber(), transfer.getTransactionUuid(), currentBalance.getBalance());

		} else {			
			long runningBalance = currentBalance.getBalance() - Math.abs(request.getTransferAmount());
	
			transfer.setRunningBalanceAmount( runningBalance );
			transfer.setTransactionTypeCode(LoggedTransaction.TRANSFER_FROM);
	
			workflow.getResults().add(transfer);
			workflow.getAccumulatedResults().add(transfer);
			workflow.setState(TransferWorkflow.TRANSFER_FUNDS);
			workflow.setProcessingAccountNumber(request.getTransferToAccountNumber());
			workflow.setErrorMessage(null);
			newBalance = new BalanceLog( currentBalance.getAccountNumber(), transfer.getTransactionUuid(), runningBalance);
		}

		return newBalance;
	}

	public static BalanceLog processTransfer(TransferWorkflow workflow, final BalanceLog currentBalance ) {
		
		TransferRequest request = workflow.getRequest();
		long runningBalance = currentBalance.getBalance() + Math.abs(request.getTransferAmount());
				
		LoggedTransaction transaction = new LoggedTransaction();
		transaction.setAccountNumber(request.getTransferToAccountNumber());
		transaction.setDebitCardNumber(null);
		transaction.setRequestUuid(request.getRequestUuid());
		transaction.setReservationUuid(null);
		transaction.setTransactionAmount(request.getTransferAmount());
		transaction.setTransactionMetaDataJson(request.getJsonMetaData());
		transaction.setTransactionTime(LocalDateTime.now());
		transaction.setTransactionUuid(UUID.randomUUID());
		transaction.setRunningBalanceAmount( runningBalance );
		transaction.setTransactionTypeCode(LoggedTransaction.TRANSFER_TO);

		workflow.getResults().add(transaction);
		workflow.getAccumulatedResults().add(transaction);
		workflow.setState(TransferWorkflow.SUCCESS);
		workflow.setErrorMessage(null);

		log.debug("Transaction Approved with Overdraft. Account ={}, New Balance = {}", currentBalance.getAccountNumber(), runningBalance);

		return new BalanceLog( currentBalance.getAccountNumber(), transaction.getTransactionUuid(), runningBalance);
	}

}
