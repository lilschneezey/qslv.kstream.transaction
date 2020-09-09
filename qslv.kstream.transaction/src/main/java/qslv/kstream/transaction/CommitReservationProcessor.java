package qslv.kstream.transaction;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import qslv.data.BalanceLog;
import qslv.data.OverdraftInstruction;
import qslv.kstream.CommitReservationRequest;
import qslv.kstream.LoggedTransaction;
import qslv.kstream.workflow.CommitReservationWorkflow;

public class CommitReservationProcessor {
	private static final Logger log = LoggerFactory.getLogger(CommitReservationProcessor.class);

	public static BalanceLog processCommitStart(CommitReservationWorkflow workflow, final BalanceLog currentBalance ) {

		CommitReservationRequest request = workflow.getRequest();
		List<OverdraftInstruction> overdrafts = workflow.getUnprocessedInstructions();
		long remainingCharge = request.getTransactionAmount() - workflow.getReservation().getTransactionAmount();
		boolean insufficientFunds = remainingCharge < 0L &&	Math.abs(remainingCharge) > currentBalance.getBalance();
		boolean canOverdraft = request.isProtectAgainstOverdraft()
				&& workflow.getAccount().isProtectAgainstOverdraft()
				&& overdrafts != null 
				&& overdrafts.size() > 0;
		BalanceLog newBalance = null;

		if ( workflow.getReservation() == null 
				|| false == workflow.getReservation().getTransactionUuid().equals(request.getReservationUuid()) ) {
			
			String errorMessage = String.format("Reservation not found. {}", request.getReservationUuid());
			log.error(errorMessage);
			workflow.setErrorMessage(errorMessage);
			workflow.setState(CommitReservationWorkflow.NO_MATCH);
			
		} else if ( insufficientFunds && canOverdraft ) {
	
			// Log Reject Transaction
			LoggedTransaction rejection = new LoggedTransaction();
			rejection.setAccountNumber(request.getAccountNumber());
			rejection.setDebitCardNumber(request.getDebitCardNumber());
			rejection.setRequestUuid(request.getRequestUuid());
			rejection.setReservationUuid( workflow.getReservation().getTransactionUuid() );
			rejection.setRunningBalanceAmount( currentBalance.getBalance() );
			rejection.setTransactionAmount( remainingCharge );
			rejection.setTransactionMetaDataJson(request.getJsonMetaData());
			rejection.setTransactionTime(LocalDateTime.now());
			rejection.setTransactionTypeCode(LoggedTransaction.REJECTED_TRANSACTION);
			rejection.setTransactionUuid(UUID.randomUUID());
			
			workflow.getResults().add(rejection);
			workflow.getAccumulatedResults().add(rejection);
			
			workflow.setProcessingAccountNumber(overdrafts.get(0).getOverdraftAccount().getAccountNumber());
			if (workflow.getProcessedInstructions()==null) workflow.setProcessedInstructions(new ArrayList<>());
			workflow.getProcessedInstructions().add(overdrafts.get(0));
			workflow.getUnprocessedInstructions().remove(0);
			workflow.setState(CommitReservationWorkflow.TRANSFER_FROM_OVERDRAFT_PROTECTION);

			if (log.isDebugEnabled()) {
				log.debug("Insufficient funds. {} requested. {} balance. Protect against overdraft {}/{} Overdraft records processed: {}",
						request.getTransactionAmount(), currentBalance.getBalance(), request.isProtectAgainstOverdraft(), 
						workflow.getAccount().isProtectAgainstOverdraft(), workflow.getProcessedInstructions().size() );
			}
			
			newBalance = new BalanceLog( currentBalance.getAccountNumber(), rejection.getTransactionUuid(), currentBalance.getBalance());

		} else {
			// Commit Approved even if it takes us insufficient
			
			long runningBalance = currentBalance.getBalance() + remainingCharge;

			LoggedTransaction commit = new LoggedTransaction();
			commit.setAccountNumber(request.getAccountNumber());
			commit.setDebitCardNumber(workflow.getReservation().getDebitCardNumber());
			commit.setRequestUuid(request.getRequestUuid());
			commit.setReservationUuid(workflow.getReservation().getTransactionUuid());
			commit.setRunningBalanceAmount( runningBalance );
			commit.setTransactionAmount( remainingCharge );
			commit.setTransactionMetaDataJson(request.getJsonMetaData());
			commit.setTransactionTime(LocalDateTime.now());
			commit.setTransactionTypeCode(LoggedTransaction.RESERVATION_COMMIT);
			commit.setTransactionUuid(UUID.randomUUID());
			
			workflow.getResults().add(commit);
			workflow.getAccumulatedResults().add(commit);
			workflow.setErrorMessage(null);
			workflow.setState(CommitReservationWorkflow.SUCCESS);

			log.debug("Commit Reservation Approved. Account ={}, New Balance = {}", 
					currentBalance.getAccountNumber(), runningBalance);

			newBalance = new BalanceLog( currentBalance.getAccountNumber(), 
					commit.getTransactionUuid(), runningBalance);
		}
		return newBalance;
	}

	public static BalanceLog processTransferFromOverdraft(CommitReservationWorkflow workflow, final BalanceLog currentBalance ) {

		CommitReservationRequest request = workflow.getRequest();
		List<OverdraftInstruction> overdrafts = workflow.getUnprocessedInstructions();
		long remainingCharge = request.getTransactionAmount() - workflow.getReservation().getTransactionAmount();
		boolean insufficientFunds = remainingCharge < 0L &&	Math.abs(remainingCharge) > currentBalance.getBalance();
		boolean canOverdraft = overdrafts != null && overdrafts.size() > 0;
		BalanceLog newBalance = null;

		if ( insufficientFunds ) {
	
			// Log Reject Transaction
			LoggedTransaction rejection = new LoggedTransaction();
			rejection.setAccountNumber(workflow.getProcessingAccountNumber());
			rejection.setDebitCardNumber(null);
			rejection.setRequestUuid(request.getRequestUuid());
			rejection.setReservationUuid( null );
			rejection.setRunningBalanceAmount( currentBalance.getBalance() );
			rejection.setTransactionAmount( remainingCharge );
			rejection.setTransactionMetaDataJson(request.getJsonMetaData());
			rejection.setTransactionTime(LocalDateTime.now());
			rejection.setTransactionTypeCode(LoggedTransaction.REJECTED_TRANSACTION);
			rejection.setTransactionUuid(UUID.randomUUID());
			
			workflow.getResults().add(rejection);
			workflow.getAccumulatedResults().add(rejection);
			
			if ( canOverdraft ) {
				// send to the next account for sufficient funds
				workflow.setProcessingAccountNumber(overdrafts.get(0).getOverdraftAccount().getAccountNumber());
				workflow.getProcessedInstructions().add(overdrafts.get(0));
				workflow.getUnprocessedInstructions().remove(0);
				workflow.setState(CommitReservationWorkflow.TRANSFER_FROM_OVERDRAFT_PROTECTION);
			} else {
				// return to the original account and process the commit as an overdraft of the account
				workflow.setProcessingAccountNumber(request.getAccountNumber());
				workflow.setState(CommitReservationWorkflow.OVERDRAFT_ACCOUNT);
			}

			if (log.isDebugEnabled()) {
				log.debug("Insufficient funds. {} requested. {} balance. Protect against overdraft {}/{} Overdraft records processed: {}",
						request.getTransactionAmount(), currentBalance.getBalance(), request.isProtectAgainstOverdraft(), 
						workflow.getAccount().isProtectAgainstOverdraft(), workflow.getProcessedInstructions().size() );
			}
			
			newBalance = new BalanceLog( currentBalance.getAccountNumber(), rejection.getTransactionUuid(), currentBalance.getBalance());

		} else {
			// Initiate a transfer to the original account			
			long runningBalance = currentBalance.getBalance() + remainingCharge;

			LoggedTransaction commit = new LoggedTransaction();
			commit.setAccountNumber(workflow.getProcessingAccountNumber());
			commit.setDebitCardNumber(null);
			commit.setRequestUuid(request.getRequestUuid());
			commit.setReservationUuid(null);
			commit.setRunningBalanceAmount( runningBalance );
			commit.setTransactionAmount( remainingCharge );
			commit.setTransactionMetaDataJson(request.getJsonMetaData());
			commit.setTransactionTime(LocalDateTime.now());
			commit.setTransactionTypeCode(LoggedTransaction.TRANSFER_FROM);
			commit.setTransactionUuid(UUID.randomUUID());
			
			workflow.getResults().add(commit);
			workflow.getAccumulatedResults().add(commit);
			workflow.setErrorMessage(null);
			workflow.setState(CommitReservationWorkflow.TRANSFER_AND_COMMIT);
			workflow.setProcessingAccountNumber(request.getAccountNumber());

			log.debug("Overdraft Transfer for Commit Reservation Approved. Account ={}, New Balance = {}", 
					currentBalance.getAccountNumber(), runningBalance);

			newBalance = new BalanceLog( currentBalance.getAccountNumber(), 
					commit.getTransactionUuid(), runningBalance);
		}
		return newBalance;
	}
	
	public static BalanceLog processTransferAndCommit(CommitReservationWorkflow workflow, final BalanceLog currentBalance ) {

		CommitReservationRequest request = workflow.getRequest();
		long remainingCharge = request.getTransactionAmount() - workflow.getReservation().getTransactionAmount();
		long transferRunningBalance = currentBalance.getBalance() + Math.abs(remainingCharge);
		long commitRunningBalance = currentBalance.getBalance() + Math.abs(remainingCharge) - Math.abs(remainingCharge);

		LoggedTransaction transfer = new LoggedTransaction();
		transfer.setAccountNumber(workflow.getProcessingAccountNumber());
		transfer.setDebitCardNumber(null);
		transfer.setRequestUuid(request.getRequestUuid());
		transfer.setReservationUuid(null);
		transfer.setRunningBalanceAmount( transferRunningBalance );
		transfer.setTransactionAmount( Math.abs(remainingCharge) );
		transfer.setTransactionMetaDataJson(request.getJsonMetaData());
		transfer.setTransactionTime(LocalDateTime.now());
		transfer.setTransactionTypeCode(LoggedTransaction.TRANSFER_TO);
		transfer.setTransactionUuid(UUID.randomUUID());

		LoggedTransaction commit = new LoggedTransaction();
		commit.setAccountNumber(workflow.getProcessingAccountNumber());
		commit.setDebitCardNumber(request.getDebitCardNumber());
		commit.setRequestUuid(request.getRequestUuid());
		commit.setReservationUuid(request.getReservationUuid());
		commit.setRunningBalanceAmount( commitRunningBalance );
		commit.setTransactionAmount( remainingCharge );
		commit.setTransactionMetaDataJson(request.getJsonMetaData());
		commit.setTransactionTime(LocalDateTime.now());
		commit.setTransactionTypeCode(LoggedTransaction.RESERVATION_COMMIT);
		commit.setTransactionUuid(UUID.randomUUID());
			
		workflow.getResults().add(transfer);
		workflow.getAccumulatedResults().add(transfer);
		workflow.getResults().add(commit);
		workflow.getAccumulatedResults().add(commit);
		workflow.setErrorMessage(null);
		workflow.setState(CommitReservationWorkflow.SUCCESS);

		log.debug("Overdraft Transfer and Commit complete. Account ={}, New Balance = {}", 
				workflow.getProcessingAccountNumber(), commitRunningBalance);

		return new BalanceLog( currentBalance.getAccountNumber(), 
				commit.getTransactionUuid(), commitRunningBalance);
	}
	
	public static BalanceLog processOverDraftAccount(CommitReservationWorkflow workflow, final BalanceLog currentBalance ) {

		CommitReservationRequest request = workflow.getRequest();
		long remainingCharge = request.getTransactionAmount() - workflow.getReservation().getTransactionAmount();
		long runningBalance = currentBalance.getBalance() + remainingCharge;

		LoggedTransaction commit = new LoggedTransaction();
		commit.setAccountNumber(workflow.getProcessingAccountNumber());
		commit.setDebitCardNumber(request.getDebitCardNumber());
		commit.setRequestUuid(request.getRequestUuid());
		commit.setReservationUuid(request.getReservationUuid());
		commit.setRunningBalanceAmount( runningBalance );
		commit.setTransactionAmount( remainingCharge );
		commit.setTransactionMetaDataJson(request.getJsonMetaData());
		commit.setTransactionTime(LocalDateTime.now());
		commit.setTransactionTypeCode(LoggedTransaction.RESERVATION_COMMIT);
		commit.setTransactionUuid(UUID.randomUUID());
			
		workflow.getResults().add(commit);
		workflow.getAccumulatedResults().add(commit);
		workflow.setErrorMessage(null);
		workflow.setState(CommitReservationWorkflow.SUCCESS);

		log.debug("Overdraft Transfer and Commit complete. Account ={}, New Balance = {}", 
				workflow.getProcessingAccountNumber(), runningBalance);

		return new BalanceLog( currentBalance.getAccountNumber(), 
				commit.getTransactionUuid(), runningBalance);
	}

	public static BalanceLog processNoMatch(CommitReservationWorkflow workflow) {
		String errorMessage = String.format("No match found for reservation UUID %s", 
				workflow.getRequest().getReservationUuid()== null ? "null" : workflow.getRequest().getReservationUuid().toString());
		log.error(errorMessage);
		workflow.setErrorMessage(errorMessage);
		workflow.setState(CommitReservationWorkflow.NO_MATCH);
		return null;
	}

}
