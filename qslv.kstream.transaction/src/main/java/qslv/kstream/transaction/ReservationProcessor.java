package qslv.kstream.transaction;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import qslv.data.BalanceLog;
import qslv.data.OverdraftInstruction;
import qslv.kstream.LoggedTransaction;
import qslv.kstream.ReservationRequest;
import qslv.kstream.workflow.ReservationWorkflow;

public class ReservationProcessor {
	private static final Logger log = LoggerFactory.getLogger(ReservationProcessor.class);

	public static BalanceLog processReservation(ReservationWorkflow workflow, final BalanceLog currentBalance ) {
		
		ReservationRequest request = workflow.getRequest();
		ArrayList<OverdraftInstruction> overdrafts = workflow.getUnprocessedInstructions();
		ArrayList<OverdraftInstruction> processed = workflow.getProcessedInstructions();
		boolean insufficientFunds = request.getTransactionAmount() < 0L 
				&&	Math.abs(request.getTransactionAmount()) > currentBalance.getBalance();
		boolean canOverdraft = request.isProtectAgainstOverdraft()
				&& workflow.getAccount().isProtectAgainstOverdraft()
				&& overdrafts != null
				&& overdrafts.size() > 0;
		BalanceLog newBalance = null;

		if ( insufficientFunds ) {
			
			// Reject and try to acquire a reservation from an Overdraft account 
			LoggedTransaction rejection = new LoggedTransaction();
			rejection.setAccountNumber(workflow.getProcessingAccountNumber());
			rejection.setDebitCardNumber(request.getDebitCardNumber());
			rejection.setRequestUuid(request.getRequestUuid());
			rejection.setReservationUuid(null);
			rejection.setRunningBalanceAmount(currentBalance.getBalance());
			rejection.setTransactionAmount(request.getTransactionAmount());
			rejection.setTransactionMetaDataJson(request.getJsonMetaData());
			rejection.setTransactionTime(LocalDateTime.now());
			rejection.setTransactionTypeCode(LoggedTransaction.REJECTED_TRANSACTION);
			rejection.setTransactionUuid(UUID.randomUUID());
			
			workflow.getResults().add(rejection);
			workflow.getAccumulatedResults().add(rejection);
			newBalance = new BalanceLog( currentBalance.getAccountNumber(), rejection.getTransactionUuid(), currentBalance.getBalance());

			if ( canOverdraft ) {
				if (log.isDebugEnabled()) {
					log.debug("Insufficient funds. {} requested. {} balance. Protect against overdraft {}. "
							+ "Overdraft records processed {}. Next OD account {}.",
							request.getTransactionAmount(), currentBalance.getBalance(), 
							request.isProtectAgainstOverdraft(),  processed==null ? 0 : processed.size(),
							overdrafts.get(0).getOverdraftAccount().getAccountNumber() );
				}
				workflow.setProcessingAccountNumber(overdrafts.get(0).getOverdraftAccount().getAccountNumber());
				if (processed == null ) {
					processed = new ArrayList<>();
					workflow.setProcessedInstructions(processed);
				}
				processed.add(overdrafts.get(0));
				overdrafts.remove(0);
				workflow.setState(ReservationWorkflow.ACQUIRE_RESERVATION);
			} else {
				String errorMessage = String.format("Insufficient funds. %d requested. %d balance. "
						+ "Protect against overdraft %s/%s. %d Overdraft records processed.",
						request.getTransactionAmount(), currentBalance.getBalance(), request.isProtectAgainstOverdraft(),  
						workflow.getAccount().isProtectAgainstOverdraft(), ( processed==null ? 0 : processed.size()) );
				log.debug(errorMessage);
				workflow.setErrorMessage(errorMessage);
				workflow.setState(ReservationWorkflow.INSUFFICIENT_FUNDS);
			}
		} else {
			
			// Reservation Approved
			long runningBalance = currentBalance.getBalance() + request.getTransactionAmount();
			LoggedTransaction reservation = new LoggedTransaction();
			reservation.setAccountNumber(workflow.getProcessingAccountNumber());
			reservation.setDebitCardNumber(request.getDebitCardNumber());
			reservation.setRequestUuid(request.getRequestUuid());
			reservation.setReservationUuid(null);
			reservation.setRunningBalanceAmount(runningBalance);
			reservation.setTransactionAmount(request.getTransactionAmount());
			reservation.setTransactionMetaDataJson(request.getJsonMetaData());
			reservation.setTransactionTime(LocalDateTime.now());
			reservation.setTransactionTypeCode(LoggedTransaction.RESERVATION);
			reservation.setTransactionUuid(UUID.randomUUID());

			workflow.getResults().add(reservation);
			workflow.getAccumulatedResults().add(reservation);
			workflow.setErrorMessage(null);
			workflow.setState(ReservationWorkflow.SUCCESS);

			log.debug("Reservation Approved. Account ={}, New Balance = {}", 
					currentBalance.getAccountNumber(), runningBalance);

			newBalance = new BalanceLog( currentBalance.getAccountNumber(), 
					reservation.getTransactionUuid(), runningBalance);			
		}
		return newBalance;
	}
	
}
