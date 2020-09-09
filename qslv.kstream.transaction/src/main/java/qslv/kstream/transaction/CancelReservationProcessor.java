package qslv.kstream.transaction;

import java.time.LocalDateTime;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import qslv.data.BalanceLog;
import qslv.kstream.CancelReservationRequest;
import qslv.kstream.LoggedTransaction;
import qslv.kstream.workflow.CancelReservationWorkflow;

public class CancelReservationProcessor {
	private static final Logger log = LoggerFactory.getLogger(CancelReservationProcessor.class);

	public static BalanceLog processCancel(CancelReservationWorkflow workflow, final BalanceLog currentBalance ) {

		CancelReservationRequest request = workflow.getRequest();
		BalanceLog newBalance = null;
		boolean reservationMatched = workflow.getReservation() != null 
				&& request.getReservationUuid().equals( workflow.getReservation().getTransactionUuid() );

		if ( reservationMatched ) {
			// Cancel
			long runningBalance = currentBalance.getBalance() - workflow.getReservation().getTransactionAmount();

			LoggedTransaction cancellation = new LoggedTransaction();
			cancellation.setAccountNumber(request.getAccountNumber());
			cancellation.setDebitCardNumber(workflow.getReservation().getDebitCardNumber());
			cancellation.setRequestUuid(request.getRequestUuid());
			cancellation.setReservationUuid(workflow.getReservation().getTransactionUuid());
			cancellation.setRunningBalanceAmount(runningBalance);
			cancellation.setTransactionAmount( 0L - workflow.getReservation().getTransactionAmount() );
			cancellation.setTransactionMetaDataJson(request.getJsonMetaData());
			cancellation.setTransactionTime(LocalDateTime.now());
			cancellation.setTransactionTypeCode(LoggedTransaction.RESERVATION_CANCEL);
			cancellation.setTransactionUuid(UUID.randomUUID());
			
			workflow.getResults().add(cancellation);
			workflow.getAccumulatedResults().add(cancellation);
			workflow.setState(CancelReservationWorkflow.SUCCESS);
			workflow.setErrorMessage(null);

			log.debug("Cancel Reservation Approved. Account ={}, New Balance = {}", 
					currentBalance.getAccountNumber(), runningBalance);

			newBalance = new BalanceLog( currentBalance.getAccountNumber(), 
					cancellation.getTransactionUuid(), runningBalance);			
			
		} else {

			String errorMessage = String.format("Reservation not found. {}", request.getReservationUuid());
			workflow.setErrorMessage(errorMessage);
			workflow.setState(CancelReservationWorkflow.NO_MATCH);
			
		}

		return newBalance;
	}

	public static BalanceLog processNoMatch(CancelReservationWorkflow workflow) {
		String errorMessage = String.format("No match found for reservation UUID %s", 
				workflow.getRequest().getReservationUuid() == null ? "null" : workflow.getRequest().getReservationUuid().toString());
		log.error(errorMessage);
		workflow.setErrorMessage(errorMessage);
		workflow.setState(CancelReservationWorkflow.NO_MATCH);
		return null;
	}
}
