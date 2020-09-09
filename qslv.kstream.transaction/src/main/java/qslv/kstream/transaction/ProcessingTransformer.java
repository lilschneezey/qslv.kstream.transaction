package qslv.kstream.transaction;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qslv.common.kafka.TraceableMessage;
import qslv.data.BalanceLog;
import qslv.kstream.workflow.CancelReservationWorkflow;
import qslv.kstream.workflow.CommitReservationWorkflow;
import qslv.kstream.workflow.ReservationWorkflow;
import qslv.kstream.workflow.TransactionWorkflow;
import qslv.kstream.workflow.TransferWorkflow;
import qslv.kstream.workflow.Workflow;
import qslv.kstream.workflow.WorkflowMessage;

public class ProcessingTransformer  implements ValueTransformer<TraceableMessage<WorkflowMessage>,TraceableMessage<WorkflowMessage>> {
	private static final Logger log = LoggerFactory.getLogger(ProcessingTransformer.class);
	
	ConfigProperties configProperties;
	private KeyValueStore<String, ValueAndTimestamp<BalanceLog>> balanceStore=null;
	
	public ProcessingTransformer(ConfigProperties configProperties) {
		this.configProperties = configProperties;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void init(ProcessorContext context) {
		this.balanceStore = (KeyValueStore<String, ValueAndTimestamp<BalanceLog>>) context.getStateStore(configProperties.getBalanceStoreName());
	}
	
	@Override
	public TraceableMessage<WorkflowMessage> transform(TraceableMessage<WorkflowMessage> message) {
		log.trace("transform ENTRY");
		
		WorkflowMessage workflowMessage = message.getPayload();
		String accountNumber = workflowMessage.getWorkflow().getProcessingAccountNumber();
		BalanceLog balanceLog = this.balanceStore.get(accountNumber).value();
		if ( balanceLog == null ) {
			balanceLog = new BalanceLog(accountNumber, null, 0L);
		}

		if (workflowMessage.hasCancelReservationWorkflow()) {
			switch( workflowMessage.getWorkflow().getState() ) {	
				case CancelReservationWorkflow.NO_MATCH:
					balanceLog = CancelReservationProcessor.processNoMatch(workflowMessage.getCancelReservationWorkflow());
					break;
				case CancelReservationWorkflow.CANCEL_RESERVATION:
					balanceLog = CancelReservationProcessor.processCancel(workflowMessage.getCancelReservationWorkflow(), balanceLog);	
					break;
				default:
					processErrorState(workflowMessage.getWorkflow());
			}
			
		} else if (workflowMessage.hasCommitReservationWorkflow()) {
			switch( workflowMessage.getWorkflow().getState() ) {	
				case CommitReservationWorkflow.NO_MATCH:
					balanceLog = CommitReservationProcessor.processNoMatch(workflowMessage.getCommitReservationWorkflow());
					break;
				case CommitReservationWorkflow.COMMIT_START:
					balanceLog = CommitReservationProcessor.processCommitStart(workflowMessage.getCommitReservationWorkflow(), balanceLog);
					break;	
				case CommitReservationWorkflow.TRANSFER_FROM_OVERDRAFT_PROTECTION:
					balanceLog = CommitReservationProcessor.processTransferFromOverdraft(workflowMessage.getCommitReservationWorkflow(), balanceLog);
					break;
				case CommitReservationWorkflow.TRANSFER_AND_COMMIT:
					balanceLog = CommitReservationProcessor.processTransferAndCommit(workflowMessage.getCommitReservationWorkflow(), balanceLog);
					break;
				case CommitReservationWorkflow.OVERDRAFT_ACCOUNT:
					balanceLog = CommitReservationProcessor.processOverDraftAccount(workflowMessage.getCommitReservationWorkflow(), balanceLog);
					break;
				default:
					processErrorState(workflowMessage.getWorkflow());
			}
			
		} else if (workflowMessage.hasReservationWorkflow()) {
			if (workflowMessage.getWorkflow().getState() == ReservationWorkflow.ACQUIRE_RESERVATION ) {
				balanceLog = ReservationProcessor.processReservation( workflowMessage.getReservationWorkflow(), balanceLog );
			} else {
				processErrorState( workflowMessage.getWorkflow());
			}
			
		} else if (workflowMessage.hasTransferWorkflow()) {
			switch( workflowMessage.getWorkflow().getState() ) {
				case TransferWorkflow.START_TRANSFER_FUNDS:
					balanceLog = TransferProcessor.processStartTransfer(workflowMessage.getTransferWorkflow(), balanceLog);	
					break;
				case TransferWorkflow.TRANSFER_FUNDS:
					balanceLog = TransferProcessor.processTransfer(workflowMessage.getTransferWorkflow(), balanceLog);	
					break;
				default:
					processErrorState(workflowMessage.getWorkflow());
			}

			
		} else { //Transaction Workflow assumed.

			switch( workflowMessage.getWorkflow().getState() ) {	
				case TransactionWorkflow.TRANSACT_START:
					balanceLog = TransactionProcessor.processTransactStart(workflowMessage.getTransactionWorkflow(), balanceLog);
					break;
				case TransactionWorkflow.TRANSFER_FROM_OVERDRAFT_PROTECTION:
					balanceLog = TransactionProcessor.processTransferFromOverdraft(workflowMessage.getTransactionWorkflow(), balanceLog);
					break;
				case TransactionWorkflow.TRANSFER_AND_TRANSACT:
					balanceLog = TransactionProcessor.processTransferAndTransact(workflowMessage.getTransactionWorkflow(), balanceLog);
					break;
				case TransactionWorkflow.OVERDRAFT_ACCOUNT:
					balanceLog = TransactionProcessor.processOverDraftAccount(workflowMessage.getTransactionWorkflow(), balanceLog);
					break;
				default:
					processErrorState(workflowMessage.getWorkflow());
			}
		}
		
		if (balanceLog != null) {
			this.balanceStore.put(accountNumber, ValueAndTimestamp.make(balanceLog,System.currentTimeMillis()) );
		}
		log.trace("transform EXIT");
		return message;
	}
	
	private void processErrorState( Workflow workflow ) {
		String errorMessage = String.format("Invalid state encountered. state=%d, workflow=%s", 
				workflow.getState(), workflow.getClass().getCanonicalName());
		workflow.setErrorMessage(errorMessage);
		workflow.setState(Workflow.FAILURE);
	}

	@Override
	public void close() {
		return;
	}

}
