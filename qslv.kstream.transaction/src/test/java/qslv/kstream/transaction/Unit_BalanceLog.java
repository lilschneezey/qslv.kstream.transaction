package qslv.kstream.transaction;

import static org.junit.jupiter.api.Assertions.*;

import java.util.UUID;

import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import qslv.data.BalanceLog;
import qslv.util.Random;

@ExtendWith(MockitoExtension.class)
class Unit_BalanceLog {

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
	void test_balanceLog_success() {
    	BalanceLog input = new BalanceLog(Random.randomDigits(12), UUID.randomUUID(), Random.randomLong());
    	context.getBalanceLogTopic().pipeInput(input.getAccountNumber(), input);
    	ValueAndTimestamp<BalanceLog> vtoutput = context.getBalanceStore().get(input.getAccountNumber());
    	
    	assertNotNull( vtoutput );
    	BalanceLog output = vtoutput.value();
    	assertNotNull(output);
    	assertEquals(input.getAccountNumber(), output.getAccountNumber() );
    	assertEquals( input.getBalance(), output.getBalance());
    	assertEquals( input.getLastTransaction(), output.getLastTransaction() );
	}
}
