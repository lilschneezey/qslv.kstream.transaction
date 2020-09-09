package qslv.kstream.transaction;

import org.junit.platform.runner.JUnitPlatform;
import org.junit.platform.suite.api.IncludeClassNamePatterns;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.runner.RunWith;

@RunWith(JUnitPlatform.class)
@SelectPackages( "qslv.kstream.transaction")
@IncludeClassNamePatterns("^(Unit_.*|.+[.$]Unit_.*)$")
class UnitSuiteTransactionTest {


}
