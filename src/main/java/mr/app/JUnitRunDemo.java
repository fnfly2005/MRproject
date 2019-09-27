package mr.app;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class JUnitRunDemo {

	public static void main(String[] args) {

		Result r1 = JUnitCore.runClasses(ConfDemo.class);
		for (Failure failure :r1.getFailures()) {
			System.out.println(failure.toString());
		}
		System.out.println("r1 run :" + r1.wasSuccessful());
		
	}

}
