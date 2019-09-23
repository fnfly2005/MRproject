package io.demo;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class JUnitRunDemo {

	public static void main(String[] args) {

		Result r1 = JUnitCore.runClasses(StringTextComparisonTest.class);
		for (Failure failure :r1.getFailures()) {
			System.out.println(failure.toString());
		}
		System.out.println("r1 run :" + r1.wasSuccessful());
		

		Result r2 = JUnitCore.runClasses(SerializeDemo.class);
		for (Failure failure :r2.getFailures()) {
			System.out.println(failure.toString());
		}
		System.out.println("r2 run :" + r2.wasSuccessful());
	}

}
