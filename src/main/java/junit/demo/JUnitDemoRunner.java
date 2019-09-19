package junit.demo;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;


public class JUnitDemoRunner {

	public static void main(String[] args) {

			System.out.println("JUnit-环境设置");
			Result result = JUnitCore.runClasses(JUnitDemo.class);
			for (Failure failure :result.getFailures()) {
				System.out.println(failure.toString());
			}
			System.out.println(result.wasSuccessful());
	}

}
