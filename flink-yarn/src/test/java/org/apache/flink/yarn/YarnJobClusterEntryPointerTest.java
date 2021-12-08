package org.apache.flink.yarn;

import org.apache.flink.yarn.entrypoint.YarnJobClusterEntrypoint;
import org.junit.Test;

public class YarnJobClusterEntryPointerTest {

	@Test
	public void test(){
		YarnJobClusterEntrypoint.main(null);
	}

}
