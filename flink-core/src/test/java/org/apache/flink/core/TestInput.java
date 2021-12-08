package org.apache.flink.core;

import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.NoFetchingInput;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.junit.Test;

public class TestInput {

	@Test
	public void test(){
		DataInputDeserializer dataInputDeserializer = 	new DataInputDeserializer();
		byte[] bytes = new byte[0];
		dataInputDeserializer.setBuffer(bytes);
		DataInputViewStream inputStream = new DataInputViewStream(dataInputDeserializer);
		NoFetchingInput input = new NoFetchingInput(inputStream);
		input.read();
		System.out.println("0000");
	}

}
