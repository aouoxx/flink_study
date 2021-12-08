package org.apache.flink.client.cli;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.junit.Test;

import java.util.List;

import static org.apache.flink.client.cli.CliFrontend.getConfigurationDirectoryFromEnv;
import static org.apache.flink.client.cli.CliFrontend.loadCustomCommandLines;

public class ConfigurationTest {

	@Test
	public void test (){
		// 1. find the configuration directory
		 String configurationDirectory = getConfigurationDirectoryFromEnv();

		// 2. load the global configuration
		 Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);


		// 3. load the custom command lines
		 List<CustomCommandLine> customCommandLines = loadCustomCommandLines(
			configuration,
			configurationDirectory);

		CliFrontend cli = new CliFrontend(
				configuration,
				customCommandLines);
		System.out.println("----");
	}
}
