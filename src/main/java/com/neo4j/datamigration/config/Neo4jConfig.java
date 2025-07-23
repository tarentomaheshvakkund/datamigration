package com.neo4j.datamigration.config;

import java.util.concurrent.TimeUnit;

import com.neo4j.datamigration.exception.GraphException;
import com.neo4j.datamigration.utils.Constants;
import com.neo4j.datamigration.utils.PropertiesCache;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.exceptions.AuthenticationException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class Neo4jConfig {

	private Logger logger = LoggerFactory.getLogger(Neo4jConfig.class);

	@Bean
	public Driver Neo4jDriver() {
		try {
			if (Boolean.parseBoolean(PropertiesCache.getInstance().getProperty(Constants.NEO4J_AUTH_ENABLED))) {
				return GraphDatabase.driver(PropertiesCache.getInstance().getProperty(Constants.NEO4J_HOST_URL),
						AuthTokens.basic(PropertiesCache.getInstance().getProperty(Constants.NEO4J_USER_NAME),
								PropertiesCache.getInstance().getProperty(Constants.NEO4J_PASSWORD)));
			} else {
				Integer timeout = Integer.parseInt(PropertiesCache.getInstance().getProperty(Constants.NEO$J_TIMEOUT));
				Config config = Config.build()
						.withConnectionTimeout(timeout, TimeUnit.SECONDS)
						.withConnectionLivenessCheckTimeout(10L, TimeUnit.SECONDS).toConfig();
				logger.info("Using timeout config of : " + timeout);
				return GraphDatabase.driver(PropertiesCache.getInstance().getProperty(Constants.NEO4J_HOST_URL),
						config);
			}
		} catch (AuthenticationException | ServiceUnavailableException e) {
			logger.error("Failed to initialize Neo4J connection. Exception: ", e);
			throw new GraphException(e.code(), e.getMessage());
		}
	}
}
