package com.neo4j.datamigration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.neo4j.datamigration.cassandra.CassandraOperation;
import com.neo4j.datamigration.cassandra.CassandraOperationImpl;
import com.neo4j.datamigration.utils.Constants;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;


import java.io.*;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.poi.ss.usermodel.*;

@SpringBootApplication
public class DatamigrationApplication {

    public static void main(String[] args) {
        SpringApplication.run(DatamigrationApplication.class, args);
    }

}