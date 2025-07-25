package com.neo4j.datamigration.migration.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.neo4j.datamigration.cassandra.CassandraOperation;
import com.neo4j.datamigration.migration.model.Response;
import com.neo4j.datamigration.utils.Constants;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;
import com.opencsv.CSVReader;


@Service
public class DataMigrationServiceImpl implements DataMigrationService {

    private static final Logger logger = LoggerFactory.getLogger(DataMigrationServiceImpl.class);

    @Autowired
    private Driver neo4jDriver;

    @Autowired
    private CassandraOperation cassandraOperation;

    @Override
    public Response onBoardNewUsers(MultipartFile file) {
        List<List<String>> userIdBatches = null;
        try {
            userIdBatches = streamUserIdsInBatches(file, 1000);
        } catch (Exception e) {
            logger.error("Error reading user IDs from file: {}", e.getMessage(), e);
        }
        for (List<String> userIds : userIdBatches) {
            processUserBatch(userIds);
            System.out.println("Processed batch of user IDs: " + userIds.size());
        }
        return null;
    }

    public List<List<String>> streamUserIdsInBatches(MultipartFile file, int batchSize) throws Exception {
        List<List<String>> batches = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            String headerLine = reader.readLine();
            if (headerLine == null) throw new RuntimeException("CSV file is empty");
            String[] headers = headerLine.split(",");
            int idColumnIndex = -1;
            for (int i = 0; i < headers.length; i++) {
                if ("id".equalsIgnoreCase(headers[i].trim())) {
                    idColumnIndex = i;
                    break;
                }
            }
            if (idColumnIndex == -1) throw new RuntimeException("No 'id' column found");
            List<String> currentBatch = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length > idColumnIndex) {
                    String userId = values[idColumnIndex].trim();
                    currentBatch.add(userId);
                    if (currentBatch.size() == batchSize) {
                        batches.add(new ArrayList<>(currentBatch));
                        currentBatch.clear();
                    }
                }
            }
            if (!currentBatch.isEmpty()) {
                batches.add(currentBatch);
            }
        }
        return batches;
    }

    public void processUserBatch(List<String> userIds) {
        logger.info("Starting processing batch of {} user IDs", userIds.size());
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.ID, userIds);

        logger.info("Fetching user info from Cassandra for user IDs: {}", userIds);
        List<Map<String, Object>> userInfoList = cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD, Constants.TABLE_USER, propertyMap,
                Arrays.asList("id", "rootorgid", "profiledetails", "roles")
        );
        logger.info("Fetched {} user records from Cassandra", userInfoList.size());

        ObjectMapper mapper = new ObjectMapper();

        try (Session session = neo4jDriver.session(); Transaction tx = session.beginTransaction()) {
            for (Map<String, Object> userInfo : userInfoList) {
                logger.debug("Processing user info: {}", userInfo);
                handleUserInfo(userInfo, mapper, tx);
            }
            tx.success();
            logger.info("Committed Neo4j transaction for batch of {} users", userInfoList.size());
        } catch (Exception e) {
            logger.error("Neo4j session error: {}", e.getMessage());
        }
        logger.info("Finished processing batch of {} user IDs", userIds.size());
    }

    private void handleUserInfo(Map<String, Object> userInfo, ObjectMapper mapper, Transaction tx) {
        String userId = (String) userInfo.get("id");
        String rootOrgId = (String) userInfo.get("rootorgid");
        if (StringUtils.isEmpty(userId) || StringUtils.isEmpty(rootOrgId)) {
            logger.warn("User is missing ID or rootOrgId: {}", userInfo);
            return;
        }
        logger.debug("Fetching roles for userId: {}, rootOrgId: {}", userId, rootOrgId);
        List<String> roles = getUserRoles(userId, rootOrgId);
        userInfo.put("userRoles", roles);

        String profileDetailsJson = (String) userInfo.get("profiledetails");
        if (CollectionUtils.isNotEmpty(roles)) {
            String designation = null;
            if (StringUtils.isNotEmpty(profileDetailsJson)) {
                designation = extractDesignation(profileDetailsJson, mapper, userId);
            }
            if (CollectionUtils.isNotEmpty(roles)) {
                logger.info("Updating Neo4j for userId: {}, rootOrgId: {}, designation: {}, roles: {}", userId, rootOrgId, designation, roles);
                updateNeo4jUser(tx, userId, rootOrgId, designation, roles);
            } else {
                logger.warn("Skipping user {} due to missing required fields", userId);
            }
        }
    }

    private String extractDesignation(String profileDetailsJson, ObjectMapper mapper, String userId) {
        try {
            Map<String, Object> userProfileDetailsMap = mapper.readValue(
                    profileDetailsJson, new TypeReference<Map<String, Object>>() {});
            List<Map<String, Object>> professionDetails = (List<Map<String, Object>>) userProfileDetailsMap.get("professionalDetails");
            if (!CollectionUtils.isEmpty(professionDetails)) {
                Map<String, Object> professionDetailsMap = professionDetails.get(0);
                logger.debug("Extracted designation for user {}: {}", userId, professionDetailsMap.get("designation"));
                return (String) professionDetailsMap.get("designation");
            }
        } catch (IOException e) {
            logger.error("Failed to parse profile details for user {}: {}", userId, e.getMessage());
        }
        return null;
    }

    private void updateNeo4jUser(Transaction tx, String userId, String rootOrgId, String designation, List<String> roles) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("userId", userId);
        parameters.put("organisationId", rootOrgId);
        parameters.put("designation", designation);
        parameters.put("role", roles);

        String query = "MERGE (u:userV3 {userId: $userId}) " +
                "SET u.organisationId = $organisationId, " +
                "u.designation = $designation, " +
                "u.role = $role " +
                "RETURN u.userId";
        logger.debug("Running Neo4j query for userId: {}", userId);
        StatementResult result = tx.run(query, parameters);
        if (result.hasNext()) {
            logger.info("Added/Updated user {} to Neo4j with label userV3", result.next().get(0).asString());
        } else {
            logger.warn("No result returned for user {} in Neo4j update", userId);
        }
    }

    public List<String> getUserRoles(String userId, String rootOrgId) {
        Map<String, Object> paramMaps = new HashMap<>();
        paramMaps.put("userid", userId);
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> records = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD, "user_roles", paramMaps, Arrays.asList("role", "scope"));
        return records.stream().map(record -> {
            Object scopeObj = record.get("scope");
            List<Map<String, Object>> scopes = new ArrayList<>();
            if (scopeObj instanceof List) {
                scopes = (List<Map<String, Object>>) scopeObj;
            } else if (scopeObj instanceof String) {
                String scopeStr = (String) scopeObj;
                if (!scopeStr.trim().isEmpty()) {
                    try {
                        // Use Jackson's TypeReference
                        scopes = mapper.readValue(scopeStr, new TypeReference<List<Map<String, Object>>>() {
                        });
                    } catch (Exception e) {
                        logger.warn("Failed to parse scope JSON for userId {}: {}", userId, e.getMessage());
                        return null;
                    }
                }
            }
            if (!scopes.isEmpty() && scopes.stream().allMatch(scope -> rootOrgId.equals(scope.get(Constants.ORGANISATION_ID)))) {
                return (String) record.get("role");
            }
            return null;
        }).filter(Objects::nonNull).distinct().collect(Collectors.toList());
    }

    @Override
    public Response updateRelaionsUsers(MultipartFile file) {
        List<List<List<String>>> userIdBatches = null;
        try {
            userIdBatches = streamUserRelationsInBatches(file, 1000);
        }catch (Exception e) {
            logger.error("Error reading user relations from file: {}", e.getMessage(), e);
        }
        try (Session session = neo4jDriver.session(); Transaction tx = session.beginTransaction()) {
            processUserRelationsBatches(userIdBatches, tx);
            tx.success();
        } catch (Exception e) {
            logger.error("Neo4j session error: {}", e.getMessage());
        }
        return null;
    }

    public List<List<List<String>>> streamUserRelationsInBatches(MultipartFile file, int batchSize) throws Exception {
        List<List<List<String>>> batches = new ArrayList<>();
        try (CSVReader csvReader = new CSVReader(new InputStreamReader(file.getInputStream()))) {
            String[] header = csvReader.readNext();
            if (header == null) throw new RuntimeException("CSV file is empty");
            List<List<String>> currentBatch = new ArrayList<>();
            String[] values;
            while ((values = csvReader.readNext()) != null) {
                if (values.length >= 3) {
                    List<String> relation = Arrays.asList(
                            values[0].trim(),
                            values[1].trim(),
                            values[2].trim()
                    );
                    currentBatch.add(relation);
                    if (currentBatch.size() == batchSize) {
                        batches.add(new ArrayList<>(currentBatch));
                        currentBatch.clear();
                    }
                }
            }
            if (!currentBatch.isEmpty()) {
                batches.add(currentBatch);
            }
        }
        return batches;
    }


    public void processUserRelationsBatches(List<List<List<String>>> userIdBatches, Transaction tx) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        for (List<List<String>> batch : userIdBatches) {
            for (List<String> row : batch) {
                String userId = row.get(0);
                String relationshipTypeJson = row.get(1);
                String relationUserId = row.get(2);
                Map<String, Object> relProps = mapper.readValue(toValidJson(relationshipTypeJson), new TypeReference<Map<String, Object>>() {});
                String relQuery = "MATCH (u:userV3 {userId: $userId}), (r:userV3 {userId: $relationUserId}) " +
                        "MERGE (u)-[rel:connect]->(r) " +
                        "SET rel += $relProps";
                Map<String, Object> params = new HashMap<>();
                params.put("userId", userId);
                params.put("relationUserId", relationUserId);
                params.put("relProps", relProps);
                logger.info("Processing relation for userId: {}, relationUserId: {}, relProps: {}", userId, relationUserId, relProps);
                tx.run(relQuery, params);
            }
        }
    }

    public static String toValidJson(String input) {
        String trimmed = input.trim();
        if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        String[] pairs = trimmed.split(",");
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < pairs.length; i++) {
            String[] kv = pairs[i].split(":", 2);
            sb.append("\"").append(kv[0].trim()).append("\":");
            sb.append("\"").append(kv[1].trim()).append("\"");
            if (i < pairs.length - 1) sb.append(",");
        }
        sb.append("}");
        return sb.toString();
    }
}
