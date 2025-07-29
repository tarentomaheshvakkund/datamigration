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
import org.neo4j.driver.v1.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import com.opencsv.CSVReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Service
public class DataMigrationServiceImpl implements DataMigrationService {

    private static final Logger logger = LoggerFactory.getLogger(DataMigrationServiceImpl.class);

    @Autowired
    private Driver neo4jDriver;

    @Autowired
    private CassandraOperation cassandraOperation;

    private static final int BATCH_SIZE = 4000;
    private static final int THREAD_POOL_SIZE = 10; 

    @Override
    public Response onBoardNewUsers(MultipartFile file) {
        List<List<String>> userIdBatches = null;
        try {
            userIdBatches = streamUserIdsInBatches(file, BATCH_SIZE);
        } catch (Exception e) {
            logger.error("Error reading user IDs from file: {}", e.getMessage(), e);
            return null;
        }
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        List<CompletableFuture<Void>> futures = userIdBatches.stream()
                .map(userIds -> CompletableFuture.runAsync(() -> processUserBatchOptimized(userIds), executor))
                .collect(Collectors.toList());
        futures.forEach(CompletableFuture::join);
        executor.shutdown();
        logger.info("All batches processed.");
        return null;
    }

    public void processUserBatchOptimized(List<String> userIds) {
        logger.info("Starting processing batch of {} user IDs", userIds.size());
        List<Map<String, Object>> userInfoList = fetchUserInfo(userIds);
        Map<String, List<String>> userIdToRoles = fetchUserRoles(userIds);
        List<Map<String, Object>> neo4jUpdates = buildNeo4jUpdates(userInfoList, userIdToRoles);
        bulkUpdateNeo4j(neo4jUpdates);
        logger.info("Finished processing batch of {} user IDs", userIds.size());
    }

    private List<Map<String, Object>> fetchUserInfo(List<String> userIds) {
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.ID, userIds);
        return cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD, Constants.TABLE_USER, propertyMap,
                Arrays.asList("id", "rootorgid", "profiledetails", "roles")
        );
    }

    private Map<String, List<String>> fetchUserRoles(List<String> userIds) {
        Map<String, Object> roleQueryMap = new HashMap<>();
        roleQueryMap.put("userid", userIds);
        List<Map<String, Object>> allRoles = cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD, "user_roles", roleQueryMap,
                Arrays.asList("userid", "role", "scope")
        );
        ObjectMapper mapper = new ObjectMapper();
        Map<String, List<String>> userIdToRoles = new HashMap<>();
        for (Map<String, Object> roleRecord : allRoles) {
            String userId = (String) roleRecord.get("userid");
            String role = (String) roleRecord.get("role");
            Object scopeObj = roleRecord.get("scope");
            try {
                if (scopeObj instanceof String && !((String) scopeObj).trim().isEmpty()) {
                    mapper.readValue((String) scopeObj, new TypeReference<List<Map<String, Object>>>() {});
                }
            } catch (Exception e) {
                logger.warn("Failed to parse scope JSON for userId {}: {}", userId, e.getMessage());
                continue;
            }
            userIdToRoles.computeIfAbsent(userId, k -> new ArrayList<>()).add(role);
        }
        return userIdToRoles;
    }

    private List<Map<String, Object>> buildNeo4jUpdates(List<Map<String, Object>> userInfoList, Map<String, List<String>> userIdToRoles) {
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> neo4jUpdates = new ArrayList<>();
        for (Map<String, Object> userInfo : userInfoList) {
            String userId = (String) userInfo.get("id");
            String rootOrgId = (String) userInfo.get("rootorgid");
            List<String> roles = userIdToRoles.getOrDefault(userId, Collections.emptyList());
            boolean missingIdOrOrg = StringUtils.isEmpty(userId) || StringUtils.isEmpty(rootOrgId);
            boolean missingRoles = CollectionUtils.isEmpty(roles);
            if (missingIdOrOrg || missingRoles) {
                if (missingIdOrOrg) {
                    logger.warn("User is missing ID or rootOrgId: {}", userInfo);
                }
                if (!missingIdOrOrg && missingRoles) {
                    logger.warn("Skipping user {} due to missing roles", userId);
                }
                continue;
            }
            String profileDetailsJson = (String) userInfo.get("profiledetails");
            String designation = null;
            if (StringUtils.isNotEmpty(profileDetailsJson)) {
                designation = extractDesignation(profileDetailsJson, mapper, userId);
            }
            Map<String, Object> updateObj = new HashMap<>();
            updateObj.put("userId", userId);
            updateObj.put("organisationId", rootOrgId);
            updateObj.put("designation", designation);
            updateObj.put("role", roles);
            neo4jUpdates.add(updateObj);
        }
        return neo4jUpdates;
    }

    private void bulkUpdateNeo4j(List<Map<String, Object>> neo4jUpdates) {
        if (neo4jUpdates.isEmpty()) return;
        try (Session session = neo4jDriver.session(); Transaction tx = session.beginTransaction()) {
            String query = "UNWIND $users AS user " +
                    "MERGE (u:userV3_testing {userId: user.userId}) " +
                    "SET u.organisationId = user.organisationId, " +
                    "u.designation = user.designation, " +
                    "u.role = user.role";
            Map<String, Object> params = new HashMap<>();
            params.put("users", neo4jUpdates);
            tx.run(query, params);
            tx.success();
            logger.info("Bulk updated {} users in Neo4j", neo4jUpdates.size());
        } catch (Exception e) {
            logger.error("Neo4j session error: {}", e.getMessage());
        }
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

    private String extractDesignation(String profileDetailsJson, ObjectMapper mapper, String userId) {
        try {
            Map<String, Object> userProfileDetailsMap = mapper.readValue(
                    profileDetailsJson, new TypeReference<Map<String, Object>>() {});
            Object professionDetailsObj = userProfileDetailsMap.get("professionalDetails");
            if (professionDetailsObj instanceof List) {
                List<?> professionDetails = (List<?>) professionDetailsObj;
                if (!CollectionUtils.isEmpty(professionDetails)) {
                    Object first = professionDetails.get(0);
                    if (first instanceof Map) {
                        Map<?, ?> professionDetailsMap = (Map<?, ?>) first;
                        logger.debug("Extracted designation for user {}: {}", userId, professionDetailsMap.get("designation"));
                        return (String) professionDetailsMap.get("designation");
                    }
                }
            } else if (professionDetailsObj instanceof Map) {
                Map<?, ?> professionDetailsMap = (Map<?, ?>) professionDetailsObj;
                logger.debug("Extracted designation for user {}: {}", userId, professionDetailsMap.get("designation"));
                return (String) professionDetailsMap.get("designation");
            }
        } catch (IOException e) {
            logger.error("Failed to parse profile details for user {}: {}", userId, e.getMessage());
        }
        return null;
    }


    @Override
    public Response updateRelaionsUsers(MultipartFile file) {
        List<List<List<String>>> userIdBatches = null;
        try {
            userIdBatches = streamUserRelationsInBatches(file, BATCH_SIZE);
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