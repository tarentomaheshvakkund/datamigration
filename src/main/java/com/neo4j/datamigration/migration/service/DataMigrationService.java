package com.neo4j.datamigration.migration.service;

import com.neo4j.datamigration.migration.model.Response;
import org.springframework.web.multipart.MultipartFile;


public interface DataMigrationService {
    Response onBoardNewUsers(MultipartFile file);

    Response updateRelaionsUsers(MultipartFile file);
}
