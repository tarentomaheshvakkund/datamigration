package com.neo4j.datamigration.migration.controller;

import com.neo4j.datamigration.migration.model.Response;
import com.neo4j.datamigration.migration.service.DataMigrationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/datamigration")
public class DataMigrationController {

    @Autowired
    private DataMigrationService dataMigrationService;

    @PostMapping("/onBoardNewUsers")
    public ResponseEntity<Response> onBoardNewUsers(@RequestParam("file") MultipartFile file) {
        Response response = dataMigrationService.onBoardNewUsers(file);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @PostMapping("/updateRelationsUsers")
    public ResponseEntity<Response> updateRelaionsUsers(@RequestParam("file") MultipartFile file) {
        Response response = dataMigrationService.updateRelaionsUsers(file);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
