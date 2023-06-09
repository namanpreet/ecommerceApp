package com.qaddoo.qaddooanalytics.repository;

import com.qaddoo.qaddooanalytics.model.*;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public class SupportDashboardDAO {

    private final MongoTemplate mongoTemplate;

    public SupportDashboardDAO(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }


    public SupportAttrs getSupportDetails() {

        SupportAttrs supportAttrs = new SupportAttrs();

        List<ComplaintDTO> complaintDTOList = mongoTemplate.findAll(ComplaintDTO.class);
        supportAttrs.setTotalNumberOfIssues(complaintDTOList.size());

        List<ComplaintDTO> complaintDTOResolvedList = mongoTemplate.find(new Query(new Criteria().orOperator(
                Criteria.where("complaintStatus").is("COMPLETED"),
                Criteria.where("complaintStatus").is("CLOSE"),
                Criteria.where("complaintStatus").is("RESOLVED"))), ComplaintDTO.class);
        supportAttrs.setResolvedIssues(complaintDTOResolvedList.size());


        List<ComplaintDTO> complaintDTOUnResolvedList = mongoTemplate.find(new Query(new Criteria().orOperator(
                Criteria.where("complaintStatus").is("UNRESOLVED"),
                Criteria.where("complaintStatus").is("OPEN"),
                Criteria.where("complaintStatus").is("open"),
                Criteria.where("complaintStatus").is("WAITING"),
                Criteria.where("complaintStatus").is("REOPEN"))), ComplaintDTO.class);

        supportAttrs.setUnResolvedIssues(complaintDTOUnResolvedList.size());


        return supportAttrs;
    }

}
