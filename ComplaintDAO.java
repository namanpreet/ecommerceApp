package com.qaddoo.qaddooanalytics.repository;

import com.qaddoo.qaddooanalytics.model.ComplaintDTO;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class ComplaintDAO {

    private final MongoTemplate mongoTemplate;

    public ComplaintDAO(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public List<ComplaintDTO> getRaisedTickets(int page, String status) {
        Pageable pageable = PageRequest.of(page, 30, Sort.by("complaintTime").descending());
        Query query = new Query().with(pageable);
        switch (status) {
            case "OPEN":
                query = new Query(new Criteria().orOperator(Criteria.where("complaintStatus").is("OPEN"),
                        Criteria.where("complaintStatus").is("REOPEN"))).with(pageable);
                break;
            case "IN_PROGRESS":
                query = new Query(new Criteria().orOperator(Criteria.where("complaintStatus").is("IN_PROGRESS"))).with(pageable);
                break;
            case "FORWARDED":
                query = new Query(new Criteria().orOperator(Criteria.where("complaintStatus").is("FORWARDED"))).with(pageable);
                break;
            case "CLOSED":
                query = new Query(new Criteria().orOperator(Criteria.where("complaintStatus").is("CLOSED"),
                        Criteria.where("complaintStatus").is("RESOLVED"))).with(pageable);
                break;
        }
        return mongoTemplate.find(query, ComplaintDTO.class);
    }

    public ComplaintDTO getTicketDetails(long id) {
        return mongoTemplate.findOne(new Query(Criteria.where("id").is(id)), ComplaintDTO.class);
    }

    public void updateTicket(ComplaintDTO complaintDTO) {
        mongoTemplate.findAndReplace(Query.query(Criteria.where("id").is(complaintDTO.getId())), complaintDTO);
    }
}
