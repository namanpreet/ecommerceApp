package com.qaddoo.persistence.dao.impl;

import com.qaddoo.persistence.dao.FaqDAO;
import com.qaddoo.persistence.dto.FaqDTO;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class FaqDAOImpl implements FaqDAO {

    private final MongoTemplate mongoTemplate;

    public FaqDAOImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public FaqDTO addFaq(FaqDTO faqDTO) {
        return mongoTemplate.insert(faqDTO);
    }

    @Override
    public List<FaqDTO> listAllFaqs() {
        return mongoTemplate.findAll(FaqDTO.class);
    }

    @Override
    public List<FaqDTO> listParticularFaqs(String type) {
        return mongoTemplate.find(new Query(Criteria.where("type").is(type)), FaqDTO.class);
    }
}
