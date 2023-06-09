package com.qaddoo.qaddooanalytics.repository;

import com.qaddoo.qaddooanalytics.model.ItemDTO;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ItemDAO {

    private final MongoTemplate mongoTemplate;

    public ItemDAO(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public long itemCount() {
        return mongoTemplate.findAll(ItemDTO.class).size();
    }
}
