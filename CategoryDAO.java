package com.qaddoo.qaddooanalytics.repository;

import com.qaddoo.qaddooanalytics.model.CategoryDTO;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

@Repository
public class CategoryDAO {

    private final MongoTemplate mongoTemplate;

    public CategoryDAO(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public String getCategoryName(Long categoryId) {
        CategoryDTO categoryDTO= mongoTemplate.findOne(new Query(Criteria.where("_id").is(categoryId)), CategoryDTO.class);
        return  categoryDTO.getEnglish();
    }
}
