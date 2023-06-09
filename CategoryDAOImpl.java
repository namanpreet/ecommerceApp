package com.qaddoo.persistence.dao.impl;

import com.qaddoo.persistence.dao.CategoryDAO;
import com.qaddoo.persistence.dto.CategoryDTO;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class CategoryDAOImpl implements CategoryDAO {

    private final MongoTemplate mongoTemplate;

    public CategoryDAOImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public CategoryDTO addCategory(CategoryDTO categoryDTO) {
        return mongoTemplate.insert(categoryDTO);
    }

    @Override
    public List<CategoryDTO> listCategories() {
        return mongoTemplate.findAll(CategoryDTO.class);
    }

    @Override
    public CategoryDTO getCategoryById(long id) {
        return mongoTemplate.findOne(new Query(Criteria.where("_id").is(id)), CategoryDTO.class);
    }

}
