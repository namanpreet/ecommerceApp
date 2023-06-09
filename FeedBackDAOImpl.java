package com.qaddoo.persistence.dao.impl;

import com.qaddoo.persistence.dao.FeedBackDAO;
import com.qaddoo.persistence.dto.FeedBackDTO;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;


/**
 * Created by HP on 14-12-2021.
 */
@Repository("feedBackDAO")
public class FeedBackDAOImpl implements FeedBackDAO {

    private final MongoTemplate mongoTemplate;

    public FeedBackDAOImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public FeedBackDTO addFeedBack(FeedBackDTO feedBackDTO) {
        return mongoTemplate.insert(feedBackDTO);
    }
}
