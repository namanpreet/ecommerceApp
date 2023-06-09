package com.qaddoo.persistence.dao.impl;

import com.mongodb.client.result.DeleteResult;
import com.qaddoo.persistence.dao.ReactionsDAO;
import com.qaddoo.persistence.dto.ReactionsDTO;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;

@Repository
public class ReactionsDAOImpl implements ReactionsDAO {
    private final MongoTemplate mongoTemplate;

    public ReactionsDAOImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    //DONE
    @Override
    public ReactionsDTO addReaction(ReactionsDTO reactionDTO) {
        return mongoTemplate.insert(reactionDTO);
    }

    //DONE
    @Override
    public ReactionsDTO updateReaction(long forumId, long userId, int reaction, int flag, Date timestamp) {
        Query query = new Query(Criteria.where("forumId").is(forumId).andOperator(Criteria.where("userId").is(userId)));
        UpdateDefinition updateDefinition = new Update().set("reaction", reaction).set("flag", flag).set("reactedOn", timestamp);
        return mongoTemplate.findAndModify(query, updateDefinition, ReactionsDTO.class);
    }

    @Override
    public ReactionsDTO addUserIdToReactions(long forumId, String handleName, long userId) {
        Query query = new Query(Criteria.where("forumId").is(forumId).andOperator(Criteria.where("reactionBy").is(handleName)));
        UpdateDefinition updateDefinition = new Update().set("userId", userId);
        return mongoTemplate.findAndModify(query, updateDefinition, ReactionsDTO.class);
    }

    @Override
    public List<ReactionsDTO> listAllReactions() {
        return mongoTemplate.findAll(ReactionsDTO.class);
    }

    //DONE-TO BE VERIFIED
    @Override
    public long getLikeCountByForumId(long forumId) {
        int xLike = 1;
        Query query = new Query();
        query.addCriteria(
                new Criteria().andOperator(
                        Criteria.where("forumId").is(forumId),
                        Criteria.where("reaction").is(xLike)
                )
        );
        return mongoTemplate.count(query, ReactionsDTO.class);
    }

    //DONE-TO BE VERIFIED
    @Override
    public long getDislikeCountByForumId(long forumId) {
        int xDislike = 2;
        Query query = new Query();
        query.addCriteria(
                new Criteria().andOperator(
                        Criteria.where("forumId").is(forumId),
                        Criteria.where("reaction").is(xDislike)
                )
        );
        return mongoTemplate.count(query, ReactionsDTO.class);
    }

    //DONE-TO BE VERIFIED
    @Override
    public long getFlagCountByForumId(long forumId) {
        int xFlag = 1;
        Query query = new Query();
        query.addCriteria(
                new Criteria().andOperator(
                        Criteria.where("forumId").is(forumId),
                        Criteria.where("flag").is(xFlag)
                )
        );
        return mongoTemplate.count(query, ReactionsDTO.class);
    }

    @Override
    public ReactionsDTO getReactionsByHandle(long forumId, String handleName) {
        return mongoTemplate.findOne(new Query(Criteria.where("forumId").is(forumId)
                .andOperator(Criteria.where("reactionBy").is(handleName))), ReactionsDTO.class);
    }

    //DONE
    @Override
    public void deleteReactionsInForum(long forumId) {
        mongoTemplate.remove(new Query(Criteria.where("forumId").is(forumId)), ReactionsDTO.class);
    }

    @Override
    public DeleteResult removeReaction(long forumId, long userId) {
        return mongoTemplate.remove(new Query(Criteria.where("forumId").is(forumId)
                .andOperator(Criteria.where("userId").is(userId))), ReactionsDTO.class);
    }
}
