package com.qaddoo.persistence.dao.impl;

import com.qaddoo.persistence.PersistenceUtils;
import com.qaddoo.persistence.dao.MessageDAO;
import com.qaddoo.persistence.dto.MessageDTO;
import com.qaddoo.pojo.entity.MessageAttrs;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.List;


@Repository
public class MessageDAOImpl implements MessageDAO {

    private final MongoTemplate mongoTemplate;

    public MessageDAOImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    //DONE
    @Override
    public long addMessage(MessageDTO msgDTO) {
        MessageDTO messageDTO = mongoTemplate.insert(msgDTO);
        return messageDTO.getId();
    }

    //DONE-TO BE VERIFIED
    @Override
    public List<MessageDTO> searchMessagesInForum(long forumId, String searchString) {
        searchString = new PersistenceUtils().addBackslash(searchString);
        Query query = new Query();
        query.addCriteria(
                new Criteria().andOperator(
                        Criteria.where("forumId").is(forumId),
                        Criteria.where("messageChar".toUpperCase()).regex(searchString)
                )
        ).with(Sort.by(Sort.Direction.DESC, "createdOn"));
        return mongoTemplate.find(query, MessageDTO.class);
    }

    @Override
    public long countMessagesInForum(long forumId) {
        return mongoTemplate.count(new Query(Criteria.where("forumId").is(forumId)), MessageDTO.class);
    }

    @Override
    public void deleteMessagesInForum(long forumId) {
        mongoTemplate.remove(new Query(Criteria.where("forumId").is(forumId)), MessageDTO.class);
    }

    @Override
    public List<MessageAttrs> listMessage(long userId, long forumId) {
        AggregationOperation match = Aggregation.match(new Criteria("forumId").is(forumId));
        String addFieldUserId = "{\n" +
                "    $addFields: {\n" +
                "        userId: " + userId + "\n" +
                "    }\n" +
                "}";
        AggregationOperation authUsersLookup = Aggregation.lookup("users", "userId", "_id", "authUsers");
        String createdBy = "{\n" +
                "    $addFields: {\n" +
                "        createdByAddField: {\n" +
                "            $arrayElemAt: [{\n" +
                "                    $objectToArray: '$createdBy'\n" +
                "                },\n" +
                "                1\n" +
                "            ]\n" +
                "        }\n" +
                "    }\n" +
                "}";
        AggregationOperation createdByHandleLookup = Aggregation.lookup("handles", "createdByAddField.v", "_id", "createdByHandle");
        AggregationOperation createdByHandleUnwind = Aggregation.unwind("createdByHandle");
        AggregationOperation authUsersByUnwind = Aggregation.unwind("authUsers");
        String isBlockedNull="{\n" +
                "    $addFields: {\n" +
                "        isBlockedNull: {\n" +
                "            $cond: {\n" +
                "                'if': {\n" +
                "                    $ifNull: [\n" +
                "                        '$authUsers.blockedUsers',\n" +
                "                        false\n" +
                "                    ]\n" +
                "                },\n" +
                "                then: false,\n" +
                "                'else': true\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
               String isBlocked= "{\n" +
                       "    $addFields: {\n" +
                       "        isBlocked: {\n" +
                       "            $cond: {\n" +
                       "                'if': {\n" +
                       "                    $eq: [\n" +
                       "                        '$isBlockedNull',\n" +
                       "                        true\n" +
                       "                    ]\n" +
                       "                },\n" +
                       "                then: 'false',\n" +
                       "                'else': {\n" +
                       "                    $in: [\n" +
                       "                        '$createdByHandle.user',\n" +
                       "                        '$authUsers.blockedUsers'\n" +
                       "                    ]\n" +
                       "                }\n" +
                       "            }\n" +
                       "        }\n" +
                       "    }\n" +
                       "}";
        AggregationOperation createdByLookup = Aggregation.lookup("users", "createdByHandle.userId", "_id", "createdBy");
        AggregationOperation createdByUnwind = Aggregation.unwind("createdBy");
        String matchBlockUser = "{\n" +
                "    $match: {\n" +
                "        $expr: {\n" +
                "            $ne: [\n" +
                "                '$isBlocked',\n" +
                "                true\n" +
                "            ]\n" +
                "        }\n" +
                "    }\n" +
                "}";
        String addFieldCurrentTime = " {\n" +
                "    $addFields: {\n" +
                "        currentTime: '$$NOW'\n" +
                "    }\n" +
                "}";
        String project = " {\n" +
                "    $project: {\n" +
                "        _id: 0,\n" +
                "        messageId: '$_id',\n" +
                "        forumId: '$forumId',\n" +
                "        postedByHandleName: '$createdBy.name',\n" +
                "        chatUsername: '$createdBy.chatServerUserName',\n" +
                "        postedAtTime: '$createdOn',\n" +
                "        currentTime: '$currentTime',\n" +
                "        message: '$messageChar',\n" +
                "        hasImage: '$hasImage',\n" +
                "        imageName: '$imageName',\n" +
                "        creatorHasImage: '$createdBy.hasImage',\n" +
                "        creatorImageName: '$createdBy.imageName'\n" +
                "    }\n" +
                "}";
        Aggregation aggregation = Aggregation.newAggregation(
                match,
                new CustomProjectAggregationOperation(addFieldUserId),
                authUsersLookup,
                new CustomProjectAggregationOperation(createdBy),
                createdByHandleLookup,
                createdByHandleUnwind,
                createdByLookup,
                createdByUnwind,
                authUsersByUnwind,
                new CustomProjectAggregationOperation(isBlockedNull),
                new CustomProjectAggregationOperation(isBlocked),
                new CustomProjectAggregationOperation(matchBlockUser),
                new CustomProjectAggregationOperation(addFieldCurrentTime),
                new CustomProjectAggregationOperation(project)
                );
        List<MessageAttrs> messages = mongoTemplate.aggregate(aggregation, "messages", MessageAttrs.class).getMappedResults();
            return messages;
    }
}
