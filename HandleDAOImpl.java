package com.qaddoo.persistence.dao.impl;

import com.qaddoo.persistence.dao.HandleDAO;
import com.qaddoo.persistence.dto.HandleDTO;
import com.qaddoo.persistence.dto.MessageDTO;
import com.qaddoo.pojo.entity.PaymentAttrs;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Repository("handleDAO")
public class HandleDAOImpl implements HandleDAO {

    private final MongoTemplate mongoTemplate;

    public HandleDAOImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public long addHandle(HandleDTO handleDTO) {
        HandleDTO handle = mongoTemplate.insert(handleDTO);
        return handle.getId();
    }

    @Override
    public HandleDTO getHandleByUserAndTan(long userId, long tanId) {
        return mongoTemplate.findOne(new Query(Criteria.where("userId").is(userId)
                .andOperator(Criteria.where("tanId").is(tanId))), HandleDTO.class);
    }

    @Override
    public HandleDTO getHandleByName(String handleName) {
        return mongoTemplate.findOne(new Query(Criteria.where("name").is(handleName)), HandleDTO.class);
    }

    @Override
    public List<HandleDTO> getHandlesByTan(long tanId, long userId) {
        return mongoTemplate.find(new Query(Criteria.where("tanId").is(tanId)
                .andOperator(Criteria.where("userId").ne(userId))), HandleDTO.class);
    }

    @Override
    public List<HandleDTO> getUserListOfRepliesInForum(long forumId) {
        List<MessageDTO> messageDTOList = mongoTemplate.find(new Query(Criteria.where("forumId").is(forumId)), MessageDTO.class);
        Set<HandleDTO> handleDTOSet = new HashSet<>();

        for (MessageDTO messageDTO : messageDTOList) {
            handleDTOSet.add(messageDTO.getCreatedBy());
        }

        return new ArrayList<>(handleDTOSet);
    }

    @Override
    public HandleDTO updateHandle(HandleDTO handleDTO) {
        return mongoTemplate.findAndReplace(new Query(Criteria.where("_id").is(handleDTO.getId())), handleDTO);
    }

    @Override
    public List<HandleDTO> getTanAdmins(long tanId) {
        return mongoTemplate.find(new Query(Criteria.where("tanId").is(tanId)
                .andOperator(Criteria.where("isAdmin").is(true))), HandleDTO.class);
    }

    @Override
    public List<PaymentAttrs> getUsersOverpaid(long storeId) {
        //adding Field for isOverPaid
        String addFieldDue = "{\n" +
                "    $addFields: {\n" +
                "        isOverPaid: {\n" +
                "            $gt: [\n" +
                "                '$balance',\n" +
                "                '0.0'\n" +
                "            ]\n" +
                "        }\n" +
                "    }\n" +
                "}";
        String match = "{\n" +
                "    $match: {\n" +
                "        $and: [{\n" +
                "                $and: [{\n" +
                "                        tanId: " + storeId + "\n" +
                "                    },\n" +
                "                    {\n" +
                "                        isOverPaid: true\n" +
                "                    }\n" +
                "                ]\n" +
                "            },\n" +
                "            {\n" +
                "                isAdmin: false\n" +
                "            }\n" +
                "        ]\n" +
                "    }\n" +
                "}";
        AggregationOperation lookupUser = Aggregation.lookup("users", "userId", "_id", "users");
        String sort = "{\n" +
                "    $sort: {\n" +
                "        'accountEntry.dateTime': -1\n" +
                "    }\n" +
                "}";
        AggregationOperation lookupUserUnwind = Aggregation.unwind("users");
        String project = " {\n" +
                "    $project: {\n" +
                "        userId: '$users._id',\n" +
                "        userName: '$users.name',\n" +
                "        userPhone: '$users.phone',\n" +
                "        userHasImage: '$users.hasImage',\n" +
                "        userImageName: '$users.imageName',\n" +
                "        balance: '$balance',\n" +
                "        storeId: '$tanId'\n" +
                "    }\n" +
                "}";
        Aggregation aggregation = Aggregation.newAggregation(
                new CustomProjectAggregationOperation(addFieldDue),
                new CustomProjectAggregationOperation(match),
                lookupUser,
                new CustomProjectAggregationOperation(sort),
                lookupUserUnwind,
                new CustomProjectAggregationOperation(project)
        );
        return mongoTemplate.aggregate(aggregation, "handles", PaymentAttrs.class).getMappedResults();

    }

    @Override
    public List<PaymentAttrs> getUsersDue(long storeId) {
        //adding Field for isDue
        String addFieldDue = "{\n" +
                "    $addFields: {\n" +
                "        isDue: {\n" +
                "            $lt: [\n" +
                "                '$balance',\n" +
                "                '0'\n" +
                "            ]\n" +
                "        }\n" +
                "    }\n" +
                "}";
        String match = "{\n" +
                "    $match: {\n" +
                "        $and: [{\n" +
                "                $and: [{\n" +
                "                        tanId: " + storeId + "\n" +
                "                    },\n" +
                "                    {\n" +
                "                        isDue: true\n" +
                "                    }\n" +
                "                ]\n" +
                "            },\n" +
                "            {\n" +
                "                isAdmin: false\n" +
                "            }\n" +
                "        ]\n" +
                "    }\n" +
                "}";
        AggregationOperation lookupUser = Aggregation.lookup("users", "userId", "_id", "users");
        String sort = "{\n" +
                "    $sort: {\n" +
                "        'accountEntry.dateTime': -1\n" +
                "    }\n" +
                "}";
        AggregationOperation lookupUserUnwind = Aggregation.unwind("users");
        String project = " {\n" +
                "    $project: {\n" +
                "        userId: '$users._id',\n" +
                "        userName: '$users.name',\n" +
                "        userPhone: '$users.phone',\n" +
                "        userHasImage: '$users.hasImage',\n" +
                "        userImageName: '$users.imageName',\n" +
                "        balance: '$balance',\n" +
                "        storeId: '$tanId'\n" +
                "    }\n" +
                "}";
        Aggregation aggregation = Aggregation.newAggregation(
                new CustomProjectAggregationOperation(addFieldDue),
                new CustomProjectAggregationOperation(match),
                lookupUser,
                new CustomProjectAggregationOperation(sort),
                lookupUserUnwind,
                new CustomProjectAggregationOperation(project)
        );

        return mongoTemplate.aggregate(aggregation, "handles", PaymentAttrs.class).getMappedResults();
    }


    @Override
    public List<PaymentAttrs> getAllCustomers(long storeId) {
        //adding Field for isDue
        AggregationOperation match = Aggregation.match(new Criteria()
                .andOperator(Criteria.where("tanId").is(storeId),
                        Criteria.where("isAdmin").is(false)));
        AggregationOperation lookupUser = Aggregation.lookup("users", "userId", "_id", "users");
        String sort = "{\n" +
                "    $sort: {\n" +
                "        'accountEntry.dateTime': -1\n" +
                "    }\n" +
                "}";
        AggregationOperation lookupUserUnwind = Aggregation.unwind("users");
        String project = " {\n" +
                "    $project: {\n" +
                "        userId: '$users._id',\n" +
                "        userName: '$users.name',\n" +
                "        userPhone: '$users.phone',\n" +
                "        userHasImage: '$users.hasImage',\n" +
                "        userImageName: '$users.imageName',\n" +
                "        balance: '$balance',\n" +
                "        storeId: '$tanId'\n" +
                "    }\n" +
                "}";
        Aggregation aggregation = Aggregation.newAggregation(
                match,
                lookupUser,
                new CustomProjectAggregationOperation(sort),
                lookupUserUnwind,
                new CustomProjectAggregationOperation(project)
        );
        return mongoTemplate.aggregate(aggregation, "handles", PaymentAttrs.class).getMappedResults();
    }

    @Override
    public List<HandleDTO> getAdminHandlesByUser(long userId) {
        return mongoTemplate.find(new Query(Criteria.where("userId").is(userId)
                .andOperator(Criteria.where("isAdmin").is(true))), HandleDTO.class);
    }

    @Override
    public HandleDTO markFavourite(long handleId, boolean isFavourite) {
        Query query = new Query(Criteria.where("_id").is(handleId));
        UpdateDefinition updateDefinition = new Update().set("isFavourite", isFavourite);
        FindAndModifyOptions options = FindAndModifyOptions.options().returnNew(true);
        return mongoTemplate.findAndModify(query, updateDefinition, options, HandleDTO.class);
    }
}
