package com.qaddoo.persistence.dao.impl;

import com.qaddoo.persistence.PersistenceUtils;
import com.qaddoo.persistence.dao.ForumDAO;
import com.qaddoo.persistence.dto.ForumDTO;
import com.qaddoo.pojo.entity.ForumAttrs;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;

/**
 * Created by namit on 26/2/21.
 */
@Repository
public class ForumDAOImpl implements ForumDAO {

    private final MongoTemplate mongoTemplate;

    public ForumDAOImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    //DONE
    @Override
    public long addForum(ForumDTO forumDTO) {
        ForumDTO forum = mongoTemplate.insert(forumDTO);
        return forum.getId();
    }

    //DONE
    @Override
    public ForumDTO getForumById(long forumId) {
        return mongoTemplate.findOne(new Query(Criteria.where("_id").is(forumId))
                .with(Sort.by(Sort.Direction.DESC, "lastActivityOn")), ForumDTO.class);
    }

    @Override
    public ForumAttrs getForumById(long forumId, long userId) {
        // stage to match the forum by id
        AggregationOperation match = Aggregation.match(Criteria.where("_id").is(forumId));
        // query to add field "createdBy"
        String addFieldQuery = "{$addFields: {\n" +
                "  createdBy: {\n" +
                "    $arrayElemAt:[\n" +
                "      {$objectToArray:\"$createdBy\"},1\n" +
                "    ]\n" +
                "  }\n" +
                "}}";
        // stage to fetch creator handle
        AggregationOperation handleLookup = Aggregation.lookup("handles", "createdBy.v", "_id", "createdBy");
        // stage to fetch the creator
        AggregationOperation userLookup = Aggregation.lookup("users", "createdBy.userId", "_id", "createdBy");
        // stage to unwind creator
        AggregationOperation unwind = Aggregation.unwind("createdBy", false);
        // query to fetch like reactions
        String likesLookupQuery = "{$lookup: {\n" +
                "  from: 'reactions',\n" +
                "  let: {forumId:\"$_id\"},\n" +
                "  pipeline: [\n" +
                "    {\n" +
                "      $match: {\n" +
                "        $expr: {\n" +
                "          $and: [\n" +
                "            {$eq:[\"$forumId\",\"$$forumId\"]},\n" +
                "            {$eq:[\"$reaction\",1]}\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  as: 'likes'\n" +
                "}}";
        // stage to fetch comments on the post
        AggregationOperation commentsLookup = Aggregation.lookup("messages", "_id", "forumId", "messages");
        // query to project the required fields
        String projectQuery = "{$project: {\n" +
                "  \"_id\": 0,\n" +
                "  \"forumId\": \"$_id\",\n" +
                "  \"tanId\": 1,\n" +
                "  \"chatUsername\": \"$createdBy.chatServerUserName\",\n" +
                "  \"forumTitle\": \"$title\",\n" +
                "  \"createdByHandleName\": \"$createdBy.name\",\n" +
                "  \"creatorHasImage\": \"$createdBy.hasImage\",\n" +
                "  \"creatorImageName\": \"$createdBy.imageName\",\n" +
                "  \"createdOn\": {$subtract:[\"$createdOn\",new Date(0)]},\n" +
                "  \"currentTime\": {$subtract:[new Date(),new Date(0)]},\n" +
                "  \"lastActivityTime\": {$subtract:[\"$lastActivityOn\",new Date(0)]},\n" +
                "  \"countLike\": {\n" +
                "    $cond: {\n" +
                "      if: {$isArray:\"$likes\"},\n" +
                "      then: {$size:\"$likes\"},\n" +
                "      else: 0\n" +
                "    }\n" +
                "  },\n" +
                "  \"countComment\": {$size:\"$messages\"},\n" +
                "  \"countFlag\": {\n" +
                "    $cond: {\n" +
                "      if: {$isArray:\"$flags\"},\n" +
                "      then: {$size:\"$flags\"},\n" +
                "      else: 0\n" +
                "    }\n" +
                "  },\n" +
                "  \"isLiked\": {\n" +
                "    $cond: {\n" +
                "      if: {$in:[" + userId + ",\"$likes.userId\"]},\n" +
                "      then: true,\n" +
                "      else: false\n" +
                "    }\n" +
                "  },\n" +
                "  \"hasImage\": \"$hasImage\",\n" +
                "  \"hasVideo\": \"$hasVideo\",\n" +
                "  \"imageName\": \"$imageName\"\n" +
                "}}";

        // Pass the aggregation pipeline stages, in respective order, to build the aggregation query
        Aggregation aggregation = Aggregation.newAggregation(
                match,
                new CustomProjectAggregationOperation(addFieldQuery),
                handleLookup,
                userLookup,
                unwind,
                new CustomProjectAggregationOperation(likesLookupQuery),
                commentsLookup,
                new CustomProjectAggregationOperation(projectQuery)
        );

        return mongoTemplate.aggregate(aggregation, "forums", ForumAttrs.class).getUniqueMappedResult();
    }

    //DONE
    @Override
    public void updateForum(ForumDTO forumDTO) {
        mongoTemplate.findAndReplace(new Query(Criteria.where("_id").is(forumDTO.getId())), forumDTO);
    }

    @Override
    public ForumDTO updateLastActivity(Date timestamp, long forumId) {
        return mongoTemplate.findAndModify(new Query(Criteria.where("_id").is(forumId)),
                new Update().set("lastActivityOn", timestamp), ForumDTO.class);
    }

    //DONE
    @Override
    public void deleteForum(ForumDTO forumDTO) {
        mongoTemplate.remove(new Query(Criteria.where("_id").is(forumDTO.getId())), ForumDTO.class);
    }

    //DONE-TO BE VERIFIED
    @Override
    public List<ForumDTO> listForumsInATan(long tanId) {
        return mongoTemplate.find(new Query(Criteria.where("tanId").is(tanId))
                .with(Sort.by(Sort.Direction.DESC, "lastActivityOn")).limit(100), ForumDTO.class);
    }

    @Override
    public List<ForumAttrs> listForumsInATan(long userId, long tanId, int page) {
        Pageable pageable = PageRequest.of(page, 30);

        // stage to get posts of given store/group
        AggregationOperation match = Aggregation.match(Criteria.where("tanId").is(tanId));
        // stage to sort the posts
        AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "createdOn");
        // stage to limit and skip the posts
        AggregationOperation limit = Aggregation.limit(pageable.getPageSize() +
                ((long) pageable.getPageNumber() * pageable.getPageSize()));
        AggregationOperation skip = Aggregation.skip((long) pageable.getPageNumber() * pageable.getPageSize());
        // query to add field "createdBy"
        String addFieldQuery1 = "{$addFields: {\n" +
                "  createdBy: {$arrayElemAt:[{$objectToArray:\"$createdBy\"},1]}\n" +
                "}}";
        // stage to fetch the creator handle
        AggregationOperation handleLookup = Aggregation.lookup("handles", "createdBy.v", "_id", "createdBy");
        // stage to fetch the creator
        AggregationOperation userLookup = Aggregation.lookup("users", "createdBy.userId", "_id", "createdBy");
        // stage to unwind creator
        AggregationOperation unwind = Aggregation.unwind("createdBy", false);
        // query to fetch like reactions
        String likesLookupQuery = "{$lookup: {\n" +
                "  from: 'reactions',\n" +
                "  let: {forumId:\"$_id\"},\n" +
                "  pipeline: [\n" +
                "    {\n" +
                "      $match: {\n" +
                "        $expr: {\n" +
                "          $and: [\n" +
                "            {$eq:[\"$forumId\",\"$$forumId\"]},\n" +
                "            {$eq:[\"$reaction\",1]}\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  as: 'likes'\n" +
                "}}";
        // query to fetch flags
        String flagLookupQuery = "{$lookup: {\n" +
                "  from: 'reactions',\n" +
                "  let: {forumId:\"$_id\"},\n" +
                "  pipeline: [\n" +
                "    {\n" +
                "      $match: {\n" +
                "        $expr: {\n" +
                "          $and: [\n" +
                "            {$eq:[\"$forumId\",\"$$forumId\"]},\n" +
                "            {$eq:[\"$flag\",1]}\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  as: 'flags'\n" +
                "}}";
        // query to remove posts flagged by user
        String matchQuery2 = "{$match: {\n" +
                "  $expr: {\n" +
                "    $not: {\n" +
                "      $in:[" + userId + ",\"$flags.userId\"]\n" +
                "    }\n" +
                "  }\n" +
                "}}";
        // stage to fetch the comments
        AggregationOperation commentsLookup = Aggregation.lookup("messages", "_id", "forumId", "messages");
        // query to project the required fields
        String projectQuery = "{$project: {\n" +
                "  \"_id\": 0,\n" +
                "  \"forumId\": \"$_id\",\n" +
                "  \"tanId\": 1,\n" +
                "  \"chatUsername\": \"$createdBy.chatServerUserName\",\n" +
                "  \"forumTitle\": \"$title\",\n" +
                "  \"createdByHandleName\": \"$createdBy.name\",\n" +
                "  \"creatorHasImage\": \"$createdBy.hasImage\",\n" +
                "  \"creatorImageName\": \"$createdBy.imageName\",\n" +
                "  \"createdOn\": {$subtract:[\"$createdOn\",new Date(0)]},\n" +
                "  \"currentTime\": {$subtract:[new Date(),new Date(0)]},\n" +
                "  \"lastActivityTime\": {$subtract:[\"$lastActivityOn\",new Date(0)]},\n" +
                "  \"countLike\": {\n" +
                "    $cond: {\n" +
                "      if: {$isArray:\"$likes\"},\n" +
                "      then: {$size:\"$likes\"},\n" +
                "      else: 0\n" +
                "    }\n" +
                "  },\n" +
                "  \"countComment\": {$size:\"$messages\"},\n" +
                "  \"countFlag\": {\n" +
                "    $cond: {\n" +
                "      if: {$isArray:\"$flags\"},\n" +
                "      then: {$size:\"$flags\"},\n" +
                "      else: 0\n" +
                "    }\n" +
                "  },\n" +
                "  \"isLiked\": {\n" +
                "    $cond: {\n" +
                "      if: {$in:[" + userId + ",\"$likes.userId\"]},\n" +
                "      then: true,\n" +
                "      else: false\n" +
                "    }\n" +
                "  },\n" +
                "  \"hasImage\": \"$hasImage\",\n" +
                "  \"hasVideo\": \"$hasVideo\",\n" +
                "  \"imageName\": \"$imageName\"\n" +
                "}}";

        // Pass the aggregation pipeline stages, in respective order, to build the aggregation query
        Aggregation aggregation = Aggregation.newAggregation(
                match,
                sort,
                limit,
                skip,
                new CustomProjectAggregationOperation(addFieldQuery1),
                handleLookup,
                userLookup,
                unwind,
                new CustomProjectAggregationOperation(likesLookupQuery),
                new CustomProjectAggregationOperation(flagLookupQuery),
                new CustomProjectAggregationOperation(matchQuery2),
                commentsLookup,
                new CustomProjectAggregationOperation(projectQuery)
        );

        return mongoTemplate.aggregate(aggregation, "forums", ForumAttrs.class).getMappedResults();
    }

    //DONE-TO BE VERIFIED
    @Override
    public List<ForumDTO> searchForumsInATan(long tanId, String searchString) {
        searchString = new PersistenceUtils().addBackslash(searchString);
        Query query = new Query();
        query.addCriteria(
                new Criteria().andOperator(
                        Criteria.where("tanId").is(tanId),
                        Criteria.where("title".toUpperCase()).regex(searchString)
                )
        ).with(Sort.by(Sort.Direction.DESC, "lastActivityOn"));
        return mongoTemplate.find(query, ForumDTO.class);
    }

}
