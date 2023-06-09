package com.qaddoo.persistence.dao.impl;

import com.qaddoo.persistence.dao.NotificationDAO;
import com.qaddoo.persistence.dto.NotificationDTO;
import com.qaddoo.pojo.entity.NotificationAttrs;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;

@Repository
public class NotificationDAOImpl implements NotificationDAO {

    private final MongoTemplate mongoTemplate;

    public NotificationDAOImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void addNotification(NotificationDTO notificationDTO) {
        mongoTemplate.insert(notificationDTO);
    }

    @Override
    public List<NotificationDTO> listNotificationsByUserId(long userId) {
        return mongoTemplate.find(new Query(Criteria.where("userInfo.id").is(userId)), NotificationDTO.class);
    }

    @Override
    public List<NotificationAttrs> listNotificationsByUserId(long userId, String language, int page) {
        Pageable pageable = PageRequest.of(page, 40);
        // stage to match all notifications of the user
        AggregationOperation match = Aggregation.match(Criteria.where("userInfo._id").is(userId));
        // stage to sort the notifications in descending order of creation time
        AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "createdOn");
        // stage to limit and skip documents according to pagination parameters
        AggregationOperation limit = Aggregation.limit(pageable.getPageSize() +
                ((long) pageable.getPageNumber() * pageable.getPageSize()));
        AggregationOperation skip = Aggregation.skip((long) pageable.getPageNumber() * pageable.getPageSize());
        // query to filter user's info from the "userInfo" array
        String addFieldQuery1 = "{$addFields: {\n" +
                "  \"userInfo\": {\n" +
                "    $filter: {\n" +
                "      input: \"$userInfo\",\n" +
                "      as: \"userInfo\",\n" +
                "      cond: {$eq:[\"$$userInfo._id\"," + userId + "]}\n" +
                "    }\n" +
                "  },\n" +
                "}}";
        // stage to unwind "userInfo"
        AggregationOperation unwindUserInfo = Aggregation.unwind("userInfo", false);
        // query to convert "tanId" in payload to "long"
        String addFieldQuery2 = "{$addFields: {\n" +
                "  \"userInfo.payload.tanId\": {$toLong: \"$userInfo.payload.tanId\"}\n" +
                "}}";
        // stage to fetch "tan"
        AggregationOperation tanLookup = Aggregation.lookup("tans", "userInfo.payload.tanId", "_id", "tan");
        // stage to unwind tan
        AggregationOperation unwindTan = Aggregation.unwind("tan", false);
        // query to add field "payLoadObject" and extract "handleId" from "tan.createdBy"
        String addFieldQuery3 = "{$addFields: {\n" +
                "  \"payloadObject\": \"$userInfo.payload\",\n" +
                "  \"tan.createdBy\": {$arrayElemAt:[{$objectToArray:\"$tan.createdBy\"},1]}\n" +
                "}}";
        // stage to fetch "createdByHandle"
        AggregationOperation createdByHandleLookup = Aggregation.lookup("handles", "tan.createdBy.v", "_id", "createdByHandle");
        // stage to unwind "createdByHandle"
        AggregationOperation unwindCreatedByHandle = Aggregation.unwind("createdByHandle", false);
        // stage to lookup "createdByUser"
        AggregationOperation createdByUserLookup = Aggregation.lookup("users", "createdByHandle.userId", "_id", "createdByUser");
        // stage to unwind "createdByUser"
        AggregationOperation unwindCreatedByUser = Aggregation.unwind("createdByUser", false);
        // stage to lookup "user"
        AggregationOperation userLookup = Aggregation.lookup("users", "userInfo._id", "_id", "user");
        // stage to unwind "user"
        AggregationOperation unwindUser = Aggregation.unwind("user", false);
        // query to lookup "userHandle"
        String userHandleLookupQuery = "{$lookup: {\n" +
                "  from: 'handles',\n" +
                "  let: {userId:\"$user._id\", tanId:\"$tan._id\"},\n" +
                "  pipeline: [\n" +
                "    {\n" +
                "      $match: {\n" +
                "        $expr: {\n" +
                "          $and: [\n" +
                "            {$eq:[\"$userId\",\"$$userId\"]},\n" +
                "            {$eq:[\"$tanId\",\"$$tanId\"]}\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  as: 'userHandle'\n" +
                "}}";
        // stage to unwind "userHandle"
        AggregationOperation unwindUserHandle = Aggregation.unwind("userHandle", false);
        // query to update "payloadObject"
        String addFieldQuery4 = "{$addFields: {\n" +
                "  \"payloadObject.tanHasImage\": \"$tan.hasImage\",\n" +
                "  \"payloadObject.tanId\": \"$tan._id\",\n" +
                "  \"payloadObject.tanName\": \"$tan.name\",\n" +
                "  \"payloadObject.orderId\": \"$userInfo.payload.orderId\",\n" +
                "  \"payloadObject.handleName\": \"$userHandle.name\",\n" +
                "  \"payloadObject.ownerId\": \"$createdByUser._id\",\n" +
                "  \"payloadObject.ownerHandleName\": \"$createdByHandle.name\",\n" +
                "  \"payloadObject.ownerChatServerUsername\": \"$createdByUser.chatServerUserName\",\n" +
                "  \"payloadObject.ownerUserName\": \"$createdByUser.name\",\n" +
                "  \"payloadObject.ownerHasImage\": {\n" +
                "    $ifNull:[\"$createdByUser.hasImage\",false]\n" +
                "  },\n" +
                "  \"payloadObject.ownerImageName\": \"$createdByUser.imageName\",\n" +
                "  \"payloadObject.userHasImage\": {\n" +
                "    $ifNull:[\"$user.hasImage\",false]\n" +
                "  },\n" +
                "  \"payloadObject.userImageName\": \"$user.imageName\",\n" +
                "  \"payloadObject.tanHasImage\": \"$tan.hasImage\",\n" +
                "  \"payloadObject.tanImageName\": \"$tan.imageId\",\n" +
                "  \"payloadObject.isTanAdmin\": \"$userHandle.isAdmin\"\n" +
                "}}";
        // query to project the final result
        String projectQuery = "{$project: {\n" +
                "  userId: \"$userInfo._id\",\n" +
                "  isNew: \"$userInfo.isNew\",\n" +
                "  createdOn: 1,\n" +
                "  title: {$ifNull:[\"$" + language + "Title\",\"$englishTitle\"]},\n" +
                "  message: {$ifNull:[\"$" + language + "Message\",\"$englishMessage\"]},\n" +
                "  action: 1,\n" +
                "  destination: 1,\n" +
                "  payloadObject: 1\n" +
                "}}";

        Aggregation aggregation = Aggregation.newAggregation(
                match,
                sort,
                limit,
                skip,
                new CustomProjectAggregationOperation(addFieldQuery1),
                unwindUserInfo,
                new CustomProjectAggregationOperation(addFieldQuery2),
                tanLookup,
                unwindTan,
                new CustomProjectAggregationOperation(addFieldQuery3),
                createdByHandleLookup,
                unwindCreatedByHandle,
                createdByUserLookup,
                unwindCreatedByUser,
                userLookup,
                unwindUser,
                new CustomProjectAggregationOperation(userHandleLookupQuery),
                unwindUserHandle,
                new CustomProjectAggregationOperation(addFieldQuery4),
                new CustomProjectAggregationOperation(projectQuery)
        );

        return mongoTemplate.aggregate(aggregation, "notifications", NotificationAttrs.class).getMappedResults();
    }

    @Override
    public NotificationDTO getNotificationById(long notificationId) {
        return mongoTemplate.findOne(new Query(Criteria.where("_id").is(notificationId)), NotificationDTO.class);
    }

    @Override
    public NotificationDTO getNotificationByUserIdAndDest(String tanId, long senderId, String destination) {
        return mongoTemplate.findOne(new Query(Criteria.where("destination").is(destination)
                .andOperator((Criteria.where("userInfo.payload.senderId").is(senderId))
                        .andOperator(Criteria.where("userInfo.payload.tanId").is(tanId)))), NotificationDTO.class);
    }

    @Override
    public NotificationDTO updateNotification(NotificationDTO notificationDTO) {
        return mongoTemplate.findAndReplace(new Query(Criteria.where("_id").is(notificationDTO.getId())), notificationDTO);
    }

    @Override
    public void deleteNotifications(Date dateTime) {
        mongoTemplate.remove(new Query(Criteria.where("createdOn").gt(dateTime)), NotificationDTO.class);
    }

    @Override
    public void deleteNotificationById(long notificationId) {
        mongoTemplate.remove(new Query(Criteria.where("_id").is(notificationId)), NotificationDTO.class);
    }

}
