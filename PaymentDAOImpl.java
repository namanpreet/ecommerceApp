package com.qaddoo.persistence.dao.impl;

import com.qaddoo.persistence.dao.PaymentDAO;
import com.qaddoo.persistence.dto.AccountEntry;
import com.qaddoo.persistence.dto.HandleDTO;
import com.qaddoo.persistence.dto.PaymentDTO;
import com.qaddoo.pojo.entity.TransactionAttrs;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository("paymentDAO")
public class PaymentDAOImpl implements PaymentDAO {

    private final MongoTemplate mongoTemplate;

    public PaymentDAOImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public long addPayment(PaymentDTO paymentDTO) {
        PaymentDTO payment = mongoTemplate.insert(paymentDTO);
        return payment.getId();
    }

    @Override
    public List<AccountEntry> accountEntryList(long handleId, int page) {
        Pageable pageable = PageRequest.of(page, 30);

        AggregationOperation match = Aggregation.match(new Criteria("_id").is(handleId));
        AggregationOperation unwind = Aggregation.unwind("accountEntry");
        AggregationOperation project = Aggregation.project(("accountEntry"))
                .andInclude(Aggregation.bind("isOrder", "accountEntry.isOrder"))
                .andInclude(Aggregation.bind("isPayment", "accountEntry.isPayment"))
                .andInclude(Aggregation.bind("isUdhar", "accountEntry.isUdhar"))
                .andInclude(Aggregation.bind("orderId", "accountEntry.orderId"))
                .andInclude(Aggregation.bind("paymentId", "accountEntry.paymentId"))
                .andInclude(Aggregation.bind("dateTime", "accountEntry.dateTime"))
                .andInclude(Aggregation.bind("amount", "accountEntry.amount"));

        AggregationOperation skip = Aggregation.skip((long) pageable.getPageNumber() * pageable.getPageSize());
        AggregationOperation limit = Aggregation.limit(pageable.getPageSize());

        Aggregation aggregation = Aggregation.newAggregation(match, unwind, project, skip, limit);

        return mongoTemplate.aggregate(aggregation, "handles", AccountEntry.class).getMappedResults();
    }

    @Override
    public List<TransactionAttrs> getSentPaymentsByUserId(long userId, int page) {
        Pageable pageable = PageRequest.of(page, 30);

        // stage to match payments according to given conditions
        AggregationOperation match = Aggregation.match(new Criteria("userId").is(userId)
                .orOperator(Criteria.where("isUdhar").is(false), Criteria.where("isUdhar").exists(false)));
        // stages to sort limit and skip the matched results
        AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "dateTime");
        AggregationOperation limit = Aggregation.limit(pageable.getPageSize() +
                ((long) pageable.getPageNumber() * pageable.getPageSize()));
        AggregationOperation skip = Aggregation.skip((long) pageable.getPageNumber() * pageable.getPageSize());
        // stage to fetch the store to which the payment was made
        AggregationOperation storeLookup = Aggregation.lookup("tans", "storeId", "_id", "store");
        // stage to unwind store
        AggregationOperation tanUnwind = Aggregation.unwind("store", false);
        // query to add field createdBy
        String addFieldCreatedBy = "{$addFields: {\n" +
                "        createdBy: {\n" +
                "            $arrayElemAt: [{\n" +
                "                $objectToArray: \"$store.createdBy\"\n" +
                "            }, 1]\n" +
                "        }\n" +
                "    }\n" +
                "}";
        // stage to fetch createdBy handle
        AggregationOperation createdByHandleLookUp = Aggregation.lookup("handles", "createdBy.v", "_id", "createdBy");
        // stage to unwind createdBy handle
        AggregationOperation storeOwnerUnwind = Aggregation.unwind("createdBy", false);
        // stage to fetch createdBy user
        AggregationOperation createdByUserLookup = Aggregation.lookup("users", "createdBy.userId", "_id", "createdBy");
        // stage to unwind createdBy user
        AggregationOperation usersUnwind = Aggregation.unwind("createdBy");
        // query to project the final result
        String project = "{$project: {\n" +
                "        _id: 0,\n" +
                "        date: '$dateTime',\n" +
                "        amount: '$amount',\n" +
                "        userName: '$createdBy.name',\n" +
                "        hasImage: '$store.hasImage',\n" +
                "        imageName: '$store.imageId',\n" +
                "        storeName: '$store.name',\n" +
                "    }\n" +
                "}";

        Aggregation aggregation = Aggregation.newAggregation(
                match,
                sort,
                limit,
                skip,
                storeLookup,
                tanUnwind,
                new CustomProjectAggregationOperation(addFieldCreatedBy),
                createdByHandleLookUp,
                storeOwnerUnwind,
                createdByUserLookup,
                usersUnwind,
                new CustomProjectAggregationOperation(project));

        return mongoTemplate.aggregate(aggregation, "payments", TransactionAttrs.class).getMappedResults();
    }

    @Override
    public List<TransactionAttrs> getReceivedPaymentsByUserId(long userId, int page) {
        Pageable pageable = PageRequest.of(page, 30);

        AggregationOperation match = Aggregation.match(new Criteria("userId").is(userId).andOperator(Criteria.where("isAdmin").is(true)));
        AggregationOperation tanLookup = Aggregation.lookup("tans", "tanId", "_id", "tan");
        AggregationOperation tanUnwind = Aggregation.unwind("tan");
        AggregationOperation isStoreMatch = Aggregation.match(new Criteria("tan.isStore").is(true));
        AggregationOperation paymentsLookup = Aggregation.lookup("payments", "tanId", "storeId", "payments");
        AggregationOperation paymentsUnwind = Aggregation.unwind("payments");
        AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "payments.dateTime");
        AggregationOperation limit = Aggregation.limit(pageable.getPageSize() +
                ((long) pageable.getPageNumber() * pageable.getPageSize()));
        AggregationOperation skip = Aggregation.skip((long) pageable.getPageNumber() * pageable.getPageSize());
        String matchUdhar = " {\n" +
                "    $match: {\n" +
                "        $or: [{\n" +
                "            'payments.isUdhar': false\n" +
                "        }, {\n" +
                "            'payments.isUdhar': null\n" +
                "        }]\n" +
                "    }\n" +
                "}";
        AggregationOperation userLookup = Aggregation.lookup("users", "payments.userId", "_id", "users");
        AggregationOperation usersUnwind = Aggregation.unwind("users");
        String project = " {\n" +
                "    $project: {\n" +
                "        _id: 0,\n" +
                "        date: '$payments.dateTime',\n" +
                "        amount: '$payments.amount',\n" +
                "        userName: '$users.name',\n" +
                "        hasImage: '$users.hasImage',\n" +
                "        imageName: '$users.imageName',\n" +
                "        storeName: '$tan.name',\n" +
                "    }\n" +
                "}";

        Aggregation aggregation = Aggregation.newAggregation(
                match,
                tanLookup,
                tanUnwind,
                isStoreMatch,
                paymentsLookup,
                paymentsUnwind,
                sort,
                limit,
                skip,
                new CustomProjectAggregationOperation(matchUdhar),
                userLookup,
                usersUnwind,
                new CustomProjectAggregationOperation(project)
        );

        return mongoTemplate.aggregate(aggregation, "handles", TransactionAttrs.class).getMappedResults();
    }

    @Override
    public List<TransactionAttrs> getAllPaymentsByUserId(long userId, int page) {
        Pageable pageable = PageRequest.of(page, 30);
        // stage to find admin handles of the user
        AggregationOperation handleMatch = Aggregation.match(new Criteria()
                .andOperator(Criteria.where("userId").is(userId), Criteria.where("isAdmin").is(true)));
        // query to fetch all stores where user is admin
        String storeLookupQuery = "{$lookup: {\n" +
                "  from: 'tans',\n" +
                "  let: {tanId:\"$tanId\"},\n" +
                "  pipeline: [\n" +
                "    {\n" +
                "      $match: {\n" +
                "        $expr: {\n" +
                "          $and: [\n" +
                "            {$eq:[\"$_id\",\"$$tanId\"]},\n" +
                "            {$eq:[\"$isStore\",true]}\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  as: 'store'\n" +
                "}}";
        // stage to unwind stores and only keep store handles
        AggregationOperation unwind = Aggregation.unwind("store", false);

        Aggregation intermediateAggregation = Aggregation.newAggregation(
                handleMatch,
                new CustomProjectAggregationOperation(storeLookupQuery),
                unwind
        );

        List<HandleDTO> userAdminHandles = mongoTemplate.aggregate(intermediateAggregation, "handles", HandleDTO.class).getMappedResults();

        List<Long> adminStoreIds = new ArrayList<>();

        for (HandleDTO handleDTO : userAdminHandles) {
            adminStoreIds.add(handleDTO.getTanId());
        }

        // stage to find all transactions for the user
        AggregationOperation paymentMatch = Aggregation.match(Criteria.where("isUdhar").ne(true)
                .orOperator(Criteria.where("userId").is(userId), Criteria.where("storeId").in(adminStoreIds)));
        // stage to sort payments in descending order of dateTime
        AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "dateTime");
        // stage to limit and skip documents according to pagination parameters
        AggregationOperation limit = Aggregation.limit(pageable.getPageSize() +
                ((long) pageable.getPageNumber() * pageable.getPageSize()));
        AggregationOperation skip = Aggregation.skip((long) pageable.getPageNumber() * pageable.getPageSize());
        // stage to fetch store in which payment is done
        AggregationOperation storeLookup = Aggregation.lookup("tans", "storeId", "_id", "store");
        // stage to unwind stores
        AggregationOperation storeUnwind = Aggregation.unwind("store", false);
        // query to add fields "sent" and "storeOwner"
        String addFieldQuery = "{$addFields: {\n" +
                "  sent: {\n" +
                "    $cond: {\n" +
                "      if: {$eq:[\"$userId\"," + userId + "]},\n" +
                "      then: true,\n" +
                "      else: false\n" +
                "    }\n" +
                "  },\n" +
                "  storeOwner: {\n" +
                "    $arrayElemAt: [{\n" +
                "      $objectToArray: \"$store.createdBy\"\n" +
                "    },1]\n" +
                "  }\n" +
                "}}";
        // stage to lookup storeOwner handle
        AggregationOperation ownerHandleLookup = Aggregation.lookup("handles", "storeOwner.v", "_id", "storeOwner");
        // stage to unwind storeOwner
        AggregationOperation storeOwnerUnwind = Aggregation.unwind("storeOwner", false);
        // query to fetch user who made the payment
        String userLookupQuery = "{$lookup: {\n" +
                "  from: 'users',\n" +
                "  let: {userId: {\n" +
                "    $cond: {\n" +
                "      if: {$eq:[\"$sent\",true]},\n" +
                "      then: \"$storeOwner.userId\",\n" +
                "      else: \"$userId\"\n" +
                "    }\n" +
                "  }},\n" +
                "  pipeline: [\n" +
                "    {\n" +
                "      $match: {\n" +
                "        $expr: {\n" +
                "          $eq:[\"$_id\",\"$$userId\"]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  as: 'user'\n" +
                "}}";
        // stage to unwind users
        AggregationOperation userUnwind = Aggregation.unwind("user", false);
        // query to project the final fields
        String projectQuery = "{$project: {\n" +
                "  _id: 0,\n" +
                "  date: \"$dateTime\",\n" +
                "  currency: \"$store.currency\",\n" +
                "  userName: \"$user.name\",\n" +
                "  storeName: \"$store.name\",\n" +
                "  amount: 1,\n" +
                "  sent: 1,\n" +
                "  hasImage: {\n" +
                "    $cond: {\n" +
                "      if: {$eq:[\"$sent\",true]},\n" +
                "      then: \"$store.hasImage\",\n" +
                "      else: \"$user.hasImage\"\n" +
                "    }\n" +
                "  },\n" +
                "  imageName: {\n" +
                "    $cond: {\n" +
                "      if: {$eq:[\"$sent\",true]},\n" +
                "      then: \"$store.imageId\",\n" +
                "      else: \"$user.imageName\"\n" +
                "    }\n" +
                "  },\n" +
                "\n" +
                "  }}";

        Aggregation finalAggregation = Aggregation.newAggregation(
                paymentMatch,
                sort,
                limit,
                skip,
                storeLookup,
                storeUnwind,
                new CustomProjectAggregationOperation(addFieldQuery),
                ownerHandleLookup,
                storeOwnerUnwind,
                new CustomProjectAggregationOperation(userLookupQuery),
                userUnwind,
                new CustomProjectAggregationOperation(projectQuery)
        );

        return mongoTemplate.aggregate(finalAggregation, "payments", TransactionAttrs.class).getMappedResults();
    }
}
