package com.qaddoo.persistence.dao.impl;

import com.google.common.base.Strings;
import com.qaddoo.persistence.PersistenceUtils;
import com.qaddoo.persistence.dao.TanDAO;
import com.qaddoo.persistence.dto.*;
import com.qaddoo.pojo.entity.ExploreMediaAttrs;
import com.qaddoo.pojo.entity.GetStoreDetailResponse;
import com.qaddoo.pojo.entity.StoreAttrs;
import com.qaddoo.pojo.entity.TanAttrs;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.geo.Metrics;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.*;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.*;

@Repository
public class TanDAOImpl implements TanDAO {

    private final MongoTemplate mongoTemplate;

    public TanDAOImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public TanDTO getTanById(long parseLong) {
        // contains parseLong because it is in the parameter
        return mongoTemplate.findOne(new Query(Criteria.where("_id").is(parseLong)), TanDTO.class);
    }

    @Override
    public long addTan(TanDTO tanDTO) {
        TanDTO tan = mongoTemplate.insert(tanDTO);
        return tan.getId();
    }

    @Override
    public void updateTan(TanDTO tanDTO) {
        mongoTemplate.findAndReplace(new Query(Criteria.where("_id").is(tanDTO.getId())), tanDTO);
    }

    @Override
    public void updateOrderList(OrderDTO orderDTO) {
        Query query = new Query(Criteria.where("_id").is(orderDTO.getStoreId()));
        Map<String, Object> order = new HashMap<>();
        order.put("$id", orderDTO.getId());
        order.put("$ref", "orders");
        UpdateDefinition updateDefinition = new Update().push("orders", order);
        mongoTemplate.findAndModify(query, updateDefinition, TanDTO.class);
    }

    @Override
    public void updateHandleList(HandleDTO handleDTO) {
        Query query = new Query(Criteria.where("_id").is(handleDTO.getTanId()));
        Map<String, Object> handle = new HashMap<>();
        handle.put("$id", handleDTO.getId());
        handle.put("$ref", "handles");
        UpdateDefinition updateDefinition = new Update().push("handles", handle);
        mongoTemplate.findAndModify(query, updateDefinition, TanDTO.class);
    }

    @Override
    public void updateOwner(HandleDTO handleDTO) {
        Query query = new Query(Criteria.where("_id").is(handleDTO.getTanId()));
        Map<String, Object> createdBy = new HashMap<>();
        createdBy.put("$id", handleDTO.getId());
        createdBy.put("$ref", "handles");
        UpdateDefinition updateDefinition = new Update().set("createdBy", createdBy);
        mongoTemplate.findAndModify(query, updateDefinition, TanDTO.class);
    }

    @Override
    public void updateLastUpdatedBy(HandleDTO handleDTO, Date timestamp) {
        Query query = new Query(Criteria.where("_id").is(handleDTO.getTanId()));
        UpdateDefinition updateDefinition;
        Map<String, Object> lastUpdatedBy = new HashMap<>();
        lastUpdatedBy.put("$id", handleDTO.getId());
        lastUpdatedBy.put("$ref", "handles");
        updateDefinition = new Update().set("lastUpdatedBy", lastUpdatedBy).set("lastUpdatedOn", timestamp);
        mongoTemplate.findAndModify(query, updateDefinition, TanDTO.class);
    }

    @Override
    public void updateAccount(BigDecimal amountDue, BigDecimal amountOverpaid, AccountEntry accountEntry, long tanId) {
        Query query = new Query(Criteria.where("_id").is(tanId));
        UpdateDefinition updateDefinition = new Update().set("amountOverpaid", amountOverpaid)
                .set("amountDue", amountDue).push("accountEntry", accountEntry);
        mongoTemplate.findAndModify(query, updateDefinition, TanDTO.class);
    }

    @Override
    public void updateTanProfile(TanDTO tanDTO) {
        Query query = new Query(Criteria.where("_id").is(tanDTO.getId()));
        Update update = new Update().set("name", tanDTO.getName()).set("description", tanDTO.getDescription()).set("latitude", tanDTO.getLatitude())
                .set("longitude", tanDTO.getLongitude()).set("location", tanDTO.getLocation()).set("categories", tanDTO.getCategories())
                .set("openDays", tanDTO.getOpenDays()).set("openingTime", tanDTO.getOpeningTime()).set("closingTime", tanDTO.getClosingTime())
                .set("hasImage", tanDTO.isHasImage()).set("imageId", tanDTO.getImageName()).set("address.addressLine1", tanDTO.getAddress().getAddressLine1())
                .set("address.addressLine2", tanDTO.getAddress().getAddressLine2()).set("address.city", tanDTO.getAddress().getCity())
                .set("address.state", tanDTO.getAddress().getState()).set("address.country", tanDTO.getAddress().getCountry())
                .set("address.pinCode", tanDTO.getAddress().getPinCode()).set("lastUpdatedOn", tanDTO.getLastUpdatedOn());
        if (Objects.nonNull(tanDTO.getHasBanner())) {
            update.set("hasBanner", tanDTO.getHasBanner());
        }
        if (!Strings.isNullOrEmpty(tanDTO.getBannerName())) {
            update.set("bannerName", tanDTO.getBannerName());
        }
        if (Objects.nonNull(tanDTO.getDeliveryTypes())) {
            update.set("deliveryTypes", tanDTO.getDeliveryTypes());
        }
        if (Objects.nonNull(tanDTO.getDeliveryRange())) {
            update.set("deliveryRange", tanDTO.getDeliveryRange());
        }
        if (Objects.nonNull(tanDTO.getPaymentOptions())) {
            update.set("paymentOptions", tanDTO.getPaymentOptions());
        }
        if (!Strings.isNullOrEmpty(tanDTO.getUpiId())) {
            update.set("upiId", tanDTO.getUpiId());
        }
        if (!Strings.isNullOrEmpty(tanDTO.getPayeeName())) {
            update.set("payeeName", tanDTO.getPayeeName());
        }
        if (!Strings.isNullOrEmpty(tanDTO.getPayeePhone())) {
            update.set("payeePhone", tanDTO.getPayeePhone());
        }
        if (!Strings.isNullOrEmpty(tanDTO.getphone())) {
            update.set("phone", tanDTO.getphone());
        }
        mongoTemplate.findAndModify(query, update, TanDTO.class);
    }

    @Override
    public void removeItems(List<ItemDTO> items, Set<String> subCategories, long storeId) {
        Query query = new Query(Criteria.where("_id").is(storeId));
        Update update = new Update().set("subCategories", subCategories).pullAll("items", items.toArray());
        mongoTemplate.findAndModify(query, update, TanDTO.class);
    }

    @Override
    public TanDTO updateCatalog(List<ItemDTO> newItems, Map<ItemDTO, Integer> updatedItems, Set<String> subCategories, long storeId) {
        Query query = new Query(Criteria.where("_id").is(storeId));
        // update the existing items
        if (!updatedItems.isEmpty()) {
            Update unsetUpdate = new Update();
            Update setUpdate = new Update();
            for (Map.Entry<ItemDTO, Integer> item : updatedItems.entrySet()) {
                unsetUpdate = unsetUpdate.unset("items." + item.getValue());
                setUpdate = setUpdate.set("items." + item.getValue(), item.getKey());
            }
            mongoTemplate.findAndModify(query, unsetUpdate, TanDTO.class);
            mongoTemplate.findAndModify(query, setUpdate, TanDTO.class);
        }
        // insert the new items
        UpdateDefinition updateDefinition;
        if (Objects.nonNull(newItems)) {
            updateDefinition = new Update().push("items").each(newItems).set("subCategories", subCategories);
        } else {
            updateDefinition = new Update().set("subCategories", subCategories);
        }

        FindAndModifyOptions options = FindAndModifyOptions.options().returnNew(true);
        return mongoTemplate.findAndModify(query, updateDefinition, options, TanDTO.class);
    }

    @Override
    public void updateAllItems(List<ItemDTO> items, long storeId) {
        Query query = new Query(Criteria.where("_id").is(storeId));
        UpdateDefinition updateDefinition = new Update().set("items", items);
        mongoTemplate.findAndModify(query, updateDefinition, TanDTO.class);
    }

    @Override
    public List<TanDTO> listAllTans() {
        return mongoTemplate.findAll(TanDTO.class);
    }

    @Override
    public List<StoreAttrs> listMyStores(long userId) {
        // stage to find admin handles of user
        AggregationOperation matchAdminHandles = Aggregation.match(new Criteria().andOperator(
                Criteria.where("userId").is(userId),
                Criteria.where("isAdmin").is(true))
        );
        // query to fetch stores in which user is admin
        String adminStoresLookupQuery = "{$lookup: {\n" +
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
                "  as: 'tan'\n" +
                "}}";
        // stage to unwind stores
        AggregationOperation unwindStores = Aggregation.unwind("tan", false);
        // query to project final result
        String projectQuery = "{$project: {\n" +
                "  _id:\"$tanId\",\n" +
                "  name:\"$tan.name\",\n" +
                "  description:{$ifNull:[\"$tan.description\",\"\"]},\n" +
                "  latitude:\"$tan.latitude\",\n" +
                "  longitude:\"$tan.longitude\",\n" +
                "  hasImage:\"$tan.hasImage\",\n" +
                "  imageName:{$ifNull:[\"$tan.imageId\",\"\"]},\n" +
                "  hasBanner:{$ifNull:[\"$tan.hasBanner\",false]},\n" +
                "  bannerName:{$ifNull:[\"$tan.bannerName\",\"\"]},\n" +
                "  deliveryTypes:{$ifNull:[\"$tan.deliveryTypes\",[]]},\n" +
                "  paymentOptions:{$ifNull:[\"$tan.paymentOptions\",[]]}\n" +
                "}}";

        Aggregation aggregation = Aggregation.newAggregation(
                matchAdminHandles,
                new CustomProjectAggregationOperation(adminStoresLookupQuery),
                unwindStores,
                new CustomProjectAggregationOperation(projectQuery)
        );

        return mongoTemplate.aggregate(aggregation, "handles", StoreAttrs.class).getMappedResults();
    }

    @Override
    public List<ItemDTO> listStoreItems(long storeId, String subCategory, String language, String name, int page) {
        Pageable pageable = PageRequest.of(page, 20);

        // stage to match the store by id
        AggregationOperation match = Aggregation.match(new Criteria("_id").is(storeId));
        // stage to unwind items
        AggregationOperation unwind = Aggregation.unwind("items");
        // stage to filter items based on search keyword or sub-category
        AggregationOperation filter = null;
        if (!Strings.isNullOrEmpty(name)) {
            name = new PersistenceUtils().addBackslash(name);
            filter = Aggregation.match(Criteria.where("items." + language).regex(".*" + name + ".*", "i"));
        } else if (!Strings.isNullOrEmpty(subCategory)) {
            subCategory = new PersistenceUtils().addBackslash(subCategory);
            filter = Aggregation.match(Criteria.where("items.subcategory").regex("^" + subCategory + "$", "i"));
        }
        // stage to sort items by putting hotPicks in the front
        AggregationOperation sort = Aggregation
                .sort(Sort.Direction.DESC, "items.hotPick")
                .and(Sort.Direction.DESC, "items.isAvailable")
                .and(Sort.Direction.DESC, "items.hasImage")
                .and(Sort.Direction.ASC, "items." + language);
        // stage to limit and skip documents according to pagination parameters
        AggregationOperation limit = Aggregation.limit(pageable.getPageSize() +
                ((long) pageable.getPageNumber() * pageable.getPageSize()));
        AggregationOperation skip = Aggregation.skip((long) pageable.getPageNumber() * pageable.getPageSize());
        // final project stage
        AggregationOperation project = Aggregation.project("items")
                .andInclude(Aggregation.bind("_id", "items._id"))
                .andInclude(Aggregation.bind("english", "items.english"))
                .andInclude(Aggregation.bind("hindi", "items.hindi"))
                .andInclude(Aggregation.bind("marathi", "items.marathi"))
                .andInclude(Aggregation.bind("englishDescription", "items.englishDescription"))
                .andInclude(Aggregation.bind("hindiDescription", "items.hindiDescription"))
                .andInclude(Aggregation.bind("marathiDescription", "items.marathiDescription"))
                .andInclude(Aggregation.bind("price", "items.price"))
                .andInclude(Aggregation.bind("quantity", "items.quantity"))
                .andInclude(Aggregation.bind("isAvailable", "items.isAvailable"))
                .andInclude(Aggregation.bind("hotPick", "items.hotPick"))
                .andInclude(Aggregation.bind("subcategory", "items.subcategory"))
                .andInclude(Aggregation.bind("hasImage", "items.hasImage"))
                .andInclude(Aggregation.bind("imageName", "items.imageName"));

        Aggregation aggregation;
        if (Objects.nonNull(filter)) {
            aggregation = Aggregation.newAggregation(match, unwind, filter, sort, limit, skip, project);
        } else {
            aggregation = Aggregation.newAggregation(match, unwind, sort, limit, skip, project);
        }

        return mongoTemplate.aggregate(aggregation, "tans", ItemDTO.class).getMappedResults();
    }

    @Override
    public List<TanDTO> listAllStoresContainingItems(boolean forCatalog, int page) {
        int pageSize = 1000;
        if (forCatalog) {
            pageSize = 100;
        }
        Pageable pageable = PageRequest.of(page, pageSize);
        return mongoTemplate.find(new Query(new Criteria().andOperator(
                Criteria.where("isStore").is(true),
                Criteria.where("items").ne(null)
        )).with(pageable), TanDTO.class);
    }

    @Override
    public List<ExploreMediaAttrs> listExploreMedia(double userLatitude, double userLongitude, String keyword,
                                                    long userId, String language, int page) {
        Pageable pageable = PageRequest.of(page, 30);
        // query to fetch stores/groups, according to specified conditions, in ascending order of their distance from user location
        // user does not give location access permission, distance based sorting is not applied
        AggregationOperation geoStage;
        if (userLatitude == 0 && userLongitude == 0) {
            geoStage = Aggregation.match(Criteria.where("secure").is(false));
        } else {
            NearQuery nearQuery = NearQuery
                    .near(userLongitude, userLatitude, Metrics.KILOMETERS)
                    .query(new Query(Criteria.where("secure").is(false)))
                    .spherical(true);
            geoStage = Aggregation.geoNear(nearQuery, "distance");
        }
        // query to fetch walls and posts, according to the specified conditions, associated with each store/group
        String wallLookupQuery;
        String forumLookupQuery;
        // if keyword is not empty, search walls and posts based on the keyword
        if (Strings.isNullOrEmpty(keyword)) {
            wallLookupQuery = "{$lookup: {\n" +
                    "  from: 'walls',\n" +
                    "  let: {tanId:\"$_id\"},\n" +
                    "  pipeline: [\n" +
                    "    {\n" +
                    "      $match: {\n" +
                    "        $expr: {\n" +
                    "          $and: [\n" +
                    "            {$eq: [\"$tanId\", \"$$tanId\"]},\n" +
                    "            {\n" +
                    "              $or: [\n" +
                    "                {$eq: [\"$wallPostHasBinaryData\", true]},\n" +
                    "                {$eq: [\"$hasVideo\", true]}\n" +
                    "              ]\n" +
                    "            },\n" +
                    "            {$ne: [\"$imageName\", null]},\n" +
                    "            {$ne: [\"$imageName\", \"\"]}\n" +
                    "          ]\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }\n" +
                    "  ],\n" +
                    "  as: 'walls'\n" +
                    "}}";
            forumLookupQuery = "{$lookup: {\n" +
                    "  from: 'forums',\n" +
                    "  let: {tanId:\"$_id\"},\n" +
                    "  pipeline: [\n" +
                    "    {\n" +
                    "      $match: {\n" +
                    "        $expr: {\n" +
                    "          $and: [\n" +
                    "            {$eq: [\"$tanId\", \"$$tanId\"]}, {\n" +
                    "              $or: [\n" +
                    "                {$eq: [\"$hasImage\", true]},\n" +
                    "                {$eq: [\"$hasVideo\", true]}\n" +
                    "              ]\n" +
                    "            }\n" +
                    "          ]\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }\n" +
                    "    ],\n" +
                    "  as: 'forums'\n" +
                    "}}";
        } else {
            wallLookupQuery = "{$lookup: {\n" +
                    "  from: 'walls',\n" +
                    "  let: {tanId:\"$_id\", tanName:\"$name\"},\n" +
                    "  pipeline: [\n" +
                    "    {\n" +
                    "      $match: {\n" +
                    "        $or: [\n" +
                    "          {wallPostTitle: {$regex: /" + keyword + "/i}},\n" +
                    "          {wallPostDescription: {$regex: /" + keyword + "/i}},\n" +
                    "          {\n" +
                    "            $expr: {\n" +
                    "              $regexMatch:{input:\"$$tanName\",regex:/" + keyword + "/i}\n" +
                    "            }\n" +
                    "          }\n" +
                    "        ],\n" +
                    "        $expr: {\n" +
                    "          $and: [\n" +
                    "            {$eq: [\"$tanId\", \"$$tanId\"]},\n" +
                    "            {\n" +
                    "              $or: [\n" +
                    "                {$eq: [\"$wallPostHasBinaryData\", true]},\n" +
                    "                {$eq: [\"$hasVideo\", true]}\n" +
                    "              ]\n" +
                    "            },\n" +
                    "            {$ne: [\"$imageName\", null]},\n" +
                    "            {$ne: [\"$imageName\", \"\"]}\n" +
                    "          ]\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }\n" +
                    "  ],\n" +
                    "  as: 'walls'\n" +
                    "}}";
            forumLookupQuery = "{$lookup: {\n" +
                    "  from: 'forums',\n" +
                    "  let: {tanId:\"$_id\", tanName:\"$name\"},\n" +
                    "  pipeline: [\n" +
                    "    {\n" +
                    "      $match: {\n" +
                    "        $or: [\n" +
                    "          {title: {$regex: /" + keyword + "/i}},\n" +
                    "          {\n" +
                    "            $expr: {\n" +
                    "              $regexMatch:{input:\"$$tanName\",regex:/" + keyword + "/i}\n" +
                    "            }\n" +
                    "          }\n" +
                    "        ],\n" +
                    "        $expr: {\n" +
                    "          $and: [\n" +
                    "            {$eq: [\"$tanId\", \"$$tanId\"]}, {\n" +
                    "              $or: [\n" +
                    "                {$eq: [\"$hasImage\", true]},\n" +
                    "                {$eq: [\"$hasVideo\", true]}\n" +
                    "              ]\n" +
                    "            }\n" +
                    "          ]\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }\n" +
                    "    ],\n" +
                    "  as: 'forums'\n" +
                    "}}";
        }
        // query to remove the documents not containing any wall and post
        String matchQuery = "{$match: {\n" +
                "  $or: [\n" +
                "    {\"walls\": {$ne:[]}},\n" +
                "    {\"forums\": {$ne:[]}}\n" +
                "  ]\n" +
                "}}";
        // query to project selected fields, concatenate walls and posts into media
        String projectQuery1 = "{$project: {\n" +
                "  \"_id\": 0,\n" +
                "  \"tanName\": \"$name\",\n" +
                "  \"isStore\": 1,\n" +
                "  \"categories\": 1,\n" +
                "  \"hasImage\": 1,\n" +
                "  \"imageId\": 1,\n" +
                "  \"distance\": 1,\n" +
                "  \"media\": {$concatArrays: [\"$walls\",\"$forums\"]}\n" +
                "}}";
        // stage to create separate document for each media
        AggregationOperation unwind1 = Aggregation.unwind("media", false);
        String addFieldQuery1;
        AggregationOperation sort;
        if (userLatitude == 0 && userLongitude == 0) {
            // query to add field "lastTime", i.e, the greater of "createdOn" or "lastActivityOn" time
            addFieldQuery1 = "{$addFields: {\n" +
                    "  lastTime: {\n" +
                    "    $cond: {\n" +
                    "      if:{$gt:[\"$media.lastActivityOn\", \"$media.createdOn\"]},\n" +
                    "      then:\"$media.lastActivityOn\",\n" +
                    "      else:\"$media.createdOn\"\n" +
                    "    }\n" +
                    "  }\n" +
                    "}}";
            sort = Aggregation.sort(Sort.Direction.DESC, "lastTime");
        } else {
            // query to add field "A" to group documents according to distance and "B" to sort individual groups
            addFieldQuery1 = "{$addFields: {\n" +
                    "  \"A\": {\n" +
                    "    $cond: {\n" +
                    "      if:{$lte:[\"$distance\", 5]},\n" +
                    "      then:1,\n" +
                    "      else:0\n" +
                    "    }\n" +
                    "  },\n" +
                    "  \"B\": {\n" +
                    "    $cond: {\n" +
                    "      if:{$lte:[\"$distance\", 5]},\n" +
                    "      then:\"$media.createdOn\",\n" +
                    "      else:{$multiply:[\"$distance\",-1]}\n" +
                    "    }\n" +
                    "  }\n" +
                    "}}";
            // stage to sort the results according to given conditions
            sort = Aggregation
                    .sort(Sort.Direction.DESC, "A")
                    .and(Sort.Direction.DESC, "B")
                    .and(Sort.Direction.DESC, "media.createdOn");
        }
        // stage to limit and skip documents according to pagination parameters
        AggregationOperation limit = Aggregation.limit(pageable.getPageSize() +
                ((long) pageable.getPageNumber() * pageable.getPageSize()));
        AggregationOperation skip = Aggregation.skip((long) pageable.getPageNumber() * pageable.getPageSize());
        // query to fetch all "like" reactions on the forums
        String forumLikesLookupQuery = "{$lookup: {\n" +
                "  from: 'reactions',\n" +
                "  let: {forumId:{\n" +
                "    $cond: {\n" +
                "      if: {$eq:[\"$media._class\", \"com.qaddoo.persistence.dto.ForumDTO\"]},\n" +
                "      then: \"$media._id\",\n" +
                "      else: 0\n" +
                "    }\n" +
                "  }},\n" +
                "  pipeline: [\n" +
                "    {\n" +
                "      $match: {\n" +
                "        $expr: {\n" +
                "          $and: [\n" +
                "            {$eq: [\"$forumId\", \"$$forumId\"]},\n" +
                "            {$eq: [\"$reaction\",1]},\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  as: 'forumLikes'\n" +
                "}}";
        // query to fetch all "flags" on the forums
        String forumFlagsLookupQuery = "{$lookup: {\n" +
                "  from: 'reactions',\n" +
                "  let: {forumId:{\n" +
                "    $cond: {\n" +
                "      if: {$eq:[\"$media._class\", \"com.qaddoo.persistence.dto.ForumDTO\"]},\n" +
                "      then: \"$media._id\",\n" +
                "      else: 0\n" +
                "    }\n" +
                "  }},\n" +
                "  pipeline: [\n" +
                "    {\n" +
                "      $match: {\n" +
                "        $expr: {\n" +
                "          $and: [\n" +
                "            {$eq: [\"$forumId\", \"$$forumId\"]},\n" +
                "            {$eq: [\"$flag\",1]}\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  as: 'forumFlags'\n" +
                "}}";
        // query to remove forums flagged by the user
        String matchQuery2 = "{$match: {\n" +
                "  $expr: {\n" +
                "    $not: {\n" +
                "      $in:[" + userId + ",\"$forumFlags.userId\"]\n" +
                "    }\n" +
                "  }\n" +
                "}}";
        // query to fetch all "like" reactions on the walls
        String wallLikesLookupQuery = "{$lookup: {\n" +
                "  from: 'wall_reactions',\n" +
                "  let: {wallId:{\n" +
                "    $cond: {\n" +
                "      if: {$eq:[\"$media._class\", \"com.qaddoo.persistence.dto.WallDTO\"]},\n" +
                "      then: \"$media._id\",\n" +
                "      else: 0\n" +
                "    }\n" +
                "  }},\n" +
                "  pipeline: [\n" +
                "    {\n" +
                "      $match: {\n" +
                "        $expr: {\n" +
                "          $eq: [\"$wallId\", \"$$wallId\"]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  as: 'wallLikes'\n" +
                "}}";
        // query to fetch all comments on forum
        String forumCommentsLookupQuery = "{$lookup: {\n" +
                "  from: 'messages',\n" +
                "  let: {forumId:{\n" +
                "    $cond: {\n" +
                "      if: {$eq:[\"$media._class\", \"com.qaddoo.persistence.dto.ForumDTO\"]},\n" +
                "      then: \"$media._id\",\n" +
                "      else: 0\n" +
                "    }\n" +
                "  }},\n" +
                "  pipeline: [\n" +
                "    {\n" +
                "      $match: {\n" +
                "        $expr: {\n" +
                "          $eq:[\"$forumId\",\"$$forumId\"]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  as: 'forumComments'\n" +
                "}}";
        // stage to fetch store categories
        AggregationOperation categoriesLookup = Aggregation.lookup("categories", "categories", "_id", "categories");
        // query to convert DBRef array into object
        String addFieldQuery2 = "{$addFields: {\n" +
                "  \"media.createdBy\": {$arrayElemAt:[{$objectToArray:\"$media.createdBy\"},1]}\n" +
                "}}";
        // stage to fetch createdBy handle
        AggregationOperation createdByHandleLookup = Aggregation.lookup("handles", "media.createdBy.v", "_id", "media.createdBy");
        // stage to fetch createdBy user
        AggregationOperation createdByUserLookup = Aggregation.lookup("users", "media.createdBy.userId", "_id", "media.createdBy");
        // stage to unwind createdByUser
        AggregationOperation unwind2 = Aggregation.unwind("media.createdBy");
        // query to project required fields
        String projectQuery2 = "{$project: {\n" +
                "  \"tanName\": \"$tanName\",\n" +
                "  \"isStore\": \"$isStore\",\n" +
                "  \"categories\": \"$categories." + language + "\",\n" +
                "  \"tanHasImage\":\"$hasImage\",\n" +
                "  \"tanImageName\": \"$imageId\",\n" +
                "  \"creatorName\": \"$media.createdBy.name\",\n" +
                "  \"creatorHasImage\": \"$media.createdBy.hasImage\",\n" +
                "  \"creatorImageName\": \"$media.createdBy.imageName\",\n" +
                "  \"destination\": {\n" +
                "    $cond: {\n" +
                "      if: {$eq:[\"$media._class\",\"com.qaddoo.persistence.dto.ForumDTO\"]},\n" +
                "      then: \"POST\",\n" +
                "      else: \"WALL\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"mediaId\": \"$media._id\",\n" +
                "  \"tanId\": \"$media.tanId\",\n" +
                "  \"title\": {\n" +
                "    $cond: {\n" +
                "      if: {$eq:[\"$media._class\",\"com.qaddoo.persistence.dto.ForumDTO\"]},\n" +
                "      then: \"$media.title\",\n" +
                "      else: \"$media.wallPostTitle\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"description\": {\n" +
                "    $cond: {\n" +
                "      if: {$eq:[\"$media._class\",\"com.qaddoo.persistence.dto.ForumDTO\"]},\n" +
                "      then: \"\",\n" +
                "      else: \"$media.wallPostDescription\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"createdOn\": \"$media.createdOn\",\n" +
                "  \"isLiked\": {\n" +
                "    $cond: {\n" +
                "      if: {\n" +
                "        $or: [\n" +
                "          {$in:[" + userId + ", \"$forumLikes.userId\"]},\n" +
                "          {$in:[" + userId + ", \"$wallLikes.userId\"]}\n" +
                "        ]\n" +
                "      },\n" +
                "      then: true,\n" +
                "      else: false\n" +
                "    }\n" +
                "  },\n" +
                "  \"likeCount\": {\n" +
                "    $cond: {\n" +
                "      if: {$eq:[\"$media._class\",\"com.qaddoo.persistence.dto.ForumDTO\"]},\n" +
                "      then: {$size:\"$forumLikes\"},\n" +
                "      else: {$size:\"$wallLikes\"}\n" +
                "    }\n" +
                "  },\n" +
                "  \"commentCount\": {$size:\"$forumComments\"},\n" +
                "  \"hasImage\": {\n" +
                "    $cond: {\n" +
                "      if: {$eq:[\"$media._class\",\"com.qaddoo.persistence.dto.ForumDTO\"]},\n" +
                "      then: \"$media.hasImage\",\n" +
                "      else: \"$media.wallPostHasBinaryData\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"hasVideo\": \"$media.hasVideo\",\n" +
                "  \"imageName\": \"$media.imageName\"\n" +
                "}}";

        // Pass the aggregation pipeline stages, in respective order, to build the aggregation query
        Aggregation aggregation = Aggregation.newAggregation(
                geoStage,
                new CustomProjectAggregationOperation(wallLookupQuery),
                new CustomProjectAggregationOperation(forumLookupQuery),
                new CustomProjectAggregationOperation(matchQuery),
                new CustomProjectAggregationOperation(projectQuery1),
                unwind1,
                new CustomProjectAggregationOperation(addFieldQuery1),
                sort,
                limit,
                skip,
                new CustomProjectAggregationOperation((forumLikesLookupQuery)),
                new CustomProjectAggregationOperation((forumFlagsLookupQuery)),
                new CustomProjectAggregationOperation((matchQuery2)),
                new CustomProjectAggregationOperation((wallLikesLookupQuery)),
                new CustomProjectAggregationOperation((forumCommentsLookupQuery)),
                categoriesLookup,
                new CustomProjectAggregationOperation(addFieldQuery2),
                createdByHandleLookup,
                createdByUserLookup,
                unwind2,
                new CustomProjectAggregationOperation((projectQuery2))
        );

        return mongoTemplate.aggregate(aggregation, "tans", ExploreMediaAttrs.class).getMappedResults();
    }

    @Override
    public GetStoreDetailResponse getStoreDetails(long storeId, String language, boolean profileScreen, Set<String> favouriteItems) {
        // stage to filter the store based on its id
        AggregationOperation match = Aggregation.match(Criteria.where("_id").is(storeId));
        // stage to fetch store categories
        AggregationOperation categoryLookup = Aggregation.lookup("categories", "categories", "_id", "categories");
        // query to extract createdBy handleId
        String addFieldCreatedByQuery = "{$addFields: {\n" +
                "  createdBy: {\n" +
                "    $arrayElemAt: [{$objectToArray: \"$createdBy\"}, 1]\n" +
                "  }\n" +
                "}}";
        // stage to fetch createdBy handle
        AggregationOperation createdByHandleLookup = Aggregation.lookup("handles", "createdBy.v", "_id", "createdBy");
        // query to fetch admin handles
        String adminHandlesLookupQuery = "{$lookup: {\n" +
                "  from: 'handles',\n" +
                "  let: {tanId:\"$_id\", ownerHandle:{$first: \"$createdBy._id\"}},\n" +
                "  pipeline: [\n" +
                "    {\n" +
                "      $match: {\n" +
                "        $expr: {\n" +
                "          $and: [\n" +
                "            {$eq:[\"$tanId\",\"$$tanId\"]},\n" +
                "            {$eq:[\"$isAdmin\", true]},\n" +
                "            {$ne:[\"$_id\",\"$$ownerHandle\"]}\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  as: 'admins'\n" +
                "}}";
        // stage to fetch createdBy user
        AggregationOperation createdByUserLookup = Aggregation.lookup("users", "createdBy.userId", "_id", "createdBy");
        // stage to fetch admin users
        AggregationOperation adminUsersLookup = Aggregation.lookup("users", "admins.userId", "_id", "admins");
        // query to concatenate createdBy and admin users
        String addFieldAdminsQuery = "{$addFields: {\n" +
                "  admins: {$concatArrays: [\"$createdBy\", \"$admins\"]}\n" +
                "}}";
        // stage to unwind items
        AggregationOperation unwindItems = Aggregation.unwind("items", true);
        // stage to sort items
        AggregationOperation sortItems = Aggregation
                .sort(Sort.Direction.DESC, "items.isAvailable")
                .and(Sort.Direction.DESC, "items.hotPick")
                .and(Sort.Direction.DESC, "item.hasImage");
        // stage to limit the number of items
        AggregationOperation limitItems = Aggregation.limit(4);
        // query to add field "isFavourite" for items
        String addFieldIsFavouriteItemQuery = "{$addFields: {\n" +
                "  \"items.isFavourite\": {\n" +
                "    $cond: {\n" +
                "      if:{\n" +
                "        $or:[\n" +
                "          {$in:[{$toUpper:{$trim:{input:\"$items.english\"}}}, " + favouriteItems + "]},\n" +
                "          {$in:[{$toUpper:{$trim:{input:\"$items.hindi\"}}}, " + favouriteItems + "]},\n" +
                "          {$in:[{$toUpper:{$trim:{input:\"$items.marathi\"}}}, " + favouriteItems + "]}\n" +
                "        ]\n" +
                "      },\n" +
                "      then:true,\n" +
                "      else:false\n" +
                "    }\n" +
                "  }\n" +
                "}}";
        // query to group items
        String groupItemsQuery = "{$group: {\n" +
                "  _id: \"$_id\",\n" +
                "  items: {\n" +
                "    $push: \"$items\"\n" +
                "  },\n" +
                "  doc: {$first:\"$$ROOT\"}\n" +
                "}}";
        // query to set items
        String addFieldItemsQuery = "{$addFields: {\n" +
                "  \"doc.items\": \"$items\",\n" +
                "}}";
        // stage to replace document root
        AggregationOperation replaceRoot = Aggregation.replaceRoot("doc");
        // query to set createdBy
        String addFieldCreatedBy2 = "{$addFields: {\n" +
                "  createdBy: {$first:\"$createdBy\"}\n" +
                "}}";
        // query to add field ownerPhone
        String addFieldOwnerPhone = "{$addFields: {\n" +
                "  ownerPhone: {\n" +
                "    $cond: {\n" +
                "      if: {$eq:[\"$createdBy.email\",\"storeadmin@qaddoo.com\"]},\n" +
                "      then: {\n" +
                "        $first: {\n" +
                "          $filter: {\n" +
                "            input: \"$admins\",\n" +
                "            cond: {$eq:[\"$$this.scrapedOwner\",true]}\n" +
                "          }\n" +
                "        }\n" +
                "      },\n" +
                "      else: \"$createdBy\"\n" +
                "    }\n" +
                "  }\n" +
                "}}";
        // query to project final result
        String projectQuery;
        if (profileScreen) {
            projectQuery = "{$project: {\n" +
                    "  _id: 0,\n" +
                    "  storeId: \"$_id\",\n" +
                    "  storeName: \"$name\",\n" +
                    "  description: {$ifNull:[\"$description\",\"\"]},\n" +
                    "  referralCode: {$ifNull:[\"$referralCode\",\"\"]},\n" +
                    "  storeLongitude: {$first: \"$location.coordinates\"},\n" +
                    "  storeLatitude: {$arrayElemAt: [\"$location.coordinates\", 1]},\n" +
                    "  phone: {$ifNull:[\"$phone\",\"$ownerPhone.phone\"]},\n" +
                    "  storeCurrency: {$ifNull:[\"$currency\",\"INR\"]},\n" +
                    "  hasImage: 1,\n" +
                    "  imageName: {$ifNull:[\"$imageId\",\"\"]},\n" +
                    "  hasBanner: {$ifNull:[\"$hasBanner\",false]},\n" +
                    "  bannerName: {$ifNull:[\"$bannerName\",\"\"]},\n" +
                    "  openingTime: 1,\n" +
                    "  closingTime: 1,\n" +
                    "  openDays: 1,\n" +
                    "  categories: {\n" +
                    "    $map: {\n" +
                    "      input: \"$categories\",\n" +
                    "      in: {\n" +
                    "        _id: \"$$this._id\",\n" +
                    "        name: \"$$this." + language + "\",\n" +
                    "        hasImage: \"$$this.hasImage\",\n" +
                    "        imageName: \"$$this.imageName\",\n" +
                    "      }\n" +
                    "    }\n" +
                    "  },\n" +
                    "  deliveryTypes: \"$deliveryTypes\",\n" +
                    "  deliveryRange: {$ifNull:[\"$deliveryRange\",0]},\n" +
                    "  paymentOptions: {$ifNull:[\"$paymentOptions\",[]]},\n" +
                    "  upiId: {$ifNull:[\"$upiId\",\"\"]},\n" +
                    "  payeeName: {$ifNull:[\"$payeeName\",\"\"]},\n" +
                    "  payeePhone: {$ifNull:[\"$payeePhone\",\"\"]},\n" +
                    "  storeAddress: \"$address\",\n" +
                    "  admins: {\n" +
                    "    $map: {\n" +
                    "      input: \"$admins\",\n" +
                    "      in: {\n" +
                    "        userId: \"$$this._id\",\n" +
                    "        name: \"$$this.name\",\n" +
                    "        adminPhone: \"$$this.phone\",\n" +
                    "        isOwner: {\n" +
                    "          $cond: {\n" +
                    "            if: {$eq:[{$first:\"$admins\"},\"$$this\"]},\n" +
                    "            then: true,\n" +
                    "            else: false\n" +
                    "          }\n" +
                    "        },\n" +
                    "        adminHasImage: \"$$this.hasImage\",\n" +
                    "        adminImage: \"$$this.imageName\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}}";
        } else {
            projectQuery = "{$project: {\n" +
                    "  _id: 0,\n" +
                    "  storeId: \"$_id\",\n" +
                    "  storeName: \"$name\",\n" +
                    "  description: {$ifNull:[\"$description\",\"\"]},\n" +
                    "  referralCode: {$ifNull:[\"$referralCode\",\"\"]},\n" +
                    "  storeLongitude: {$first: \"$location.coordinates\"},\n" +
                    "  storeLatitude: {$arrayElemAt: [\"$location.coordinates\", 1]},\n" +
                    "  phone: {$ifNull:[\"$phone\",\"$ownerPhone.phone\"]},\n" +
                    "  storeCurrency: {$ifNull:[\"$currency\",\"INR\"]},\n" +
                    "  hasImage: 1,\n" +
                    "  imageName: {$ifNull:[\"$imageId\",\"\"]},\n" +
                    "  hasBanner: {$ifNull:[\"$hasBanner\",false]},\n" +
                    "  bannerName: {$ifNull:[\"$bannerName\",\"\"]},\n" +
                    "  openingTime: 1,\n" +
                    "  closingTime: 1,\n" +
                    "  openDays: 1,\n" +
                    "  categories: {\n" +
                    "    $map: {\n" +
                    "      input: \"$categories\",\n" +
                    "      in: {\n" +
                    "        _id: \"$$this._id\",\n" +
                    "        name: \"$$this." + language + "\",\n" +
                    "        hasImage: \"$$this.hasImage\",\n" +
                    "        imageName: \"$$this.imageName\",\n" +
                    "      }\n" +
                    "    }\n" +
                    "  },\n" +
                    "  deliveryTypes: \"$deliveryTypes\",\n" +
                    "  deliveryRange: {$ifNull:[\"$deliveryRange\",0]},\n" +
                    "  paymentOptions: {$ifNull:[\"$paymentOptions\",[]]},\n" +
                    "  upiId: {$ifNull:[\"$upiId\",\"\"]},\n" +
                    "  payeeName: {$ifNull:[\"$payeeName\",\"\"]},\n" +
                    "  payeePhone: {$ifNull:[\"$payeePhone\",\"\"]},\n" +
                    "  storeAddress: \"$address\",\n" +
                    "  admins: {\n" +
                    "    $map: {\n" +
                    "      input: \"$admins\",\n" +
                    "      in: {\n" +
                    "        userId: \"$$this._id\",\n" +
                    "        name: \"$$this.name\",\n" +
                    "        adminPhone: \"$$this.phone\",\n" +
                    "        isOwner: {\n" +
                    "          $cond: {\n" +
                    "            if: {$eq:[{$first:\"$admins\"},\"$$this\"]},\n" +
                    "            then: true,\n" +
                    "            else: false\n" +
                    "          }\n" +
                    "        },\n" +
                    "        adminHasImage: \"$$this.hasImage\",\n" +
                    "        adminImage: \"$$this.imageName\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  },\n" +
                    "  items: {\n" +
                    "    $map: {\n" +
                    "      input: \"$items\",\n" +
                    "      in: {\n" +
                    "        itemId: \"$$this._id\",\n" +
                    "        name: \"$$this." + language + "\",\n" +
                    "        description: \"$$this." + language + "Description\",\n" +
                    "        quantity: \"$$this.quantity\",\n" +
                    "        price: \"$$this.price\",\n" +
                    "        count: \"$$this.count\",\n" +
                    "        isAvailable: \"$$this.isAvailable\",\n" +
                    "        hasImage: \"$$this.hasImage\",\n" +
                    "        imageName: \"$$this.imageName\",\n" +
                    "        subCategory: \"$$this.subCategory\"\n" +
                    "        isFavourite: \"$$this.isFavourite\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}}";
        }

        Aggregation aggregation;

        if (profileScreen) {
            aggregation = Aggregation.newAggregation(
                    match,
                    categoryLookup,
                    new CustomProjectAggregationOperation(addFieldCreatedByQuery),
                    createdByHandleLookup,
                    new CustomProjectAggregationOperation(adminHandlesLookupQuery),
                    createdByUserLookup,
                    adminUsersLookup,
                    new CustomProjectAggregationOperation(addFieldAdminsQuery),
                    new CustomProjectAggregationOperation(addFieldCreatedBy2),
                    new CustomProjectAggregationOperation(addFieldOwnerPhone),
                    new CustomProjectAggregationOperation(projectQuery)
            );
        } else {
            aggregation = Aggregation.newAggregation(
                    match,
                    categoryLookup,
                    new CustomProjectAggregationOperation(addFieldCreatedByQuery),
                    createdByHandleLookup,
                    new CustomProjectAggregationOperation(adminHandlesLookupQuery),
                    createdByUserLookup,
                    adminUsersLookup,
                    new CustomProjectAggregationOperation(addFieldAdminsQuery),
                    unwindItems,
                    sortItems,
                    limitItems,
                    new CustomProjectAggregationOperation(addFieldIsFavouriteItemQuery),
                    new CustomProjectAggregationOperation(groupItemsQuery),
                    new CustomProjectAggregationOperation(addFieldItemsQuery),
                    replaceRoot,
                    new CustomProjectAggregationOperation(addFieldCreatedBy2),
                    new CustomProjectAggregationOperation(addFieldOwnerPhone),
                    new CustomProjectAggregationOperation(projectQuery)
            );
        }

        return mongoTemplate.aggregate(aggregation, "tans", GetStoreDetailResponse.class).getUniqueMappedResult();
    }

    @Override
    public List<TanDTO> listAllStores() {
        return mongoTemplate.find(new Query(Criteria.where("isStore").is(true)), TanDTO.class);
    }

    @Override
    public void addReferralCode(String referralCode, long id) {
        Query query = new Query(Criteria.where("_id").is(id));
        Update updateDefinition = new Update().set("referralCode", referralCode);
        mongoTemplate.findAndModify(query, updateDefinition, TanDTO.class);
    }

    @Override
    public TanDTO getStoreByReferralCode(String referralCode) {
        return mongoTemplate.findOne(new Query(new Criteria()
                .andOperator(
                        Criteria.where("referralCode").is(referralCode),
                        Criteria.where("isStore").is(true)
                )), TanDTO.class);

    }

    @Override
    public List<StoreAttrs> getFavouriteStores(long userId, String language, int page) {
        Pageable pageable = PageRequest.of(page, 30);

        // stage to match handles by userId
        AggregationOperation match = Aggregation.match(new Criteria().andOperator(
                Criteria.where("userId").is(userId),
                Criteria.where("isFavourite").is(true)
        ));

        // query to fetch favourite stores
        String storesLookupQuery = "{$lookup: {\n" +
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
                "  as: 'tan'\n" +
                "}}";

        // stage to limit the results
        AggregationOperation limit = Aggregation.limit(pageable.getPageSize() +
                ((long) pageable.getPageNumber() * pageable.getPageSize()));
        AggregationOperation skip = Aggregation.skip((long) pageable.getPageNumber() * pageable.getPageSize());

        // stage to unwind stores
        AggregationOperation unwindStores = Aggregation.unwind("tan", false);

        // stage to fetch store categories
        AggregationOperation categoryLookup = Aggregation.lookup("categories", "tan.categories", "_id", "tan.categories");

        // query to project the final result
        String projectQuery = "{$project: {\n" +
                "  _id: 0,\n" +
                "  name: \"$tan.name\",\n" +
                "  description: \"$tan.description\",\n" +
                "  longitude: {$first:\"$tan.location.coordinates\"},\n" +
                "  latitude: {$arrayElemAt:[\"$tan.location.coordinates\",1]},\n" +
                "  hasImage: \"$tan.hasImage\",\n" +
                "  hasBanner: \"$tan.hasBanner\",\n" +
                "  imageName: {$ifNull:[\"$tan.imageName\",\"\"]},\n" +
                "  bannerName: {$ifNull:[\"$tan.bannerName\",\"\"]},\n" +
                "  isAdmin: 1,\n" +
                "  categories: \"$tan.categories." + language + "\",\n" +
                "}}";

        Aggregation aggregation = Aggregation.newAggregation(
                match,
                new CustomProjectAggregationOperation(storesLookupQuery),
                limit,
                skip,
                unwindStores,
                categoryLookup,
                new CustomProjectAggregationOperation(projectQuery)
        );

        return mongoTemplate.aggregate(aggregation, "handles", StoreAttrs.class).getMappedResults();
    }

    @Override
    public List<TanAttrs> getFavouriteGroups(long userId, int page) {
        Pageable pageable = PageRequest.of(page, 30);

        // stage to match handles by userId
        AggregationOperation match = Aggregation.match(new Criteria().andOperator(
                Criteria.where("userId").is(userId),
                Criteria.where("isFavourite").is(true)
        ));

        // query to fetch favourite groups
        String groupsLookupQuery = "{$lookup: {\n" +
                "  from: 'tans',\n" +
                "  let: {tanId:\"$tanId\"},\n" +
                "  pipeline: [\n" +
                "    {\n" +
                "      $match: {\n" +
                "        $expr: {\n" +
                "          $and: [\n" +
                "            {$eq:[\"$_id\",\"$$tanId\"]},\n" +
                "            {$eq:[\"$isStore\",false]}\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  as: 'tan'\n" +
                "}}";

        // stage to limit the results
        AggregationOperation limit = Aggregation.limit(pageable.getPageSize() +
                ((long) pageable.getPageNumber() * pageable.getPageSize()));
        AggregationOperation skip = Aggregation.skip((long) pageable.getPageNumber() * pageable.getPageSize());

        // stage to unwind groups
        AggregationOperation unwindGroups = Aggregation.unwind("tan", false);

        // query to project the final result
        String projectQuery = "{$project: {\n" +
                "  \"_id\": 0,\n" +
                "  \"tanId\": \"$tan._id\",\n" +
                "  \"tanName\": \"$tan.name\",\n" +
                "  \"tanDescription\": \"$tan.description\",\n" +
                "  \"tanLongitude\": {$first:\"$tan.location.coordinates\"},\n" +
                "  \"tanLatitude\": {$arrayElemAt:[\"$tan.location.coordinates\",1]},\n" +
                "  \"tanSecure\": \"$tan.secure\",\n" +
                "  \"hasImage\": \"$tan.hasImage\",\n" +
                "  \"imageName\": \"$tan.imageId\",\n" +
                "  \"tanAdmin\": \"$isAdmin\",\n" +
                "}}";

        Aggregation aggregation = Aggregation.newAggregation(
                match,
                new CustomProjectAggregationOperation(groupsLookupQuery),
                limit,
                skip,
                unwindGroups,
                new CustomProjectAggregationOperation(projectQuery)
        );

        return mongoTemplate.aggregate(aggregation, "handles", TanAttrs.class).getMappedResults();
    }

    @Override
    public List<TanDTO> listMyTans(long userId) {
        List<HandleDTO> handleDTOList = mongoTemplate.find(new Query(Criteria.where("userId").is(userId)
                .andOperator(Criteria.where("isAdmin").is(true))), HandleDTO.class);

        List<TanDTO> tanDTOList = new ArrayList<>();

        for (HandleDTO handleDTO : handleDTOList) {
            if (!handleDTO.getTan().getIsStore()) {
                tanDTOList.add(handleDTO.getTan());
            }
        }

        return tanDTOList;
    }

    @Override
    public List<TanDTO> listVisitedTans(long userId) {
        List<HandleDTO> handleDTOList = mongoTemplate.find(new Query(Criteria.where("userId").is(userId)
                .andOperator(Criteria.where("isAdmin").is(false))), HandleDTO.class);

        List<TanDTO> tanDTOList = new ArrayList<>();

        for (HandleDTO handleDTO : handleDTOList) {
            if (!handleDTO.getTan().getIsStore()) {
                tanDTOList.add(handleDTO.getTan());
            }
        }

        return tanDTOList;
    }

    @Override
    public List<TanDTO> listTansByDistance(double userLatitude, double userLongitude, Double radius) {
        return mongoTemplate.find(new Query(Criteria.where("latitude").lt(userLatitude + radius).gt(userLatitude - radius)
                .andOperator(Criteria.where("longitude").lt(userLongitude + radius).gt(userLongitude - radius)
                        .andOperator(Criteria.where("isStore").is(false)))), TanDTO.class);
    }

    @Override
    public List<TanAttrs> listTansByDistance(long userId, double userLatitude, double userLongitude, int page) {
        Pageable pageable = PageRequest.of(page, 30);

        // stage to fetch stores in ascending order of distance from user's location
        NearQuery nearQuery = NearQuery
                .near(userLongitude, userLatitude, Metrics.KILOMETERS)
                .query(new Query(Criteria.where("isStore").is(false)))
                .spherical(true)
                .skip((long) pageable.getPageSize() * pageable.getPageNumber())
                .limit(pageable.getPageSize());
        AggregationOperation geoNear = Aggregation.geoNear(nearQuery, "distance");
        // query to add field "createdBy"
        String addFieldQuery = "{$addFields: {\n" +
                "  createdBy: {\n" +
                "    $arrayElemAt:[{$objectToArray:\"$createdBy\"},1]\n" +
                "  }\n" +
                "}}";
        // stage to fetch the creator handle
        AggregationOperation creatorHandleLookup = Aggregation.lookup("handles", "createdBy.v", "_id", "createdBy");
        // stage to unwind "createdBy"
        AggregationOperation unwindCreatorHandle = Aggregation.unwind("createdBy", false);
        // query to fetch user handle
        String userHandleLookupQuery = "{$lookup: {\n" +
                "  from: 'handles',\n" +
                "  let: {tanId:\"$_id\"},\n" +
                "  pipeline: [\n" +
                "    {\n" +
                "      $match: {\n" +
                "        $expr: {\n" +
                "          $and: [\n" +
                "            {$eq:[\"$userId\"," + userId + "]},\n" +
                "            {$eq:[\"$tanId\",\"$$tanId\"]}\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  as: 'myHandle'\n" +
                "}}";
        // stage to unwind user handle
        AggregationOperation unwindUserHandle = Aggregation.unwind("myHandle", true);
        // query to project the final result
        String projectQuery = "{$project: {\n" +
                "  \"_id\": 0,\n" +
                "  \"tanId\": \"$_id\",\n" +
                "  \"tanName\": \"$name\",\n" +
                "  \"tanDescription\": \"$description\",\n" +
                "  \"tanLongitude\": \"$longitude\",\n" +
                "  \"tanLatitude\": \"$latitude\",\n" +
                "  \"ownerHandle\": \"$createdBy.name\",\n" +
                "  \"tanSecure\": \"$secure\",\n" +
                "  \"hasImage\": 1,\n" +
                "  \"imageName\": \"$imageId\",\n" +
                "  \"tanAdmin\": \"$myHandle.isAdmin\",\n" +
                "  \"member\": {\n" +
                "    $cond: {\n" +
                "      if: {$eq:[" + userId + ",\"$myHandle.userId\"]},\n" +
                "      then: true,\n" +
                "      else: false\n" +
                "    }\n" +
                "  },\n" +
                "  \"distance\": 1\n" +
                "}}";

        Aggregation aggregation = Aggregation.newAggregation(
                geoNear,
                new CustomProjectAggregationOperation(addFieldQuery),
                creatorHandleLookup,
                unwindCreatorHandle,
                new CustomProjectAggregationOperation(userHandleLookupQuery),
                unwindUserHandle,
                new CustomProjectAggregationOperation(projectQuery)
        );

        AggregationResults<TanAttrs> results = mongoTemplate.aggregate(aggregation, "tans", TanAttrs.class);

        return results.getMappedResults();
    }

    @Override
    public List<TanDTO> listStoresByDistance(double userLatitude, double userLongitude, Double radius) {
        return mongoTemplate.find(new Query(Criteria.where("latitude").lt(userLatitude + radius).gt(userLatitude - radius)
                .andOperator(Criteria.where("longitude").lt(userLongitude + radius).gt(userLongitude - radius)
                        .andOperator(Criteria.where("isStore").is(true)))), TanDTO.class);
    }

    @Override
    public List<StoreAttrs> listStoresByDistance(long userId, double userLatitude, double userLongitude, String keyword, String language, int page) {
        Pageable pageable = PageRequest.of(page, 30);
        // query to filter stores in geoNear stage
        Query query;
        if (Strings.isNullOrEmpty(keyword)) {
            query = new Query(Criteria.where("isStore").is(true));
        } else {
            keyword = new PersistenceUtils().addBackslash(keyword);
            query = new Query(Criteria.where("isStore").is(true).andOperator(Criteria.where("name").regex(keyword, "i")));
        }
        // stage to fetch stores in ascending order of distance from user's location
        NearQuery nearQuery = NearQuery
                .near(userLongitude, userLatitude, Metrics.KILOMETERS)
                .query(query)
                .spherical(true)
                .skip((long) pageable.getPageSize() * pageable.getPageNumber())
                .limit(pageable.getPageSize());
        AggregationOperation geoNear = Aggregation.geoNear(nearQuery, "distance");
        // stage to fetch store categories
        AggregationOperation categoryLookup = Aggregation.lookup("categories", "categories", "_id", "categories");
        // query to fetch user handle
        String userHandleLookupQuery = "{$lookup: {\n" +
                "  from: 'handles',\n" +
                "  let: {tanId:\"$_id\"},\n" +
                "  pipeline: [\n" +
                "    {\n" +
                "      $match: {\n" +
                "        $expr: {\n" +
                "          $and: [\n" +
                "            {$eq:[\"$userId\"," + userId + "]},\n" +
                "            {$eq:[\"$tanId\",\"$$tanId\"]}\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  as: 'myHandle'\n" +
                "}}";
        // stage to unwind user handle
        AggregationOperation unwindUserHandle = Aggregation.unwind("myHandle", true);
        // query to project the final result
        String projectQuery = "{$project: {\n" +
                "  \"name\": 1,\n" +
                "  \"description\": 1,\n" +
                "  \"longitude\": 1,\n" +
                "  \"latitude\": 1,\n" +
                "  \"hasImage\": 1,\n" +
                "  \"hasBanner\": {$ifNull:[\"$hasBanner\",false]},\n" +
                "  \"bannerName\": {$ifNull:[\"$bannerName\",\"\"]},\n" +
                "  \"imageName\": \"$imageId\",\n" +
                "  \"isAdmin\": \"$myHandle.isAdmin\",\n" +
                "  \"categories\": \"$categories." + language + "\",\n" +
                "  \"distance\": 1\n" +
                "}}";

        Aggregation aggregation = Aggregation.newAggregation(
                geoNear,
                categoryLookup,
                new CustomProjectAggregationOperation(userHandleLookupQuery),
                unwindUserHandle,
                new CustomProjectAggregationOperation(projectQuery)
        );

        return mongoTemplate.aggregate(aggregation, "tans", StoreAttrs.class).getMappedResults();
    }

    @Override
    public List<StoreAttrs> searchStoresByCategory(List<Long> categories, long userId, double userLatitude,
                                                   double userLongitude, String language, int page) {
        Pageable pageable = PageRequest.of(page, 30);

        // stage to fetch stores in ascending order of distance from user's location
        NearQuery nearQuery = NearQuery
                .near(userLongitude, userLatitude, Metrics.KILOMETERS)
                .query(new Query(Criteria.where("isStore").is(true).andOperator(Criteria.where("categories").in(categories))))
                .spherical(true)
                .skip((long) pageable.getPageSize() * pageable.getPageNumber())
                .limit(pageable.getPageSize());
        AggregationOperation geoNear = Aggregation.geoNear(nearQuery, "distance");
        // stage to fetch store categories
        AggregationOperation categoryLookup = Aggregation.lookup("categories", "categories", "_id", "categories");
        // query to fetch user handle
        String userHandleLookupQuery = "{$lookup: {\n" +
                "  from: 'handles',\n" +
                "  let: {tanId:\"$_id\"},\n" +
                "  pipeline: [\n" +
                "    {\n" +
                "      $match: {\n" +
                "        $expr: {\n" +
                "          $and: [\n" +
                "            {$eq:[\"$userId\"," + userId + "]},\n" +
                "            {$eq:[\"$tanId\",\"$$tanId\"]}\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  as: 'myHandle'\n" +
                "}}";
        // stage to unwind user handle
        AggregationOperation unwindUserHandle = Aggregation.unwind("myHandle", true);
        // query to project the final result
        String projectQuery = "{$project: {\n" +
                "  \"name\": 1,\n" +
                "  \"description\": 1,\n" +
                "  \"longitude\": 1,\n" +
                "  \"latitude\": 1,\n" +
                "  \"hasImage\": 1,\n" +
                "  \"hasBanner\": {$ifNull:[\"$hasBanner\",false]},\n" +
                "  \"bannerName\": {$ifNull:[\"$bannerName\",\"\"]},\n" +
                "  \"imageName\": \"$imageId\",\n" +
                "  \"isAdmin\": \"$myHandle.isAdmin\",\n" +
                "  \"categories\": \"$categories." + language + "\",\n" +
                "  \"distance\": 1\n" +
                "}}";

        Aggregation aggregation = Aggregation.newAggregation(
                geoNear,
                categoryLookup,
                new CustomProjectAggregationOperation(userHandleLookupQuery),
                unwindUserHandle,
                new CustomProjectAggregationOperation(projectQuery)
        );

        return mongoTemplate.aggregate(aggregation, "tans", StoreAttrs.class).getMappedResults();
    }

    @Override
    public List<StoreAttrs> searchStoresByItem(long userId, double userLatitude, double userLongitude, String itemName, String language, int page) {
        itemName = new PersistenceUtils().addBackslash(itemName);
        Pageable pageable = PageRequest.of(page, 30);
        // query to filter stores containing the item in geoNear stage
        Query query = new Query(Criteria.where("isStore").is(true).orOperator(
                Criteria.where("items.english").regex(itemName, "i"),
                Criteria.where("items.hindi").regex(itemName, "i"),
                Criteria.where("items.marathi").regex(itemName, "i")
        ));
        // stage to fetch stores in ascending order of distance from user's location
        NearQuery nearQuery = NearQuery
                .near(userLongitude, userLatitude, Metrics.KILOMETERS)
                .query(query)
                .spherical(true)
                .skip((long) pageable.getPageSize() * pageable.getPageNumber())
                .limit(pageable.getPageSize());
        AggregationOperation geoNear = Aggregation.geoNear(nearQuery, "distance");
        // stage to fetch store categories
        AggregationOperation categoryLookup = Aggregation.lookup("categories", "categories", "_id", "categories");
        // query to fetch user handle
        String userHandleLookupQuery = "{$lookup: {\n" +
                "  from: 'handles',\n" +
                "  let: {tanId:\"$_id\"},\n" +
                "  pipeline: [\n" +
                "    {\n" +
                "      $match: {\n" +
                "        $expr: {\n" +
                "          $and: [\n" +
                "            {$eq:[\"$userId\"," + userId + "]},\n" +
                "            {$eq:[\"$tanId\",\"$$tanId\"]}\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  as: 'myHandle'\n" +
                "}}";
        // stage to unwind user handle
        AggregationOperation unwindUserHandle = Aggregation.unwind("myHandle", true);
        // query to project the final result
        String projectQuery = "{$project: {\n" +
                "  \"name\": 1,\n" +
                "  \"description\": 1,\n" +
                "  \"longitude\": 1,\n" +
                "  \"latitude\": 1,\n" +
                "  \"hasImage\": 1,\n" +
                "  \"hasBanner\": {$ifNull:[\"$hasBanner\",false]},\n" +
                "  \"bannerName\": {$ifNull:[\"$bannerName\",\"\"]},\n" +
                "  \"imageName\": \"$imageId\",\n" +
                "  \"isAdmin\": \"$myHandle.isAdmin\",\n" +
                "  \"categories\": \"$categories." + language + "\",\n" +
                "  \"distance\": 1\n" +
                "}}";

        Aggregation aggregation = Aggregation.newAggregation(
                geoNear,
                categoryLookup,
                new CustomProjectAggregationOperation(userHandleLookupQuery),
                unwindUserHandle,
                new CustomProjectAggregationOperation(projectQuery)
        );

        return mongoTemplate.aggregate(aggregation, "tans", StoreAttrs.class).getMappedResults();
    }

    @Override
    public List<StoreAttrs> listStoresInDistanceRange(double userLatitude, double userLongitude, double range) {
        // query to filter stores in geoNear stage
        NearQuery nearQuery = NearQuery
                .near(userLongitude, userLatitude, Metrics.KILOMETERS)
                .query(new Query(Criteria.where("isStore").is(true)))
                .spherical(true)
                .maxDistance(range, Metrics.KILOMETERS);
        AggregationOperation geoNear = Aggregation.geoNear(nearQuery, "distance");
        AggregationOperation project = Aggregation.project("name");

        Aggregation aggregation = Aggregation.newAggregation(geoNear, project);

        return mongoTemplate.aggregate(aggregation, "tans", StoreAttrs.class).getMappedResults();
    }

}
