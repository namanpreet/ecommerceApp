package com.qaddoo.persistence.dao.impl;

import com.qaddoo.persistence.dao.UserDAO;
import com.qaddoo.persistence.dto.ExploreUserDTO;
import com.qaddoo.persistence.dto.UserDTO;
import com.qaddoo.pojo.entity.GetUserDetailResponse;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository("userDAO")
@EnableAsync
public class UserDAOImpl implements UserDAO {

    private final MongoTemplate mongoTemplate;

    public UserDAOImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public long addUser(UserDTO userDTO) {
        UserDTO user = mongoTemplate.insert(userDTO);
        return user.getId();
    }

    @Override
    @Async
    public void addExploreUser(ExploreUserDTO exploreUserDTO) {
        mongoTemplate.insert(exploreUserDTO);
    }

    @Override
    public UserDTO getUserByEmail(String email) {
        return mongoTemplate.findOne(new Query(Criteria.where("email").is(email)), UserDTO.class);
    }

    @Override
    public UserDTO getUserByPhone(String phone) {
        return mongoTemplate.findOne(new Query(Criteria.where("phone").is(phone)), UserDTO.class);
    }

    @Override
    public UserDTO getUserByUserId(long userId) {
        return mongoTemplate.findOne(new Query(Criteria.where("_id").is(userId)), UserDTO.class);
    }

    @Override
    public UserDTO updateUser(UserDTO userDTO) {
        return mongoTemplate.findAndReplace(new Query(Criteria.where("_id").is(userDTO.getId())), userDTO);
    }

    @Override
    public List<UserDTO> listNearbyUsers(long userId, double latitude, double longitude, double radius) {

        return mongoTemplate.find(new Query(Criteria.where("usersLat").lt(latitude + radius).gt(latitude - radius)
                .andOperator(Criteria.where("usersLng").lt(longitude + radius).gt(longitude - radius)
                        .andOperator(Criteria.where("_id").ne(userId)))), UserDTO.class);
    }

    @Override
    public UserDTO getUserByAuthToken(String authToken) {
        return mongoTemplate.findOne(new Query(Criteria.where("authToken").is(authToken)), UserDTO.class);
    }

    @Override
    public UserDTO existingUser(String phone) {
        return mongoTemplate.findOne(new Query(Criteria.where("phone").regex(".*" + phone, "i")
                .andOperator(Criteria.where("name").ne(null))), UserDTO.class);
    }

    @Override
    public List<UserDTO> usersToNotify(int page) {
        Pageable pageable = PageRequest.of(page, 50);
        // stage to filter the users to be notified
        AggregationOperation match = Aggregation.match(new Criteria().andOperator(Criteria.where("loggedOut").ne(true),
                Criteria.where("receiveNotification").is(true), Criteria.where("deviceId").ne(null), Criteria.where("deviceId").ne("")));
        // stage to fetch users according to page number
        AggregationOperation skip = Aggregation.skip((long) pageable.getPageNumber() * pageable.getPageSize());
        AggregationOperation limit = Aggregation.limit(pageable.getPageSize());

        Aggregation aggregation = Aggregation.newAggregation(match, skip, limit);

        return mongoTemplate.aggregate(aggregation, "users", UserDTO.class).getMappedResults();
    }

    @Override
    public GetUserDetailResponse getUserDetails(long userId, boolean redirectInfo) {
        // stage to match the user by userId
        AggregationOperation match = Aggregation.match(Criteria.where("_id").is(userId));
        // query to fetch user's admin handles
        String adminHandlesLookupQuery = "{$lookup: {\n" +
                "  from: 'handles',\n" +
                "  let: {userId:\"$_id\"},\n" +
                "  pipeline:[\n" +
                "    {\n" +
                "      $match: {\n" +
                "        $expr: {\n" +
                "          $and: [\n" +
                "            {$eq:[\"$userId\",\"$$userId\"]},\n" +
                "            {$eq:[\"$isAdmin\",true]}\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  as: 'adminHandles'\n" +
                "}}";
        // stage to lookup tans in which user is admin
        AggregationOperation adminTansLookup = Aggregation.lookup("tans", "adminHandles.tanId", "_id", "myTans");
        // query to fetch user's default address
        String defaultAddressLookupQuery = "{$lookup: {\n" +
                "  from: 'user_addresses',\n" +
                "  let: {userId:\"$_id\"},\n" +
                "  pipeline: [\n" +
                "    {\n" +
                "      $match: {\n" +
                "        $expr: {\n" +
                "          $and: [\n" +
                "            {$eq:[\"$userId\",\"$$userId\"]},\n" +
                "            {$eq:[\"$isDefault\",true]}\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  as: 'defaultAddress'\n" +
                "}}";
        // query to fetch non-default addresses
        String otherAddressesLookupQuery = "{$lookup: {\n" +
                "  from: 'user_addresses',\n" +
                "  let: {userId:\"$_id\"},\n" +
                "  pipeline: [\n" +
                "    {\n" +
                "      $match: {\n" +
                "        $expr: {\n" +
                "          $and: [\n" +
                "            {$eq:[\"$userId\",\"$$userId\"]},\n" +
                "            {$eq:[\"$isDefault\",false]}\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  as: 'otherAddresses'\n" +
                "}}";
        // query to concatenate all addresses
        String addFieldQuery = "{$addFields: {\n" +
                "  addresses: {\n" +
                "    $concatArrays:[\"$defaultAddress\",\"$otherAddresses\"]\n" +
                "  }\n" +
                "}}";
        // query to filter stores from all tans
        String addFieldQuery1 = "{$addFields: {\n" +
                "  filteredTans: {\n" +
                "    $map: {\n" +
                "      input:\"$myTans\",\n" +
                "      in:{\n" +
                "        isStore:\"$$this.isStore\",\n" +
                "        hasBanner:{$ifNull:[\"$$this.hasBanner\",\"none\"]},\n" +
                "        deliveryOptions:{$ifNull:[\"$$this.deliveryTypes\",\"none\"]}\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}}";
        // query to filter stores with no banner or deliveryOptions
        String addFieldQuery2 = "{$addFields: {\n" +
                "  addresses: {\n" +
                "    $concatArrays:[\"$defaultAddress\",\"$otherAddresses\"]\n" +
                "  },\n" +
                "  filteredTans: {\n" +
                "    $filter: {\n" +
                "      input:\"$filteredTans\",\n" +
                "      cond:{\n" +
                "        $and:[\n" +
                "          {$eq:[\"$$this.isStore\",true]},\n" +
                "          {$or:[\n" +
                "            {$eq:[\"$$this.deliveryTypes\",\"none\"]},\n" +
                "            {$eq:[\"$$this.hasBanner\",\"none\"]}\n" +
                "          ]}\n" +
                "        ]\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}}";
        // query to project the final result
        String projectQuery;
        if (redirectInfo) {
            projectQuery = "{$project: {\n" +
                    "  _id:0,\n" +
                    "  deviceId:{$ifNull:[\"$deviceId\",\"\"]},\n" +
                    "  email:{$ifNull:[\"$email\",\"\"]},\n" +
                    "  phone:{$ifNull:[\"$phone\",\"\"]},\n" +
                    "  name:{$ifNull:[\"$name\",\"\"]},\n" +
                    "  usersLat:1,\n" +
                    "  usersLng:1,\n" +
                    "  createdOn:1,\n" +
                    "  lastSignIn:1,\n" +
                    "  receiveNotification:1,\n" +
                    "  hasImage:{$ifNull:[\"$hasImage\",false]},\n" +
                    "  imageName:{$ifNull:[\"$imageName\",false]},\n" +
                    "  redirectToMyStores: {\n" +
                    "    $cond: {\n" +
                    "      if:{$eq:[\"$filteredTans\",[]]},\n" +
                    "      then:false,\n" +
                    "      else:true\n" +
                    "    }\n" +
                    "  },\n" +
                    "  addressAttrs:{\n" +
                    "    $map: {\n" +
                    "      input:\"$addresses\",\n" +
                    "      in: {\n" +
                    "        addressId:\"$$this._id\",\n" +
                    "        name:\"$$this.name\",\n" +
                    "        phone:\"$$this.phone\",\n" +
                    "        latitude:{$ifNull:[\"$$this.latitude\",\"\"]},\n" +
                    "        longitude:{$ifNull:[\"$$this.longitude\",\"\"]},\n" +
                    "        addressLine1:\"$$this.addressLine1\",\n" +
                    "        addressLine2:{$ifNull:[\"$$this.addressLine2\",\"\"]},\n" +
                    "        city:\"$$this.city\",\n" +
                    "        state:\"$$this.state\",\n" +
                    "        country:\"$$this.country\",\n" +
                    "        pincode:\"$$this.pinCode\",\n" +
                    "        isDefault:\"$$this.isDefault\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}}";
        } else {
            projectQuery = "{$project: {\n" +
                    "  _id:0,\n" +
                    "  deviceId:{$ifNull:[\"$deviceId\",\"\"]},\n" +
                    "  email:{$ifNull:[\"$email\",\"\"]},\n" +
                    "  phone:{$ifNull:[\"$phone\",\"\"]},\n" +
                    "  name:{$ifNull:[\"$name\",\"\"]},\n" +
                    "  usersLat:1,\n" +
                    "  usersLng:1,\n" +
                    "  createdOn:1,\n" +
                    "  lastSignIn:1,\n" +
                    "  receiveNotification:1,\n" +
                    "  hasImage:{$ifNull:[\"$hasImage\",false]},\n" +
                    "  imageName:{$ifNull:[\"$imageName\",false]},\n" +
                    "  addressAttrs:{\n" +
                    "    $map: {\n" +
                    "      input:\"$addresses\",\n" +
                    "      in: {\n" +
                    "        addressId:\"$$this._id\",\n" +
                    "        name:\"$$this.name\",\n" +
                    "        phone:\"$$this.phone\",\n" +
                    "        latitude:{$ifNull:[\"$$this.latitude\",\"\"]},\n" +
                    "        longitude:{$ifNull:[\"$$this.longitude\",\"\"]},\n" +
                    "        addressLine1:\"$$this.addressLine1\",\n" +
                    "        addressLine2:{$ifNull:[\"$$this.addressLine2\",\"\"]},\n" +
                    "        city:\"$$this.city\",\n" +
                    "        state:\"$$this.state\",\n" +
                    "        country:\"$$this.country\",\n" +
                    "        pincode:\"$$this.pinCode\",\n" +
                    "        isDefault:\"$$this.isDefault\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}}";
        }

        Aggregation aggregation;
        if (redirectInfo) {
            aggregation = Aggregation.newAggregation(
                    match,
                    new CustomProjectAggregationOperation(adminHandlesLookupQuery),
                    adminTansLookup,
                    new CustomProjectAggregationOperation(defaultAddressLookupQuery),
                    new CustomProjectAggregationOperation(otherAddressesLookupQuery),
                    new CustomProjectAggregationOperation(addFieldQuery1),
                    new CustomProjectAggregationOperation(addFieldQuery2),
                    new CustomProjectAggregationOperation(projectQuery)
            );
        } else {
            aggregation = Aggregation.newAggregation(
                    match,
                    new CustomProjectAggregationOperation(defaultAddressLookupQuery),
                    new CustomProjectAggregationOperation(otherAddressesLookupQuery),
                    new CustomProjectAggregationOperation(addFieldQuery),
                    new CustomProjectAggregationOperation(projectQuery)
            );
        }

        return mongoTemplate.aggregate(aggregation, "users", GetUserDetailResponse.class).getUniqueMappedResult();
    }
}

