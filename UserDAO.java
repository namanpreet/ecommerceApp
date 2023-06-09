package com.qaddoo.qaddooanalytics.repository;

import com.google.common.base.Strings;
import com.qaddoo.qaddooanalytics.model.ExploreUserDTO;
import com.qaddoo.qaddooanalytics.model.LoginDTO;
import com.qaddoo.qaddooanalytics.model.TanDTO;
import com.qaddoo.qaddooanalytics.model.UserDTO;
import com.qaddoo.qaddooanalytics.pojo.TanAttrs;
import com.qaddoo.qaddooanalytics.pojo.UserAttrs;
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
public class UserDAO {

    private final MongoTemplate mongoTemplate;

    public UserDAO(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public long registeredUserCount(Date startDate, Date endDate) {
        if (startDate == null || endDate == null) {
            return mongoTemplate.count(new Query(new Criteria()
                    .andOperator(Criteria.where("active").ne(false))), UserDTO.class);
        }

        if (startDate == endDate) {
            return mongoTemplate.count(new Query(Criteria.where("createdOn").is(startDate)
                    .andOperator(Criteria.where("active").ne(false))), UserDTO.class);
        }

        return mongoTemplate.count(new Query(Criteria.where("createdOn").lte(endDate).gte(startDate)
                .andOperator(Criteria.where("active").ne(false))), UserDTO.class);
    }

    public long profileUserCount(Date startDate, Date endDate, String language) {
        if (startDate == null || endDate == null) {
            return mongoTemplate.count(new Query(new Criteria().andOperator(Criteria.where("name").ne(null)
            )), UserDTO.class);
        }
        if (Strings.isNullOrEmpty(language) || language.equalsIgnoreCase("all")) {
            return mongoTemplate.count(new Query(Criteria.where("createdOn").lte(endDate).gte(startDate)
                    .andOperator(Criteria.where("name").ne(null)
                    )), UserDTO.class);
        }
        return mongoTemplate.count(new Query(Criteria.where("createdOn").lte(endDate).gte(startDate)
                .andOperator(Criteria.where("language").is(language)
                        .andOperator(Criteria.where("name").ne(null)
                        ))), UserDTO.class);
    }

    public long exploreUserCount(Date startDate, Date endDate, String language) {
        if (startDate == null || endDate == null) {
            return mongoTemplate.count(new Query(new Criteria().andOperator(Criteria.where("_id").ne(null))), ExploreUserDTO.class);
        }
        if (Strings.isNullOrEmpty(language) || language.equalsIgnoreCase("all")) {
            return mongoTemplate.count(new Query(Criteria.where("createdOn").lte(endDate).gte(startDate)), ExploreUserDTO.class);
        }
        return mongoTemplate.count(new Query(new Criteria().andOperator(Criteria.where("createdOn").lte(endDate).gte(startDate),
                Criteria.where("language").is(language))), ExploreUserDTO.class);
    }

    public long inactiveUserCount(Date startDate, Date endDate) {
        if (startDate == null || endDate == null) {
            return mongoTemplate.count(new Query(new Criteria()
                    .andOperator(Criteria.where("loggedOut").is(true), Criteria.where("scrapedOwner").ne(true))), UserDTO.class);
        }
        return mongoTemplate.count(new Query(Criteria.where("createdOn").lte(endDate).gte(startDate)
                .andOperator(Criteria.where("loggedOut").is(true), Criteria.where("scrapedOwner").ne(true))), UserDTO.class);
    }

    public long referredUserCount(Date startDate, Date endDate) {
        if (startDate == null || endDate == null) {
            return mongoTemplate.count(new Query(new Criteria().andOperator(
                    Criteria.where("storeReferralCode").ne(null)
            )), UserDTO.class);
        } else if (startDate == endDate) {

            return mongoTemplate.count(new Query(new Criteria().andOperator(
                    Criteria.where("storeReferralCode").ne(null),
                    Criteria.where("createdOn").is(endDate)
            )), UserDTO.class);
        }
        return mongoTemplate.count(new Query(new Criteria().andOperator(
                Criteria.where("storeReferralCode").ne(null),
                Criteria.where("createdOn").lte(endDate).gte(startDate)
        )), UserDTO.class);
    }

    public long qoinUserCount(Date startDate, Date endDate) {
        if (startDate == null || endDate == null) {
            return mongoTemplate.count(new Query(new Criteria().andOperator(
                    Criteria.where("qcoinCount").ne(0).orOperator(Criteria.where("qcoinGenerated").ne(0))
            )), UserDTO.class);
        } else if (startDate == endDate) {

            return mongoTemplate.count(new Query(new Criteria().andOperator(
                    Criteria.where("qcoinCount").ne(0).orOperator(Criteria.where("qcoinGenerated").ne(0)),
                    Criteria.where("createdOn").is(endDate)
            )), UserDTO.class);
        }
        return mongoTemplate.count(new Query(new Criteria().andOperator(
                Criteria.where("qcoinCount").ne(0),
                // .orOperator(Criteria.where("qcoinGenerated").ne(0)),
                Criteria.where("createdOn").lte(endDate).gte(startDate)
        )), UserDTO.class);
    }


    public List<UserAttrs> userDetails(Date startDate, Date endDate, boolean requireSort, int page) {

        if (startDate == null || endDate == null) {
            return mongoTemplate.findAll(UserAttrs.class, "users");
        }


        // stage to match users according to given conditions

        AggregationOperation match = Aggregation.match(new Criteria().andOperator(
                Criteria.where("name").ne(null),
                // Criteria.where("active").ne(false),
                Criteria.where("createdOn").lte(endDate).gte(startDate)
        ));
        // stage to sort the results according to creation date
        AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "createdOn");
        // stage to lookup referral store
        String referralStoreLookupQuery = "{$lookup: {\n" +
                "  from: 'tans',\n" +
                "  let: {referralCode:\"$storeReferralCode\"},\n" +
                "  pipeline: [\n" +
                "    {\n" +
                "      $match: {\n" +
                "        $expr: {\n" +
                "          $and: [\n" +
                "            {$eq:[\"$isStore\", true]},\n" +
                "            {$eq:[\"$referralCode\", \"$$referralCode\"]}\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  as: \"referralStore\"\n" +
                "}}";
        // stage to unwind referral store
        AggregationOperation unwindReferralStore = Aggregation.unwind("referralStore", true);
        // query to add field for store name
        String addStoreNameQuery = "{$addFields: {\n" +
                "  referredByStore: \"$referralStore.name\"\n" +
                "}}";

        // Pass the aggregation pipeline stages, in respective order, to build the aggregation query
        Aggregation aggregation;
        if (requireSort) {
            if (page == -1) {

                aggregation = Aggregation.newAggregation(
                        match,
                        sort,
                        new CustomProjectAggregationOperation(referralStoreLookupQuery),
                        unwindReferralStore,
                        new CustomProjectAggregationOperation(addStoreNameQuery)
                );
            } else {
                Pageable p = PageRequest.of(page, 15);
                AggregationOperation skip = Aggregation.skip((long) p.getPageNumber() * p.getPageSize());
                AggregationOperation limit = Aggregation.limit(p.getPageSize());
                aggregation = Aggregation.newAggregation(
                        match,
                        sort,
                        skip,
                        limit,
                        new CustomProjectAggregationOperation(referralStoreLookupQuery),
                        unwindReferralStore,
                        new CustomProjectAggregationOperation(addStoreNameQuery)
                );
            }
        } else {
            if (page == -1) {
                aggregation = Aggregation.newAggregation(
                        match,
                        new CustomProjectAggregationOperation(referralStoreLookupQuery),
                        unwindReferralStore,
                        new CustomProjectAggregationOperation(addStoreNameQuery)
                );
            } else {
                Pageable p = PageRequest.of(page, 15);
                AggregationOperation skip = Aggregation.skip((long) p.getPageNumber() * p.getPageSize());
                AggregationOperation limit = Aggregation.limit(p.getPageSize());
                aggregation = Aggregation.newAggregation(
                        match,
                        skip,
                        limit,
                        new CustomProjectAggregationOperation(referralStoreLookupQuery),
                        unwindReferralStore,
                        new CustomProjectAggregationOperation(addStoreNameQuery)
                );
            }
        }

        return mongoTemplate.aggregate(aggregation, "users", UserAttrs.class).getMappedResults();
    }

    public long userCount(Date startDate, Date endDate) {
        if (startDate == null || endDate == null) {
            return mongoTemplate.count(new Query(new Criteria().andOperator(
                    Criteria.where("_id").ne(null)
            )), UserDTO.class);
        }
        //Criteria.where("active").ne(false)
        if (startDate == endDate) {
            return mongoTemplate.count(new Query(new Criteria().andOperator(
                    Criteria.where("name").ne(null),
                    Criteria.where("createdOn").is(startDate))), UserDTO.class);
        }

        return mongoTemplate.count(new Query(new Criteria().andOperator(
                Criteria.where("name").ne(null),
                Criteria.where("createdOn").lte(endDate).gte(startDate))), UserDTO.class);

    }

    public List<ExploreUserDTO> exploreUserDetails(Date startDate, Date endDate) {
        if (startDate == endDate && startDate != null) {

            return mongoTemplate.find(new Query(Criteria.where("createdOn").is(startDate))
                    .with(Sort.by(Sort.Direction.DESC, "createdOn")), ExploreUserDTO.class);
        }

        return mongoTemplate.find(new Query(Criteria.where("createdOn").lte(endDate).gte(startDate))
                .with(Sort.by(Sort.Direction.DESC, "createdOn")), ExploreUserDTO.class);
    }

    public long sellerCount(Date startDate, Date endDate) {

        if (startDate == endDate && startDate != null) {
            return mongoTemplate.count(new Query(Criteria.where("createdOn").is(startDate)
                    .andOperator(Criteria.where("seller").is(true))), UserDTO.class);
        }
        if (startDate == null || endDate == null) {
            return mongoTemplate.count(new Query(new Criteria()
                    .andOperator(Criteria.where("seller").is(true))), UserDTO.class);
        }
        return mongoTemplate.count(new Query(Criteria.where("createdOn").lte(endDate).gte(startDate)
                .andOperator(Criteria.where("seller").is(true))), UserDTO.class);
    }

    public long buyerCount(Date startDate, Date endDate) {

        if (startDate == endDate && startDate != null) {
            return mongoTemplate.count(new Query(Criteria.where("createdOn").is(startDate)
                    .andOperator(Criteria.where("seller").is(false))), UserDTO.class);
        }

        if (startDate == null || endDate == null) {
            return mongoTemplate.count(new Query(new Criteria()
                    .andOperator(Criteria.where("seller").is(false))), UserDTO.class);
        }
        return mongoTemplate.count(new Query(Criteria.where("createdOn").lte(endDate).gte(startDate)
                .andOperator(Criteria.where("seller").is(false))), UserDTO.class);
    }

    public List<String> uniqueCountryCount() {
        return mongoTemplate.findDistinct(new Query(Criteria.where("country").ne(null)), "country", UserDTO.class, String.class);
    }

    public long uniqueCityCount() {
        return mongoTemplate.findDistinct(new Query(Criteria.where("city").ne(null)), "city", UserDTO.class, UserDTO.class).size();
    }

    public long uniqueCityCountIndia() {
        return mongoTemplate.findDistinct(new Query(Criteria.where("country").in("India", "भारत")), "city", UserDTO.class, UserDTO.class).size();
    }

    public LoginDTO getLoginDetails(String email, String password) throws Exception {

        LoginDTO loginDto = mongoTemplate.findOne(new Query(Criteria.where("email").is(email).andOperator(
                Criteria.where("password").is(password))), LoginDTO.class);


        if (loginDto != null) {
            loginDto.setMessage("Email and Password Match");
            return loginDto;
        } else {
            loginDto= new LoginDTO();
            loginDto.setAuthToken(null);
            loginDto.setId(0);
            loginDto.setEmail(null);
            loginDto.setPassword(null);
            loginDto.setMessage("Email or Password is incorrect");

            return loginDto;

        }

    }
}
