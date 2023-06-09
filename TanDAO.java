package com.qaddoo.qaddooanalytics.repository;

import com.qaddoo.qaddooanalytics.model.OrderDTO;
import com.qaddoo.qaddooanalytics.model.TanDTO;
import com.qaddoo.qaddooanalytics.pojo.ReferralStoreAttrs;
import com.qaddoo.qaddooanalytics.pojo.TanAttrs;
import io.swagger.models.auth.In;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.sort;

@Repository
public class TanDAO {

    private final MongoTemplate mongoTemplate;

    public TanDAO(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public long groupCount(Date startDate, Date endDate) {
        if (startDate == null || endDate == null) {
            return mongoTemplate.count(new Query(Criteria.where("isStore").is(false)), TanDTO.class);
        }
        return mongoTemplate.count(new Query(Criteria.where("createdOn").lte(endDate).gte(startDate)
                .andOperator(Criteria.where("isStore").is(false))), TanDTO.class);
    }

    public long storeCount(Date startDate, Date endDate) {
        if (startDate == null || endDate == null) {
            return mongoTemplate.count(new Query(Criteria.where("isStore").is(true)), TanDTO.class);
        } else {
            return mongoTemplate.count(new Query(Criteria.where("createdOn").lte(endDate).gte(startDate)
                    .andOperator(Criteria.where("isStore").is(true))), TanDTO.class);
        }
    }

    public long qoinStoreCount(Date startDate, Date endDate){
        if(startDate ==null || endDate==null){
            return mongoTemplate.count(new Query(new Criteria().andOperator(
                    Criteria.where("coinCount").ne(0)
            )), TanDTO.class);
        } else{
        return mongoTemplate.count(new Query(new Criteria().andOperator(
                Criteria.where("coinCount").ne(0),
                Criteria.where("createdOn").lte(endDate).gte(startDate)
        )), TanDTO.class);
    }}


    public List<TanAttrs> storeDetails(Date startDate, Date endDate, boolean sort, Integer page) {

        if (startDate == null || endDate == null) {
            AggregationOperation matches = Aggregation.match(new Criteria().andOperator(
                    Criteria.where("isStore").is(true))
            );
            AggregationOperation sortAggregations = sort(Sort.Direction.DESC, "createdOn");
            if (sort) {

                Aggregation aggregation;
                if (page == -1) {

                    aggregation = Aggregation.newAggregation(
                            matches,
                            sortAggregations
                    ).withOptions(Aggregation.newAggregationOptions().
                            allowDiskUse(true).build());
                } else {
                    Pageable p = PageRequest.of(page, 15);
                    AggregationOperation skip = Aggregation.skip((long) p.getPageNumber() * p.getPageSize());
                    AggregationOperation limit = Aggregation.limit(p.getPageSize());
                    aggregation = Aggregation.newAggregation(
                            matches,
                            sortAggregations,
                            skip,
                            limit
                    ).withOptions(Aggregation.newAggregationOptions().
                            allowDiskUse(true).build());
                }
                List<TanAttrs> tans = mongoTemplate.aggregate(aggregation, "tans", TanAttrs.class).getMappedResults();
                tans.forEach(x -> {
                    if (x.getOrders() != null) {
                        x.setNumberOfOrders(x.getOrders().size());
                        BigDecimal sum = new BigDecimal(0);
                        List<OrderDTO> orders = x.getOrders();
                        orders.forEach(y -> {
                            BigDecimal amount = y.getAmount();
                            if(amount!=null) {
                                sum.add(y.getAmount());
                            }
                        });
                        x.setTotalOrderValue(sum);
                        x.setOrders(null);
                    }

                });
                return tans;
//            return mongoTemplate.find(new Query(Criteria.where("createdOn").lte(endDate).gte(startDate)
//                    .andOperator(Criteria.where("isStore").is(true))).with(Sort.by(Sort.Direction.DESC, "createdOn")), TanDTO.class);
            } else {
                Aggregation aggregation;
                if (page == -1) {

                    aggregation = Aggregation.newAggregation(
                            matches
                    ).withOptions(Aggregation.newAggregationOptions().
                            allowDiskUse(true).build());
                } else {
                    Pageable p = PageRequest.of(page, 15);
                    AggregationOperation skip = Aggregation.skip((long) p.getPageNumber() * p.getPageSize());
                    AggregationOperation limit = Aggregation.limit(p.getPageSize());
                    aggregation = Aggregation.newAggregation(
                            matches,
                            skip,
                            limit
                    ).withOptions(Aggregation.newAggregationOptions().
                            allowDiskUse(true).build());
                }
                List<TanAttrs> tans = mongoTemplate.find(new Query(Criteria.where("isStore").is(true)), TanAttrs.class);

                tans.forEach(x -> {
                    x.setNumberOfOrders(x.getOrders().size());
                    BigDecimal sum = new BigDecimal(0);
                    List<OrderDTO> orders = x.getOrders();
                    orders.forEach(y -> {
                        BigDecimal amount = y.getAmount();
                        if(amount!=null) {
                            sum.add(y.getAmount());
                        }
                    });
                    x.setTotalOrderValue(sum);
                    x.setOrders(null);

                });
                return tans;
            }
        }

        if (startDate.compareTo(endDate) == 0) {
            return mongoTemplate.find(new Query(new Criteria().andOperator(
                    Criteria.where("createdOn").is(startDate),
                    Criteria.where("isStore").is(true)
            )), TanAttrs.class);
        }

        AggregationOperation match = Aggregation.match(new Criteria().andOperator(
                Criteria.where("isStore").is(true),
                Criteria.where("createdOn").lte(endDate).gte(startDate))
        );
        AggregationOperation sortAggregation = sort(Sort.Direction.DESC, "createdOn");
        if (sort) {

            Aggregation aggregation;
            if (page == -1) {

                aggregation = Aggregation.newAggregation(
                        match,
                        sortAggregation
                ).withOptions(Aggregation.newAggregationOptions().
                        allowDiskUse(true).build());
            } else {
                Pageable p = PageRequest.of(page, 15);
                AggregationOperation skip = Aggregation.skip((long) p.getPageNumber() * p.getPageSize());
                AggregationOperation limit = Aggregation.limit(p.getPageSize());
                aggregation = Aggregation.newAggregation(
                        match,
                        sortAggregation,
                        skip,
                        limit
                ).withOptions(Aggregation.newAggregationOptions().
                        allowDiskUse(true).build());
            }
            List<TanAttrs> tans = mongoTemplate.aggregate(aggregation, "tans", TanAttrs.class).getMappedResults();
            tans.forEach(x -> {
                if (x.getOrders() != null) {
                    x.setNumberOfOrders(x.getOrders().size());
                    List<BigDecimal> amountValues= new ArrayList<>();
                    BigDecimal sum = new BigDecimal(0);
                    List<OrderDTO> orders = x.getOrders();
                    orders.forEach(y -> {
                        BigDecimal amount = y.getAmount();
                        if(amount!=null) {
                            amountValues.add(amount);
                        }
                    });
                    for(BigDecimal d : amountValues)
                      sum=sum.add(d);
                    x.setTotalOrderValue(sum);
                    x.setOrders(null);
                }

            });

            return tans;
//            return mongoTemplate.find(new Query(Criteria.where("createdOn").lte(endDate).gte(startDate)
//                    .andOperator(Criteria.where("isStore").is(true))).with(Sort.by(Sort.Direction.DESC, "createdOn")), TanDTO.class);
        } else {
            Aggregation aggregation;
            if (page == -1) {

                aggregation = Aggregation.newAggregation(
                        match
                ).withOptions(Aggregation.newAggregationOptions().
                        allowDiskUse(true).build());
            } else {
                Pageable p = PageRequest.of(page, 15);
                AggregationOperation skip = Aggregation.skip((long) p.getPageNumber() * p.getPageSize());
                AggregationOperation limit = Aggregation.limit(p.getPageSize());
                aggregation = Aggregation.newAggregation(
                        match,
                        skip,
                        limit
                ).withOptions(Aggregation.newAggregationOptions().
                        allowDiskUse(true).build());
            }
            List<TanAttrs> tans = mongoTemplate.find(new Query(Criteria.where("createdOn").lte(endDate).gte(startDate)
                    .andOperator(Criteria.where("isStore").is(true))), TanAttrs.class);
            List<BigDecimal> amountValues= new ArrayList<>();
            tans.forEach(x -> {
                x.setNumberOfOrders(x.getOrders().size());
                BigDecimal sum =new BigDecimal(0);
                List<OrderDTO> orders = x.getOrders();
                orders.forEach(y -> {
                    BigDecimal amount = y.getAmount();
                    if(amount!=null) {
                        amountValues.add(amount);
                    }
                });
                for(BigDecimal d : amountValues)
                    sum=sum.add(d);
                x.setTotalOrderValue(sum);
                x.setOrders(null);

            });
            return tans;
        }

    }

    public List<TanDTO> groupDetails(Date startDate, Date endDate, boolean sort) {
        if (sort) {
            return mongoTemplate.find(new Query(Criteria.where("createdOn").lte(endDate).gte(startDate)
                    .andOperator(Criteria.where("isStore").is(false))).with(Sort.by(Sort.Direction.DESC, "createdOn")), TanDTO.class);
        } else {
            return mongoTemplate.find(new Query(Criteria.where("createdOn").lte(endDate).gte(startDate)
                    .andOperator(Criteria.where("isStore").is(false))), TanDTO.class);
        }

    }

    public TanDTO getStoreById(long storeId) {
        return mongoTemplate.findOne(new Query(Criteria.where("_id").is(storeId)), TanDTO.class);
    }

    public List<ReferralStoreAttrs> getReferralStores(Date startDate, Date endDate) {
        // stage to match users according to given conditions
        AggregationOperation match = Aggregation.match(new Criteria().andOperator(
                Criteria.where("storeReferralCode").ne(null),
                Criteria.where("storeReferralCode").ne(""),
                Criteria.where("createdOn").lte(endDate).gte(startDate)
        ));
        // query to group users by store referral code
        String groupQuery = "{$group: {\n" +
                "  _id: \"$storeReferralCode\",\n" +
                "  usersReferred: {\n" +
                "    $sum: 1\n" +
                "  }\n" +
                "}}";
        // stage to fetch stores by referral code
        AggregationOperation storeLookup = Aggregation.lookup("tans", "_id", "referralCode", "store");
        // stage to unwind stores
        AggregationOperation unwindStores = Aggregation.unwind("store", false);
        // final project query
        String project = "{$project: {\n" +
                "  _id: 0,\n" +
                "  name: \"$store.name\",\n" +
                "  createdOn: \"$store.createdOn\",\n" +
                "  usersReferred: 1,\n" +
                "  address: \"$store.address\"\n" +
                "}}";

        // Pass the aggregation pipeline stages, in respective order, to build the aggregation query
        Aggregation aggregation = Aggregation.newAggregation(
                match,
                new CustomProjectAggregationOperation(groupQuery),
                storeLookup,
                unwindStores,
                new CustomProjectAggregationOperation(project)
        );

        return mongoTemplate.aggregate(aggregation, "users", ReferralStoreAttrs.class).getMappedResults();
    }
}
