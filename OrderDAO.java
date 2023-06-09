package com.qaddoo.qaddooanalytics.repository;

import com.google.common.base.Strings;
import com.qaddoo.qaddooanalytics.model.OndcOrderDTO;
import com.qaddoo.qaddooanalytics.model.OrderAttrs;
import com.qaddoo.qaddooanalytics.model.OrderDTO;
import com.qaddoo.qaddooanalytics.model.TanDTO;
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

import java.math.BigDecimal;
import java.util.*;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.sort;

@Repository
public class OrderDAO {

    private final MongoTemplate mongoTemplate;

    private final TanDAO tanDAO;

    public OrderDAO(MongoTemplate mongoTemplate, TanDAO tanDAO) {
        this.mongoTemplate = mongoTemplate;
        this.tanDAO = tanDAO;
    }

    public long orderCount(Date startDate, Date endDate, String status) {
        if (startDate == null && endDate == null) {
            return mongoTemplate.count(new Query(Criteria.where("isPlaced").is(true)), OrderDTO.class);

        }
        if (Strings.isNullOrEmpty(status) || status.equalsIgnoreCase("all")) {
            return mongoTemplate.count(new Query(Criteria.where("dateTime").lte(endDate).gte(startDate)
                    .andOperator(Criteria.where("isPlaced").is(true))), OrderDTO.class);
        }
        return mongoTemplate.count(new Query(Criteria.where("dateTime").lte(endDate).gte(startDate)
                .andOperator(Criteria.where("status").is(status)
                        .andOperator(Criteria.where("isPlaced").is(true)))), OrderDTO.class);
    }

    public List<OrderDTO> valueOrders(Date startDate, Date endDate) {
        return mongoTemplate.find(new Query(Criteria.where("dateTime").lte(endDate).gte(startDate)
                .andOperator(Criteria.where("amount").ne(null))), OrderDTO.class);
    }

    public List<OrderDTO> orderDetails(Date startDate, Date endDate, String status, boolean sort) {
        if (sort) {
            if (Strings.isNullOrEmpty(status) || status.equalsIgnoreCase("all")) {
                return mongoTemplate.find(new Query(Criteria.where("dateTime").lte(endDate).gte(startDate)
                        .andOperator(Criteria.where("isPlaced").is(true)))
                        .with(Sort.by(Sort.Direction.DESC, "dateTime")), OrderDTO.class);
            }
            return mongoTemplate.find(new Query(Criteria.where("dateTime").lte(endDate).gte(startDate)
                    .andOperator(Criteria.where("status").is(status)
                            .andOperator(Criteria.where("isPlaced").is(true))))
                    .with(Sort.by(Sort.Direction.DESC, "dateTime")), OrderDTO.class);
        } else {
            if (Strings.isNullOrEmpty(status) || status.equalsIgnoreCase("all")) {
                return mongoTemplate.find(new Query(Criteria.where("dateTime").lte(endDate).gte(startDate)
                        .andOperator(Criteria.where("isPlaced").is(true))), OrderDTO.class);
            }
            return mongoTemplate.find(new Query(Criteria.where("dateTime").lte(endDate).gte(startDate)
                    .andOperator(Criteria.where("status").is(status)
                            .andOperator(Criteria.where("isPlaced").is(true)))), OrderDTO.class);
        }

    }

    public List<OrderAttrs> orderDetailsRequired(Date startDate, Date endDate, String status, boolean sort, Integer page) {

        if (startDate == null || endDate == null) {

                AggregationOperation match = Aggregation.match(new Criteria().andOperator(
                        Criteria.where("isPlaced").is(true))
                );
                AggregationOperation sortAggregation = sort(Sort.Direction.DESC, "dateTime");
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
                    if (Strings.isNullOrEmpty(status) || status.equalsIgnoreCase("all")) {

                        List<OrderAttrs> orderAttrs = mongoTemplate.aggregate(aggregation, "orders", OrderAttrs.class).getMappedResults();

                        orderAttrs.forEach(orderAttr ->
                        {
                            TanDTO storeById = tanDAO.getStoreById(orderAttr.getStoreId());
                            orderAttr.setStoreName(storeById.getName());

                        });
                        return orderAttrs;
                    }
                    List<OrderAttrs> orderAttrs = mongoTemplate.aggregate(aggregation, "orders", OrderAttrs.class).getMappedResults();


                    orderAttrs.forEach(x ->
                    {
                        TanDTO storeById = tanDAO.getStoreById(x.getStoreId());
                        x.setStoreName(storeById.getName());

                    });
                    return orderAttrs;
                } else {
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
                    if (Strings.isNullOrEmpty(status) || status.equalsIgnoreCase("all")) {
                        List<OrderAttrs> orderAttrs = mongoTemplate.find(new Query(Criteria.where("dateTime").lte(endDate).gte(startDate)
                                .andOperator(Criteria.where("isPlaced").is(true)))
                                .with(Sort.by(Sort.Direction.DESC, "dateTime")), OrderAttrs.class);
                        orderAttrs.forEach(x ->
                        {
                            TanDTO storeById = tanDAO.getStoreById(x.getStoreId());
                            x.setStoreName(storeById.getName());

                        });
                        return orderAttrs;
                    }
                    List<OrderAttrs> orderAttrs = mongoTemplate.find(new Query(Criteria.where("dateTime").lte(endDate).gte(startDate)
                            .andOperator(Criteria.where("isPlaced").is(true)))
                            .with(Sort.by(Sort.Direction.DESC, "dateTime")), OrderAttrs.class);
                    orderAttrs.forEach(x ->
                    {
                        TanDTO storeById = tanDAO.getStoreById(x.getStoreId());
                        x.setStoreName(storeById.getName());

                    });
                    return orderAttrs;
                }

            }
        else {
            AggregationOperation match = Aggregation.match(new Criteria().andOperator(
                    Criteria.where("dateTime").lte(endDate).gte(startDate)
                            .andOperator(Criteria.where("isPlaced").is(true)))

            );
            AggregationOperation sortAggregation = sort(Sort.Direction.DESC, "dateTime");
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
                if (Strings.isNullOrEmpty(status) || status.equalsIgnoreCase("all")) {

                    List<OrderAttrs> orderAttrs = mongoTemplate.aggregate(aggregation, "orders", OrderAttrs.class).getMappedResults();

                    orderAttrs.forEach(orderAttr ->
                    {
                        TanDTO storeById = tanDAO.getStoreById(orderAttr.getStoreId());
                        orderAttr.setStoreName(storeById.getName());

                    });
                    return orderAttrs;
                }
                List<OrderAttrs> orderAttrs = mongoTemplate.aggregate(aggregation, "orders", OrderAttrs.class).getMappedResults();


                orderAttrs.forEach(x ->
                {
                    TanDTO storeById = tanDAO.getStoreById(x.getStoreId());
                    x.setStoreName(storeById.getName());

                });
                return orderAttrs;
            } else {
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
                if (Strings.isNullOrEmpty(status) || status.equalsIgnoreCase("all")) {
                    List<OrderAttrs> orderAttrs = mongoTemplate.find(new Query(Criteria.where("dateTime").lte(endDate).gte(startDate)
                            .andOperator(Criteria.where("isPlaced").is(true)))
                            .with(Sort.by(Sort.Direction.DESC, "dateTime")), OrderAttrs.class);

                    orderAttrs.forEach(x ->
                    {
                        TanDTO storeById = tanDAO.getStoreById(x.getStoreId());
                        x.setStoreName(storeById.getName());

                    });
                    return orderAttrs;
                }
                List<OrderAttrs> orderAttrs = mongoTemplate.find(new Query(Criteria.where("dateTime").lte(endDate).gte(startDate)
                        .andOperator(Criteria.where("isPlaced").is(true)))
                        .with(Sort.by(Sort.Direction.DESC, "dateTime")), OrderAttrs.class);
                orderAttrs.forEach(x ->
                {
                    TanDTO storeById = tanDAO.getStoreById(x.getStoreId());
                    x.setStoreName(storeById.getName());

                });
                return orderAttrs;
            }

        }
    }

    public OrderDTO getOrderByUid(String uid) {
        return mongoTemplate.findOne(Query.query(Criteria.where("uid").is(uid)), OrderDTO.class);
    }

    public Map<String, BigDecimal> orderLabels(Date startDate, Date endDate) {
        Map<String, BigDecimal> hashMap = new HashMap<String, BigDecimal>();
        List<OrderDTO> orderDTOList = new ArrayList<>();
        if (startDate == null || endDate == null) {
            orderDTOList = mongoTemplate.find(new Query(Criteria.where("amount").ne(null)), OrderDTO.class);

        } else {
            orderDTOList = mongoTemplate.find(new Query(Criteria.where("amount").ne(null).andOperator(Criteria.where("dateTime").lte(endDate).gte(startDate))), OrderDTO.class);
        }
        BigDecimal totalPickupValue = BigDecimal.ZERO;
        BigDecimal totalDeliveryValue = BigDecimal.ZERO;

        if (Objects.nonNull(orderDTOList) && !orderDTOList.isEmpty()) {
            for (OrderDTO orderDTO : orderDTOList) {
                if (orderDTO.getDeliveryType() != null && orderDTO.getDeliveryType().equals("Pickup") && orderDTO.getAmount() != null) {
                    totalPickupValue = totalPickupValue.add(orderDTO.getAmount());
                } else {
                    if (orderDTO.getDeliveryType() != null && orderDTO.getAmount() != null) {
                        totalDeliveryValue = totalDeliveryValue.add(orderDTO.getAmount());
                    }
                }
            }
        }

        hashMap.put("Pickup", totalPickupValue);
        hashMap.put("Delivery", totalDeliveryValue);

        return hashMap;
    }

    public OndcOrderDTO getOndcOrderById(long id) {
        return mongoTemplate.findOne(Query.query(Criteria.where("_id").is(id)),OndcOrderDTO.class, "ondcOrder");
    }
}
