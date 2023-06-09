package com.qaddoo.persistence.dao.impl;

import com.qaddoo.persistence.dao.OrderDAO;
import com.qaddoo.persistence.dto.OrderDTO;
import com.qaddoo.pojo.entity.OrderAttrs;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndReplaceOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class OrderDAOImpl implements OrderDAO {

    private final MongoTemplate mongoTemplate;

    public OrderDAOImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public long addOrder(OrderDTO orderDTO) {
        OrderDTO order = mongoTemplate.insert(orderDTO);
        return order.getId();
    }

    @Override
    public OrderDTO getOrderById(long id) {
        return mongoTemplate.findOne(new Query(Criteria.where("_id").is(id)), OrderDTO.class);
    }

    @Override
    public List<OrderDTO> getUserOrdersInStore(long userId, long storeId, List<String> status) {
        return mongoTemplate.find(new Query(Criteria.where("userId").is(userId)
                .andOperator(Criteria.where("storeId").is(storeId)
                        .andOperator(Criteria.where("isPlaced").is(true)
                                .andOperator(Criteria.where("status").in(status)))))
                .with(Sort.by(Sort.Direction.DESC, "dateTime")), OrderDTO.class);
    }

    @Override
    public List<OrderDTO> getUserOrdersInStore(long userId, long storeId, List<String> status, int page) {
        Pageable pageable = PageRequest.of(page, 30);

        return mongoTemplate.find(new Query(new Criteria().andOperator(
                Criteria.where("userId").is(userId),
                Criteria.where("storeId").is(storeId),
                Criteria.where("isPlaced").is(true),
                Criteria.where("status").in(status)
        )).with(Sort.by(Sort.Direction.DESC, "dateTime")).with(pageable), OrderDTO.class);
    }

    @Override
    public List<OrderDTO> getAllUserOrders(long userId) {
        return mongoTemplate.find(new Query(Criteria.where("userId").is(userId)
                .andOperator(Criteria.where("isPlaced").is(true)))
                .with(Sort.by(Sort.Direction.DESC, "dateTime")), OrderDTO.class);
    }

    @Override
    public List<OrderAttrs> getAllUserOrders(long userId, int page) {
        Pageable pageable = PageRequest.of(page, 30);

        AggregationOperation match = Aggregation.match(new Criteria("userId").is(userId).andOperator(Criteria.where("isPlaced").is(true)));
        AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "dateTime");
        AggregationOperation limit = Aggregation.limit(pageable.getPageSize() +
                ((long) pageable.getPageNumber() * pageable.getPageSize()));
        AggregationOperation skip = Aggregation.skip((long) pageable.getPageNumber() * pageable.getPageSize());
        AggregationOperation tanLookup = Aggregation.lookup("tans", "storeId", "_id", "tan");
        AggregationOperation tanUnwind = Aggregation.unwind("tan");
        String addFieldCreatedByDataHandle = "{\n" +
                "    $addFields: {\n" +
                "        createdByDataHandle: {\n" +
                "            $arrayElemAt: [{\n" +
                "                $objectToArray: \"$tan.createdBy\"\n" +
                "            }, 1]\n" +
                "        }\n" +
                "    }\n" +
                "}";
        String placedByAddField = "{\n" +
                "    $addFields: {\n" +
                "        placedBy: {\n" +
                "            $arrayElemAt: [{\n" +
                "                $objectToArray: \"$placedBy\"\n" +
                "            }, 1]\n" +
                "        }\n" +
                "    }\n" +
                "}";
        AggregationOperation placedByLookup = Aggregation.lookup("handles", "placedBy.v", "_id", "placedBy");
        AggregationOperation placedByUnwind = Aggregation.unwind("placedBy");
        AggregationOperation handleLookup = Aggregation.lookup("handles", "createdByDataHandle.v", "_id", "createdByhandle");
        AggregationOperation createdByHandleUnwind = Aggregation.unwind("createdByhandle");
        AggregationOperation createdByLookup = Aggregation.lookup("users", "createdByhandle.userId", "_id", "createdBy");
        AggregationOperation createdByUnwind = Aggregation.unwind("createdBy");
        String project = " {\n" +
                "    $project: {\n" +
                "        dateTime: '$dateTime',\n" +
                "        amount: '$amount',\n" +
                "        userId: '$userId',\n" +
                "        storeName: '$tan.name',\n" +
                "        storeId: '$storeId',\n" +
                "        orderStatus: '$status',\n" +
                "        deliveryType: '$deliveryType',\n" +
                "        storeAddress: '$storeAddress',\n" +
                "        discount: '$discount',\n" +
                "        items: '$orderItems',\n" +
                "        total: '$total',\n" +
                "        orderId: '$_id',\n" +
                "        placedBy: '$placedBy.name',\n" +
                "        storeAddress: {\n" +
                "            name: \"$createdBy.name\",\n" +
                "            pincode: \"$tan.address.pinCode\",\n" +
                "            phone: \"$createdBy.phone\",\n" +
                "            city: \"$tan.address.city\",\n" +
                "            state: \"$tan.address.state\",\n" +
                "            addressLine1: \"$tan.address.addressLine1\",\n" +
                "            addressLine2: \"$tan.address.addressLine2\",\n" +
                "            country: \"$tan.address.country\"\n" +
                "        },\n" +
                "        rejectionReason: '$rejectionReason',\n" +
                "        deliveryAddress: {\n" +
                "            pincode: '$deliveryAddress.pinCode',\n" +
                "            name: '$deliveryAddress.name',\n" +
                "            addressLine1: '$deliveryAddress.addressLine1',\n" +
                "            addressLine2: '$deliveryAddress.addressLine2',\n" +
                "            city: '$deliveryAddress.city',\n" +
                "            state: '$deliveryAddress.state',\n" +
                "            country: '$deliveryAddress.country',\n" +
                "            isDefault: '$deliveryAddress.isDefault',\n" +
                "            phone: '$deliveryAddress.phone',\n" +
                "            latitude: '$deliveryAddress.latitude',\n" +
                "            longitude: '$deliveryAddress.longitude'\n" +
                "        }\n" +
                "\n" +
                "    }\n" +
                "}";
        Aggregation aggregation = Aggregation.newAggregation(
                match,
                sort,
                limit,
                skip,
                tanLookup,
                tanUnwind,
                new CustomProjectAggregationOperation(addFieldCreatedByDataHandle),
                new CustomProjectAggregationOperation(placedByAddField),
                placedByLookup,
                placedByUnwind,
                handleLookup,
                createdByHandleUnwind,
                createdByLookup,
                createdByUnwind,
                new CustomProjectAggregationOperation(project));

        return mongoTemplate.aggregate(aggregation, "orders", OrderAttrs.class).getMappedResults();
    }

    @Override
    public List<OrderDTO> getStoreOrders(long storeId, List<String> status) {
        return mongoTemplate.find(new Query(Criteria.where("storeId").is(storeId)
                .andOperator(Criteria.where("status").in(status)
                        .andOperator(Criteria.where("isPlaced").is(true))))
                .with(Sort.by(Sort.Direction.DESC, "dateTime")), OrderDTO.class);
    }

    @Override
    public List<OrderDTO> getStoreOrders(long storeId, List<String> status, int page) {
        Pageable pageable = PageRequest.of(page, 30);

        return mongoTemplate.find(new Query(new Criteria().andOperator(
                Criteria.where("storeId").is(storeId),
                Criteria.where("isPlaced").is(true),
                Criteria.where("status").in(status)
        )).with(Sort.by(Sort.Direction.DESC, "dateTime")).with(pageable), OrderDTO.class);
    }

    @Override
    public OrderDTO getSavedOrder(long userId, long storeId) {
        return mongoTemplate.findOne(new Query(new Criteria().andOperator(
                Criteria.where("userId").is(userId),
                Criteria.where("storeId").is(storeId),
                Criteria.where("isPlaced").is(false)
        )), OrderDTO.class);
    }


    @Override
    public OrderDTO updateOrder(OrderDTO orderDTO) {
        FindAndReplaceOptions options = FindAndReplaceOptions.options().returnNew();
        return mongoTemplate.findAndReplace(new Query(Criteria.where("_id").is(orderDTO.getId())), orderDTO, options);
    }

    @Override
    public List<OrderAttrs> getAllAdminOrders(long userId, int page) {
        Pageable pageable = PageRequest.of(page, 30);

        AggregationOperation match = Aggregation.match(new Criteria("userId").is(userId).andOperator(Criteria.where("isAdmin").is(true)));
        AggregationOperation tanLookup = Aggregation.lookup("tans", "tanId", "_id", "tans");
        AggregationOperation tanUnwind = Aggregation.unwind("tans");
        AggregationOperation orderDataUnwind = Aggregation.unwind("tans.orders");
        String orderDef = "{\n" +
                "    $addFields: {\n" +
                "        orderdef: {\n" +
                "            $arrayElemAt: [{\n" +
                "                $objectToArray: \"$tans.orders\"\n" +
                "            }, 1]\n" +
                "        }\n" +
                "    }\n" +
                "}";
        AggregationOperation orderLookup = Aggregation.lookup("orders",
                "orderdef.v", "_id", "order");
        AggregationOperation orderUnwind = Aggregation.unwind("order");
        AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "order.dateTime");
        AggregationOperation limit = Aggregation.limit(pageable.getPageSize() +
                ((long) pageable.getPageNumber() * pageable.getPageSize()));
        AggregationOperation skip = Aggregation.skip((long) pageable.getPageNumber() * pageable.getPageSize());

        String placedByAddField = "{\n" +
                "    $addFields: {\n" +
                "        placedbydef: {\n" +
                "            $arrayElemAt: [{\n" +
                "                $objectToArray: \"$order.placedBy\"\n" +
                "            }, 1]\n" +
                "        }\n" +
                "    }\n" +
                "}";
        AggregationOperation placedByLookup = Aggregation.lookup("handles", "placedbydef.v", "_id", "placedBy");
        AggregationOperation placedByUnwind = Aggregation.unwind("placedBy");
        String addFieldCreatedByDataHandle = " {\n" +
                "    $addFields: {\n" +
                "        createdByDataHandle: {\n" +
                "            $arrayElemAt: [{\n" +
                "                $objectToArray: \"$tans.createdBy\"\n" +
                "            }, 1]\n" +
                "        }\n" +
                "    }\n" +
                "}";
        AggregationOperation createdHandleByLookup = Aggregation.lookup("handles", "createdByDataHandle.v", "_id", "createdByHandle");
        AggregationOperation createdHandleByUnwind = Aggregation.unwind("createdByHandle");
        AggregationOperation createdByLookup = Aggregation.lookup("users", "createdByHandle.userId", "_id", "createdBy");
        AggregationOperation createdByUnwind = Aggregation.unwind("createdBy");
        String project = " {\n" +
                "    $project: {\n" +
                "        dateTime: '$order.dateTime',\n" +
                "        amount: '$order.amount',\n" +
                "        userId: '$order.userId',\n" +
                "        storeName: '$tans.name',\n" +
                "        storeId: '$order.storeId',\n" +
                "        orderStatus: '$order.status',\n" +
                "        deliveryType: '$order.deliveryType',\n" +
                "        storeAddress: '$storeAddress',\n" +
                "        discount: '$order.discount',\n" +
                "        items: '$order.orderItems',\n" +
                "        total: '$order.total',\n" +
                "        orderId: '$order._id',\n" +
                "        placedBy: '$placedBy.name',\n" +
                "        storeAddress: {\n" +
                "            name: \"$createdBy.name\",\n" +
                "            pincode: \"$tans.address.pinCode\",\n" +
                "            phone: \"$createdBy.phone\",\n" +
                "            city: \"$tans.address.city\",\n" +
                "            state: \"$tans.address.state\",\n" +
                "            addressLine1: \"$tans.address.addressLine1\",\n" +
                "            addressLine2: \"$tans.address.addressLine2\",\n" +
                "            country: \"$tans.address.country\"\n" +
                "        },\n" +
                "        rejectionReason: '$rejectionReason',\n" +
                "        deliveryAddress: {\n" +
                "            pincode: '$order.deliveryAddress.pinCode',\n" +
                "            name: '$order.deliveryAddress.name',\n" +
                "            addressLine1: '$order.deliveryAddress.addressLine1',\n" +
                "            addressLine2: '$order.deliveryAddress.addressLine2',\n" +
                "            city: '$order.deliveryAddress.city',\n" +
                "            state: '$order.deliveryAddress.state',\n" +
                "            country: '$order.deliveryAddress.country',\n" +
                "            isDefault: '$order.deliveryAddress.isDefault',\n" +
                "            phone: '$order.deliveryAddress.phone',\n" +
                "            latitude: '$order.deliveryAddress.latitude',\n" +
                "            longitude: '$order.deliveryAddress.longitude',\n" +
                "            addressId: '$order.deliveryAddress._id'\n" +
                "        }\n" +
                "\n" +
                "    }\n" +
                "}";
        Aggregation aggregation = Aggregation.newAggregation(
                match,
                tanLookup,
                tanUnwind,
                orderDataUnwind,
                new CustomProjectAggregationOperation(orderDef),
                orderLookup,
                orderUnwind,
                sort,
                limit,
                skip,
                new CustomProjectAggregationOperation(placedByAddField),
                placedByLookup,
                placedByUnwind,
                new CustomProjectAggregationOperation(addFieldCreatedByDataHandle),
                createdHandleByLookup,
                createdHandleByUnwind,
                createdByLookup,
                createdByUnwind,
                new CustomProjectAggregationOperation(project));

        return mongoTemplate.aggregate(aggregation, "handles", OrderAttrs.class).getMappedResults();
    }

    @Override
    public OrderDTO updateUid(long orderId, String uid) {
        Query query = new Query(Criteria.where("_id").is(orderId));
        UpdateDefinition updateDefinition = new Update().set("uid", uid);
        return mongoTemplate.findAndModify(query, updateDefinition, OrderDTO.class);
    }

    @Override
    public long getStoreOrderCount(long storeId, String status) {
        return mongoTemplate.count(new Query(new Criteria().andOperator(
                Criteria.where("storeId").is(storeId),
                Criteria.where("isPlaced").is(true),
                Criteria.where("status").is(status)
        )), OrderDTO.class);
    }

    @Override
    public boolean doesUserOrderExist(long userId) {
        return mongoTemplate.exists(new Query(new Criteria().andOperator(
                Criteria.where("userId").is(userId),
                Criteria.where("isPlaced").is(true)
        )), OrderDTO.class);
    }

    @Override
    public boolean doesSellerOrderExist(List<Long> storeIds, String status) {
        return mongoTemplate.exists(new Query(new Criteria().andOperator(
                Criteria.where("storeId").in(storeIds),
                Criteria.where("status").is(status)
        )), OrderDTO.class);
    }


}
