//package com.qaddoo.persistence.dao.impl;
//
//import com.qaddoo.persistence.CommonDAOImpl;
//import com.qaddoo.persistence.dao.OrderItemDAO;
//import com.qaddoo.persistence.dto.OrderItemDTO;
//import org.springframework.stereotype.Repository;
//
//@Repository("orderItemDAO")
//public class OrderItemDAOImpl extends CommonDAOImpl implements OrderItemDAO {
//
//    @Override
//    public long addOrderItem(OrderItemDTO orderItemDTO) {
//        return (Long) this.saveObject(orderItemDTO);
//    }
//
//    @Override
//    public OrderItemDTO getOrderItemById(long id) {
//        String namedQuery = "orderItems.getOrderItemById";
//        return getSingleObjectFromNamedQuery(namedQuery, OrderItemDTO.class, "id", id);
//    }
//}
