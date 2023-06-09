package com.qaddoo.persistence.dao.impl;

import com.qaddoo.persistence.dao.UserAddressDAO;
import com.qaddoo.persistence.dto.UserAddressDTO;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

@Repository("userAddressDAO")
public class UserAddressDAOImpl implements UserAddressDAO {

    private final MongoTemplate mongoTemplate;

    public UserAddressDAOImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public long addAddress(UserAddressDTO userAddressDTO) {
        UserAddressDTO userAddress = mongoTemplate.insert(userAddressDTO);
        return userAddress.getId();
    }

    @Override
    public UserAddressDTO getAddressById(long id) {
        return mongoTemplate.findOne(new Query(Criteria.where("_id").is(id)), UserAddressDTO.class);
    }

    @Override
    public UserAddressDTO getDefaultAddress(long userId) {
        return mongoTemplate.findOne(new Query(Criteria.where("userId").is(userId)
                .andOperator(Criteria.where("isDefault").is(true))), UserAddressDTO.class);
    }

    @Override
    public UserAddressDTO updateAddress(UserAddressDTO userAddressDTO) {
        return mongoTemplate.findAndReplace(new Query(Criteria.where("_id").is(userAddressDTO.getId())), userAddressDTO);
    }

    @Override
    public void deleteAddress(long addressId) {
        mongoTemplate.remove(new Query(Criteria.where("_id").is(addressId)), UserAddressDTO.class);
    }
}
