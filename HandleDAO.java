package com.qaddoo.qaddooanalytics.repository;

import com.qaddoo.qaddooanalytics.model.HandleDTO;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

@Repository
public class HandleDAO {

    private final MongoTemplate mongoTemplate;

    public HandleDAO(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public BigDecimal totalBalance() {
        List<HandleDTO> handleDTOList = mongoTemplate.findAll(HandleDTO.class);
        BigDecimal balance = BigDecimal.ZERO;

        for (HandleDTO handleDTO : handleDTOList) {
            if (Objects.nonNull(handleDTO.getBalance())) {
                balance = balance.add(handleDTO.getBalance());
            }
        }

        return balance;
    }
}
