package com.qaddoo.qaddooanalytics.repository;

import com.mongodb.lang.NonNull;
import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;

// Class to allow custom JSON mongodb query to be used in aggregation operation
public class CustomProjectAggregationOperation implements AggregationOperation {
    private final String jsonOperation;

    public CustomProjectAggregationOperation(String jsonOperation) {
        this.jsonOperation = jsonOperation;
    }

    @Override
    @NonNull
    public Document toDocument(AggregationOperationContext aggregationOperationContext) {
        return aggregationOperationContext.getMappedObject(Document.parse(jsonOperation));
    }
}
