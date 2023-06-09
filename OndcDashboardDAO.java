package com.qaddoo.qaddooanalytics.repository;

import com.qaddoo.qaddooanalytics.model.*;
import com.qaddoo.qaddooanalytics.pojo.ondc.Fulfillment;
import com.qaddoo.qaddooanalytics.pojo.ondc.FulfillmentEnd;
import com.qaddoo.qaddooanalytics.pojo.ondc.Quotation;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Repository
public class OndcDashboardDAO {

    private final MongoTemplate mongoTemplate;

    public OndcDashboardDAO(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }


    public List<OndcOrderAttrs> getOndcOrders() {

        List<OndcOrderDTO> ondcOrder = mongoTemplate.find(new Query(Criteria.where("isPlaced").is(true)).with(Sort.by(Sort.Direction.DESC, "createdOn")), OndcOrderDTO.class);
        DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");

        List<OndcOrderAttrs> ondcOrderAttrsList = new ArrayList<>();

        ondcOrder.forEach(x -> {


            x.getItems().forEach(y -> {

                OndcOrderAttrs ondcOrderAttrs = new OndcOrderAttrs();

                if (y.getId() != null) {
                    //for each entry on react they needed a unique id
                    ondcOrderAttrs.setOndcOrderUniqueId(x.getOrderId() + y.getId());

                    ondcOrderAttrs.setBuyerNpName("QADDOO");

                    ondcOrderAttrs.setSellerNpName(x.getSellerNP());

                    ondcOrderAttrs.setOrderCreateDateTime(dateFormat.format(x.getCreatedOn()));

                    ondcOrderAttrs.setNetworkOrderId(x.getOrderId());
                    ondcOrderAttrs.setNpOrderId(x.getOrderId());

                    ondcOrderAttrs.setNetworkTransactionId(x.getTransactionId());

                    ondcOrderAttrs.setBuyerNpOrderItemId(y.getId());
                    ondcOrderAttrs.setItemId(y.getId());

                    ondcOrderAttrs.setQuantity(y.getQuantity().getCount());
                    ondcOrderAttrs.setStatus(x.getState());

                    ondcOrderAttrs.setSellerName(x.getProvider().getDescriptor().getName());

                    if (x.getProvider().getLocations().get(0).getAddress() != null) {

                        ondcOrderAttrs.setSellerPinCode(x.getProvider().getLocations().get(0).getAddress().getAreaCode());
                        ondcOrderAttrs.setSellerCity(x.getProvider().getLocations().get(0).getAddress().getCity());

                    }
                    if (y.getDescriptor() != null) {
                        ondcOrderAttrs.setSkuName(y.getDescriptor().getName());
                        ondcOrderAttrs.setSkuCode(y.getDescriptor().getCode());
                    }

                    ondcOrderAttrs.setOrderCategory(y.getCategoryId());

                    ondcOrderAttrs.setReadyToShipDateTime(null);

                    if(Objects.nonNull(x.getOrderShipDate())) ondcOrderAttrs.setShippedAtDateTime(dateFormat.format(x.getOrderShipDate()));
                    if(Objects.nonNull(x.getOrderDeliverDate())) ondcOrderAttrs.setDeliveredAtDateTime(dateFormat.format(x.getOrderDeliverDate()));

                    ondcOrderAttrs.setDeliveryType(x.getFulfillments().get(0).getType().toString());
                    ondcOrderAttrs.setLogisticSellerName(x.getFulfillments().get(0).get_atOndcorgproviderName());

//                    ondcOrderAttrs.setLogisticOrderId(x.getFulfillments().get(0).getId());
//                    ondcOrderAttrs.setLogisticTransactionId(x.getFulfillments().get(0).getId());
                    FulfillmentEnd fulfillmentEnd = x.getFulfillments().get(0).getEnd();
                    if(Objects.nonNull(fulfillmentEnd)) {
                        ondcOrderAttrs.setDeliveryCity(fulfillmentEnd.getLocation().getAddress().getCity());
                        ondcOrderAttrs.setDeliveryPinCode(fulfillmentEnd.getLocation().getAddress().getAreaCode());
                    }

                    if(Objects.nonNull(x.getOrderCancelledDate())) ondcOrderAttrs.setCancelledAtDateTime(dateFormat.format(x.getOrderCancelledDate()));

                    ondcOrderAttrs.setCancelledBy(x.getCancelRequestedBy());
                    ondcOrderAttrs.setCancellationReason(x.getCancellation_reason_id());

                    if (x.getQuote() != null) {
                        x.getQuote().getBreakup().forEach(breakup -> {
                            if (breakup.getTitle() != null) {
                                if (breakup.getTitle().equals("Delivery Charges")) {
                                    ondcOrderAttrs.setTotalShippingCharges(breakup.getPrice().getValue());
                                }
                            }
                        });
                        ondcOrderAttrs.setTotalOrderValue(x.getQuote().getPrice().getValue());
                    }


                    ondcOrderAttrs.setTotalRefundAmount(null);

                    ondcOrderAttrsList.add(ondcOrderAttrs);

                }
            });


        });

        return ondcOrderAttrsList;
    }

    public List<PaymentOndcOrderAttrs> getPaymentOndcOrders() {

            List<OndcOrderDTO> ondcOrder = mongoTemplate.find(new Query(Criteria.where("isPlaced").is(true))
                    .with(Sort.by(Sort.Direction.DESC, "createdOn")), OndcOrderDTO.class);

            List<PaymentOndcOrderAttrs> paymentOndcOrderAttrsList = new ArrayList<>();

            ondcOrder.forEach(x -> {

                PaymentOndcOrderAttrs paymentOndcOrderAttrs = new PaymentOndcOrderAttrs();
                double bffOnOrder = buyerFinderFeeCalculation(x.getQuote() , 3);

                x.getItems().forEach(y -> {

                    paymentOndcOrderAttrs.setSellerNpName(x.getSellerNP()); //seller app Name
                    paymentOndcOrderAttrs.setSellerName(x.getProvider().getDescriptor().getName());

                    paymentOndcOrderAttrs.setOrderCreateDateTime(x.getCreatedOn());

                    paymentOndcOrderAttrs.setNetworkOrderId(x.getOrderId());

                    paymentOndcOrderAttrs.setBuyerNpOrderItemId(y.getId());

                    paymentOndcOrderAttrs.setItemId(y.getId());

                    paymentOndcOrderAttrs.setNpOrderId(x.getOrderId());

                    paymentOndcOrderAttrs.setQuantity(y.getQuantity().getCount());

                    paymentOndcOrderAttrs.setStatus(x.getState());

                    if (y.getName() != null) {   //this is case for old orders
                        paymentOndcOrderAttrs.setSkuName(y.getName());
                    } else {
                        if (y.getDescriptor() != null) {
                            paymentOndcOrderAttrs.setSkuName(y.getDescriptor().getName());
                        }
                    }

                    paymentOndcOrderAttrs.setOrderReturnPeriodExpiryDate(null);

                    paymentOndcOrderAttrs.setSettlementDueDate(x.getExpectedSettlementDate());
                    paymentOndcOrderAttrs.setActualSettlementDate(x.getActualSettlementDate());

                    paymentOndcOrderAttrs.setBuyerFinderFee(String.valueOf(bffOnOrder));

                    if (x.getQuote() != null) {
                        paymentOndcOrderAttrs.setTotalOrderValue(x.getQuote().getPrice().getValue());
                        paymentOndcOrderAttrs.setMerchantPayableAmount(String.valueOf(Double.parseDouble(x.getQuote().getPrice().getValue())-bffOnOrder)); //Total - Buyer finder fee

                        x.getQuote().getBreakup().forEach(breakup -> {
                            if (breakup.getTitle() != null) {
                                if (breakup.getTitle().equals("Delivery Charges")) {
                                    paymentOndcOrderAttrs.setTotalShippingCharges(breakup.getPrice().getValue());
                                }

                                if (breakup.getAtOndcorgtitleType() != null) {
                                    if (breakup.getAtOndcorgtitleType().name().equals("TAX")) {
                                        paymentOndcOrderAttrs.setTaxApplicable(breakup.getPrice().getValue());
                                    }
                                    if (breakup.getAtOndcorgtitleType().name().equals("ITEM")) {
                                        paymentOndcOrderAttrs.setTotalItemValue(breakup.getPrice().getValue());
                                    }
                                    if (breakup.getAtOndcorgtitleType().name().equals("CONVENIENCE_CHARGE")) {
                                        paymentOndcOrderAttrs.setConvenienceCharges(breakup.getPrice().getValue());
                                    }
                                    if (breakup.getAtOndcorgtitleType().name().equals("PACKING")) {
                                        paymentOndcOrderAttrs.setPackagingCharges(breakup.getPrice().getValue());
                                    }
                                }
                            }
                        });

                    }

                    paymentOndcOrderAttrsList.add(paymentOndcOrderAttrs);

                });


            });

            return paymentOndcOrderAttrsList;
    }

    public List<ComplaintTicketsAttrs> getComplaintTickets() {

        AggregationOperation match = Aggregation.match(new Criteria("ondcOrder").is(true));
        String addFieldQuery = "{\n" +
                "  $addFields:\n" +
                "    {\n" +
                "      localFieldString: {\n" +
                "        $toString: \"$orderId\",\n" +
                "      }\n" +
                "    },\n" +
                "}";

        AggregationOperation orderLookup = Aggregation.lookup("ondcOrder", "localFieldString", "_id", "ondcOrderData");
        AggregationOperation unwind = Aggregation.unwind("ondcOrderData", true);

        String projectQuery = "{\n" +
                "  $project: {\n" +
                "  \"networkOrderId\": '$orderId',\n" +
                "  \"ticketId\": '$_id',\n" +
                "  \"orderId\": '$orderId',\n" +
                "  \"buyerNP\": 'QADDOO',\n" +
                "  \"sellerNP\": '$ondcOrderData.sellerNP',\n" +
                "  \"logisticsNP\": \"\",\n" +
                "  \"ticketCreationDate\": { $dateToString: { format: \"%d-%m-%Y\", date: \"$complaintTime\" } }," +
                "  \"ticketCreationTime\": { $dateToString: { format: \"%H:%M:%S\", date: \"$complaintTime\" } }," +
                "  \"ticketStatus\": '$complaintStatus',\n" +
                "  \"issueCategory\": '$category',\n" +
                "  \"orderCategory\": '$ondcOrderData.orderCategory',\n" +
                "  \"ticketLastUpdateDate\": { $dateToString: { format: \"%d-%m-%Y\", date: \"$updatedAt\" } }," +
                "  \"ticketLastUpdateTime\": { $dateToString: { format: \"%H:%M:%S\", date: \"$updatedAt\" } }," +
                "  \"ticketClosureDate\": { $dateToString: { format: \"%d-%m-%Y\", date: \"$closeTime\" } }," +
                "  \"ticketClosureTime\": { $dateToString: { format: \"%H:%M:%S\", date: \"$closeTime\" } }," +
//                "  \"ticketRelayDate\": {\n" +
//                "    $cond:{\n" +
//                "      if: { $eq: [\"$requestCascaded\", true] }, \n" +
//                "      then: { $dateToString: { format: \"%d-%m-%Y\", date: \"$com\" }, \n" +
//                "      else: \"$valueIfFalse\"\n" +
//                "    }\n" +
//                "  }\n" +
                "}}";

        Aggregation aggregation = Aggregation.newAggregation(
                match,
                new CustomProjectAggregationOperation(addFieldQuery),
                orderLookup,
                unwind,
                new CustomProjectAggregationOperation(projectQuery)
        );

        return mongoTemplate.aggregate(aggregation, "complaint_ticket", ComplaintTicketsAttrs.class).getMappedResults();
    }

    private static double buyerFinderFeeCalculation(Quotation quotation, int percent) {

        double itemTotal = 0;   //this includes BFF on product
        double deliveryCharges = 0;  //this includes BFF on logistics charges
        for (Quotation.QuotationBreakup x : quotation.getBreakup()) {
            if (x.getAtOndcorgtitleType().toString().equals("delivery")) {
                deliveryCharges += Double.parseDouble(String.valueOf(x.getPrice().getValue()));
            } else if (x.getAtOndcorgtitleType().toString().equals("item")) {
                itemTotal += Double.parseDouble(String.valueOf(x.getPrice().getValue()));
            }
        }

        // x + percent*x/100 = itemTotal
        double actualItemsCost = itemTotal/(1+((double)percent/100));
        double actualDeliveryCost = deliveryCharges/(1+((double)percent/100));


        double bff = 0;
        bff +=itemTotal-actualItemsCost;
        bff +=deliveryCharges-actualDeliveryCost;

        return (double) Math.round(bff * 100) / 100;  //round off upto 2 decimal
    }

    public List<NotificationDTO> getDashboardNotifications() {

        return mongoTemplate.find(new Query(Criteria.where("isDashboardNotification").is(true)), NotificationDTO.class);
    }
}
