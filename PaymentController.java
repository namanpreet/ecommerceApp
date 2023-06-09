package com.qaddoo.controller;

import com.qaddoo.pojo.entity.*;
import com.qaddoo.service.manager.PaymentMgmt;
import com.qaddoo.service.util.ServiceUtils;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/payment")
public class PaymentController {

    private final PaymentMgmt paymentMgmt;

    public PaymentController(PaymentMgmt paymentMgmt) {
        this.paymentMgmt = paymentMgmt;
    }

    @PostMapping(value = "/add")
    public AddPaymentResponse addPayment(@RequestHeader("authToken") String authToken,
                                         @RequestBody AddPaymentRequest request) {

        AddPaymentResponse response = new AddPaymentResponse();

        try {
            response = paymentMgmt.addPayment(ServiceUtils.createAuthObj(authToken), request, false);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Add Payment", e);
        }

        return response;
    }

    @PutMapping(value = "/list")
    public ListStorePaymentsResponse listStorePayments(@RequestHeader("authToken") String authToken,
                                                       @RequestBody ListStorePaymentsRequest request) {

        ListStorePaymentsResponse response = new ListStorePaymentsResponse();

        try {
            response = paymentMgmt.listPayments(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List Store Payments", e);
        }

        return response;
    }

    // 12/10/2021 (4.0.10) and previous releases
    @PutMapping(value = "/getAccount")
    public UserAccountResponse getUserAccount(@RequestHeader("authToken") String authToken,
                                              @RequestBody UserAccountRequest request) {

        UserAccountResponse response = new UserAccountResponse();

        try {
            response = paymentMgmt.getUserAccount(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get User Account", e);
        }

        return response;
    }

    // 2/11/2021 release (4.0.11)
    @PutMapping(value = "/getAccount/v1")
    public UserAccountResponse getUserAccountV1(@RequestHeader("authToken") String authToken, @RequestParam("page") int page,
                                                @RequestBody UserAccountRequest request) {

        UserAccountResponse response = new UserAccountResponse();

        try {
            response = paymentMgmt.getUserAccountV1(ServiceUtils.createAuthObj(authToken), request, page);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get User Account", e);
        }

        return response;
    }

    @PutMapping(value = "/getTransaction")
    public ProfileTransactionResponse getTransaction(@RequestHeader("authToken") String authToken, @RequestParam("page") int page,
                                                     @RequestBody ProfileTransactionRequest request) {
        ProfileTransactionResponse response = new ProfileTransactionResponse();
        try {
            response = paymentMgmt.getUserTransaction(ServiceUtils.createAuthObj(authToken), request, page);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get user transactions", e);
        }
        return response;
    }

    @PutMapping(value = "/paymentConfirmationNotification")
    public SuccessResponse paymentConfirmationNotification(@RequestHeader("authToken") String authToken,
                                                           @RequestBody PaymentRequest request) {
        SuccessResponse response = new SuccessResponse();
        try {
            response = paymentMgmt.paymentConfirmationNotification(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "x", e);
        }
        return response;
    }


}
