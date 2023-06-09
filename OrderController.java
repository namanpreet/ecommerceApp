package com.qaddoo.controller;

import com.qaddoo.pojo.entity.*;
import com.qaddoo.service.manager.OrderMgmt;
import com.qaddoo.service.util.ServiceUtils;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/order")
public class OrderController {

    private final OrderMgmt orderMgmt;

    public OrderController(OrderMgmt orderMgmt) {
        this.orderMgmt = orderMgmt;
    }

    @PostMapping(value = "/place")
    public PlaceOrderResponse placeOrder(@RequestHeader("authToken") String authToken, @RequestBody PlaceOrderRequest request) {
        PlaceOrderResponse response = new PlaceOrderResponse();

        try {
            response = orderMgmt.placeOrder(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Place Order", e);
        }

        return response;
    }

    // 12/10/2021 (4.0.10) and previous releases
    @Deprecated
    @PostMapping(value = "/userOrderList")
    public ListOrderResponse getUserOrderList(@RequestHeader("authToken") String authToken,
                                              @RequestBody ListOrderRequest request) {
        ListOrderResponse response = new ListOrderResponse();

        try {
            response = orderMgmt.getUserOrderList(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "User Order List", e);
        }

        return response;
    }

    // 2/11/2021 release (4.0.11)
    @PostMapping(value = "/userOrderList/v1")
    public ListOrderResponse getUserOrderListV1(@RequestHeader("authToken") String authToken, @RequestParam("page") int page,
                                                @RequestBody ListOrderRequest request) {
        ListOrderResponse response = new ListOrderResponse();

        try {
            response = orderMgmt.getUserOrderListV1(ServiceUtils.createAuthObj(authToken), request, page);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "User Order List", e);
        }

        return response;
    }

    // 12/10/2021 (4.0.10) and previous releases
    @Deprecated
    @PostMapping(value = "/storeOrderList")
    public ListOrderResponse getStoreOrderList(@RequestHeader("authToken") String authToken,
                                               @RequestBody ListOrderRequest request) {

        ListOrderResponse response = new ListOrderResponse();

        try {
            response = orderMgmt.getStoreOrderList(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get Store Order List", e);
        }

        return response;
    }

    // 2/11/2021 release (4.0.11)
    @PostMapping(value = "/storeOrderList/v1")
    public ListOrderResponse getStoreOrderListV1(@RequestHeader("authToken") String authToken, @RequestParam("page") int page,
                                                 @RequestBody ListOrderRequest request) {

        ListOrderResponse response = new ListOrderResponse();

        try {
            response = orderMgmt.getStoreOrderListV1(ServiceUtils.createAuthObj(authToken), request, page);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get Store Order List", e);
        }

        return response;
    }

    // 12/10/2021 (4.0.10) and previous releases
    @Deprecated
    @GetMapping(value = "/allUserOrders")
    public ListOrderResponse getAllUserOrders(@RequestHeader("authToken") String authToken) {

        ListOrderResponse response = new ListOrderResponse();

        try {
            response = orderMgmt.getAllUserOrders(ServiceUtils.createAuthObj(authToken));
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get All User Orders", e);
        }

        return response;
    }

    // 2/11/2021 release (4.0.11)
    @GetMapping(value = "/allUserOrders/v1")
    public ListOrderResponse getAllUserOrdersV1(@RequestHeader("authToken") String authToken, @RequestParam("page") int page) {

        ListOrderResponse response = new ListOrderResponse();

        try {
            response = orderMgmt.getAllUserOrdersV1(ServiceUtils.createAuthObj(authToken), page);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get All User Orders", e);
        }

        return response;
    }

    @GetMapping(value = "/allAdminOrders")
    public ListOrderResponse getAllAdminOrders(@RequestHeader("authToken") String authToken) {

        ListOrderResponse response = new ListOrderResponse();

        try {
            response = orderMgmt.getAllAdminOrders(ServiceUtils.createAuthObj(authToken));
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get All Admin Orders", e);
        }

        return response;
    }

    @GetMapping(value = "/allAdminOrders/v1")
    public ListOrderResponse getAllAdminOrdersV1(@RequestHeader("authToken") String authToken, @RequestParam("page") int page) {

        ListOrderResponse response = new ListOrderResponse();

        try {
            response = orderMgmt.getAllAdminOrdersV1(ServiceUtils.createAuthObj(authToken), page);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get All Admin Orders", e);
        }

        return response;
    }

    @GetMapping(value = "/get/{orderId}")
    public GetOrderDetailsResponse getOrderDetails(@RequestHeader("authToken") String authToken,
                                                   @PathVariable("orderId") long orderId) {

        GetOrderDetailsResponse response = new GetOrderDetailsResponse();

        try {
            response = orderMgmt.getOrderDetails(ServiceUtils.createAuthObj(authToken), orderId);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get Order Details", e);
        }

        return response;
    }

    @GetMapping(value = "/saved/{storeId}")
    public GetSavedOrderResponse getSavedOrder(@RequestHeader("authToken") String authToken,
                                               @PathVariable("storeId") long storeId) {

        GetSavedOrderResponse response = new GetSavedOrderResponse();

        try {
            response = orderMgmt.getSavedOrder(ServiceUtils.createAuthObj(authToken), storeId);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get Saved Order", e);
        }

        return response;
    }


    @PutMapping(value = "/update")
    public UpdateOrderResponse updateOrder(@RequestHeader("authToken") String authToken,
                                           @RequestBody UpdateOrderRequest request) {

        UpdateOrderResponse response = new UpdateOrderResponse();

        try {
            response = orderMgmt.updateOrder(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Update Order", e);
        }

        return response;
    }

    @PutMapping(value = "/update/v1")
    public UpdateOrderResponse updateOrderV1(@RequestHeader("authToken") String authToken,
                                           @RequestBody UpdateOrderRequest request) {

        UpdateOrderResponse response = new UpdateOrderResponse();

        try {
            response = orderMgmt.updateOrderV1(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Update Order", e);
        }

        return response;
    }

    @PutMapping(value = "/favourite/add")
    public FavouriteItemsResponse addFavouriteItems(@RequestHeader("authToken") String authToken,
                                                    @RequestBody FavouriteItemsRequest request) {

        FavouriteItemsResponse response = new FavouriteItemsResponse();

        try {
            response = orderMgmt.addFavouriteItems(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Add Favourite Items", e);
        }

        return response;
    }

    @PutMapping(value = "/favourite/remove")
    public FavouriteItemsResponse removeFavouriteItems(@RequestHeader("authToken") String authToken,
                                                       @RequestBody FavouriteItemsRequest request) {

        FavouriteItemsResponse response = new FavouriteItemsResponse();

        try {
            response = orderMgmt.removeFavouriteItems(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Remove favourite items", e);
        }

        return response;
    }

    @GetMapping(value = "/favourite/get/{storeId}")
    public GetFavouriteItemsResponse getFavouriteItems(@RequestHeader("authToken") String authToken,
                                                       @PathVariable("storeId") long storeId) {

        GetFavouriteItemsResponse response = new GetFavouriteItemsResponse();

        try {
            response = orderMgmt.getFavouriteItems(ServiceUtils.createAuthObj(authToken), storeId);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get Favourite Items", e);
        }

        return response;
    }

}
