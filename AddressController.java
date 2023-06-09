package com.qaddoo.controller;

import com.qaddoo.pojo.entity.*;
import com.qaddoo.service.manager.AddressMgmt;
import com.qaddoo.service.util.ServiceUtils;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/address")
public class AddressController {

    private final AddressMgmt addressMgmt;

    public AddressController(AddressMgmt addressMgmt) {
        this.addressMgmt = addressMgmt;
    }

    @PostMapping(value = "/add")
    public AddAddressResponse addAddress(@RequestHeader("authToken") String authToken, @RequestBody AddAddressRequest request) {
        AddAddressResponse response = new AddAddressResponse();

        try {
            response = addressMgmt.addAddress(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Add Address", e);
        }
        return response;
    }

    @PutMapping(value = "/update")
    public GetAddressResponse updateAddress(@RequestHeader("authToken") String authToken, @RequestBody UpdateAddressRequest request) {

        GetAddressResponse response = new GetAddressResponse();

        try {
            response = addressMgmt.updateAddress(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Update User Address", e);
        }

        return response;
    }

    @GetMapping(value = "/default")
    public GetAddressResponse getDefaultAddress(@RequestHeader("authToken") String authToken) {

        GetAddressResponse response = new GetAddressResponse();

        try {
            response = addressMgmt.getDefaultAddress(ServiceUtils.createAuthObj(authToken));
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get Default Address", e);
        }
        return response;
    }

    @GetMapping(value = "/list")
    public ListAddressResponse listAddresses(@RequestHeader("authToken") String authToken) {

        ListAddressResponse response = new ListAddressResponse();

        try {
            response = addressMgmt.listAddresses(ServiceUtils.createAuthObj(authToken));
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List User Addresses", e);
        }

        return response;
    }

    @DeleteMapping(value = "/delete/{addressId}")
    public DeleteAddressResponse deleteAddress(@RequestHeader("authToken") String authToken,
                                               @PathVariable("addressId") long addressId) {

        DeleteAddressResponse response = new DeleteAddressResponse();

        try {
            response = addressMgmt.deleteAddress(ServiceUtils.createAuthObj(authToken), addressId);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Delete User Address", e);
        }

        return response;
    }
}
