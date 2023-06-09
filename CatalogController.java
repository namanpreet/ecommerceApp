package com.qaddoo.controller;

import com.qaddoo.pojo.entity.*;
import com.qaddoo.service.manager.CatalogMgmt;
import com.qaddoo.service.util.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping(value = "/catalog")
public class CatalogController {

    private static final Logger logger = LoggerFactory.getLogger(CatalogController.class);

    private final CatalogMgmt catalogMgmt;

    public CatalogController(CatalogMgmt catalogMgmt) {
        this.catalogMgmt = catalogMgmt;
    }

    // endpoint to add products to global catalog
    @PutMapping(value = "/add")
    public AddProductsResponse addProducts(@RequestHeader("authToken") String authToken,
                                    @RequestBody AddProductsRequest request) {
        logger.info("Request on endpoint /catalog/add = {}", request);
        AddProductsResponse response = new AddProductsResponse();

        try {
            response = catalogMgmt.addProducts(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Add Products in Catalog", e);
        }

        logger.info("Exiting /catalog/add with response = {}", response);
        return response;
    }

    // endpoint to list global catalog products
    // 12/10/2021 (4.0.10) and previous releases
    @Deprecated
    @PutMapping(value = "/list")
    public ListProductsResponse listProducts(@RequestHeader("authToken") String authToken,
                                      @RequestBody ListProductsRequest request) {
        logger.info("Request on endpoint /catalog/list = {}", request);
        ListProductsResponse response = new ListProductsResponse();

        try {
            response = catalogMgmt.listProducts(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List Catalog Products", e);
        }

        logger.info("Exiting /catalog/list with response = {}", response);
        return response;
    }

    // endpoint to list global catalog products
    // 12/10/2021 (4.0.10) to 14/12/2021 (4.0.14) release
    @Deprecated
    @PutMapping(value = "/list/v1", params = {"page"})
    public 
    ListProductsResponse listProductsV1(@RequestHeader("authToken") String authToken, @RequestParam("page") int page,
                                        @RequestBody ListProductsRequest request) {
        logger.info("Request on endpoint /catalog/list/v1 = {}", request);
        ListProductsResponse response = new ListProductsResponse();

        try {
            response = catalogMgmt.listProductsV1(ServiceUtils.createAuthObj(authToken), request, page);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List Catalog Products", e);
        }

        logger.info("Exiting /catalog/list/v1 with response = {}", response);
        return response;
    }

    // endpoint to list global catalog products
    // 30/12/2021 release (4.0.15)
    @PutMapping(value = "/list/v2")
    public ListProductsResponse listProductsV2(@RequestHeader("authToken") String authToken, @RequestBody ListProductsRequest request) {
        logger.info("Request on endpoint /catalog/list/v2 = {}", request);
        ListProductsResponse response = new ListProductsResponse();

        try {
            response = catalogMgmt.listProductsV2(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List Catalog Products", e);
        }

        logger.info("Exiting /catalog/list/v2 with response = {}", response);
        return response;
    }

    // endpoint to set the country for products in global catalog
    // 14/12/2021 (4.0.14) release to 13/06/22 (4.0.24) release
    @Deprecated
    @PutMapping(value = "/product/country", params = {"password"})
    public AddProductsResponse addProductCountry(@RequestParam("password") String password) {
        logger.info("Request on endpoint /catalog/product/country");
        AddProductsResponse response = new AddProductsResponse();

        try {
            response = catalogMgmt.addProductCountry(password);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Add Product Country", e);
        }

        logger.info("Exiting /catalog/product/country");
        return response;
    }

    // endpoint to add a particular language name and description to global catalog products
    @PutMapping(value = "/product/nameAndDescription")
    public SuccessResponse setLanguageNameAndDescription(@RequestParam("language") String language, HttpServletRequest servletRequest) {
        logger.info("Request on endpoint /catalog/product/nameAndDescription");
        String password = servletRequest.getHeader("password");
        SuccessResponse response = new SuccessResponse();

        try {
            response = catalogMgmt.setLanguageNameAndDescription(password, language);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Set Global Products Language Name and Description", e);
        }

        logger.info("Exiting /catalog/product/nameAndDescription");
        return  response;
    }
}
