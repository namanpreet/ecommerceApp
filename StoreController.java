package com.qaddoo.controller;

import com.qaddoo.pojo.entity.*;
import com.qaddoo.service.authenticate.Authenticate;
import com.qaddoo.service.manager.StoreMgmt;
import com.qaddoo.service.util.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

@RestController
@RequestMapping(value = "/store")
public class StoreController {

    private static final Logger logger = LoggerFactory.getLogger(StoreController.class);

    private final StoreMgmt storeMgmt;

    public StoreController(StoreMgmt storeMgmt) {
        this.storeMgmt = storeMgmt;
    }

    @PostMapping(value = "/add")
    public CreateStoreResponse createStore(@RequestHeader("authToken") String authToken, @RequestBody CreateStoreRequest request) {
        CreateStoreResponse response = new CreateStoreResponse();

        try {
            response = storeMgmt.createStore(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Create Store", e);
        }
        return response;
    }

    @PostMapping(value = "/scraped/add")
    public SuccessResponse createScrapedStores(@RequestParam("password") String password, @RequestBody CreateScrapedStoresRequest request) {
        SuccessResponse response = new SuccessResponse();

        try {
            response = storeMgmt.createScrapedStores(request, password);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Create scraped stores", e);
        }
        return response;
    }

    @PutMapping(value = "/owner/change")
    public SuccessResponse changeStoreOwner(@RequestHeader("authToken") String authToken, @RequestBody AdminRequest request) {
        SuccessResponse response = new SuccessResponse();

        try {
            response = storeMgmt.changeStoreOwner(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Change store owner", e);
        }

        return response;
    }

    @PutMapping(value = "/update")
    public SuccessResponse updateStore(@RequestHeader("authToken") String authToken, @RequestBody UpdateStoreRequest request) {
        SuccessResponse response = new SuccessResponse();

        try {
            response = storeMgmt.updateStore(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Update Store", e);
        }
        return response;
    }

    // available for exploring without login
    @Deprecated
    @PutMapping(value = "/list")
    public ListStoreResponse listAllStores(@RequestHeader(value = "authToken", required = false) String authToken,
                                           @RequestParam(value = "language", defaultValue = "english") String language,
                                           @RequestBody ListStoreRequest request) {
        ListStoreResponse response = new ListStoreResponse();

        try {
            response = storeMgmt.listStoresByDistance(ServiceUtils.createAuthObj(authToken), request, language);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List All Stores", e);
        }

        return response;
    }

    // available for exploring without login
    @PutMapping(value = "/list/v1")
    public ListStoreResponse listAllStoresV1(@RequestHeader(value = "authToken", required = false) String authToken,
                                             @RequestParam(value = "language", defaultValue = "english") String language,
                                             @RequestParam("page") int page, @RequestBody ExploreMediaRequest request) {
        ListStoreResponse response = new ListStoreResponse();

        try {
            response = storeMgmt.listStoresByDistanceV1(ServiceUtils.createAuthObj(authToken), request, language, page);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List All Stores", e);
        }

        return response;
    }

    // available for exploring without login
    @PutMapping(value = "/search/category")
    public ListStoreResponse searchStoresByCategory(@RequestHeader(value = "authToken", required = false) String authToken,
                                                    @RequestParam(value = "language", defaultValue = "english") String language,
                                                    @RequestParam("page") int page, @RequestBody SearchStoresRequest request) {
        ListStoreResponse response = new ListStoreResponse();

        try {
            response = storeMgmt.searchStoresByCategory(ServiceUtils.createAuthObj(authToken), request, language, page);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Search stores by category", e);
        }

        return response;
    }

    // available for exploring without login
    @PutMapping(value = "/search/name")
    public StoresAndProductsResponse searchStoresAndProductsByName(@RequestHeader(value = "authToken", required = false) String authToken,
                                                                   @RequestParam(value = "language", defaultValue = "english") String language,
                                                                   @RequestParam("page") int page, @RequestBody ExploreMediaRequest request) {
        StoresAndProductsResponse response = new StoresAndProductsResponse();

        try {
            response = storeMgmt.searchStoresAndProductsByName(ServiceUtils.createAuthObj(authToken), request, language, page);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Search stores and products by name", e);
        }

        return response;
    }

    // available for exploring without login
    @PutMapping(value = "/search/itemName")
    public ListStoreResponse searchStoresByItem(@RequestHeader(value = "authToken", required = false) String authToken,
                                                @RequestParam(value = "language", defaultValue = "english") String language,
                                                @RequestParam("page") int page, @RequestBody ExploreMediaRequest request) {
        ListStoreResponse response = new ListStoreResponse();

        try {
            response = storeMgmt.searchStoresByItem(ServiceUtils.createAuthObj(authToken), request, language, page);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Search stores by item name", e);
        }

        return response;
    }

    @GetMapping(value = "/mystores")
    public ListStoreResponse listMyStores(@RequestHeader("authToken") String authToken) {
        ListStoreResponse response = new ListStoreResponse();

        try {
            response = storeMgmt.listMyStores(ServiceUtils.createAuthObj(authToken));
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List my Stores", e);
        }

        return response;
    }

    @PostMapping(value = "/products/add/{storeId}")
    public ItemResponse addStoreProducts(@RequestHeader("authToken") String authToken, @PathVariable("storeId") long storeId,
                                         @RequestBody MultipartFile csvFile, @RequestBody List<MultipartFile> imageFile) {

        ItemResponse response = new ItemResponse();

        try {
            response = storeMgmt.addStoreProducts(ServiceUtils.createAuthObj(authToken), storeId, imageFile, csvFile);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Add products to store catalog", e);
        }

        return response;
    }

    // 12/10/2021 (4.0.10) and previous releases
    @Deprecated
    @GetMapping(value = "/items/{storeId}")
    public ListStoreItemsResponse listStoreItems(@RequestHeader("authToken") String authToken,
                                                 @PathVariable("storeId") long storeId) {

        ListStoreItemsResponse response = new ListStoreItemsResponse();

        try {
            response = storeMgmt.listStoreItems(ServiceUtils.createAuthObj(authToken), storeId);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List Store Items", e);
        }

        return response;
    }

    // available for exploring without login
    // 2/11/2021 release (4.0.11)
    @PutMapping(value = "/items/v1")
    public ListStoreItemsResponse listStoreItemsV1(@RequestHeader(value = "authToken", required = false) String authToken,
                                                   @RequestParam(value = "language", defaultValue = "english") String language,
                                                   @RequestParam("page") int page, @RequestBody ListStoreItemsRequest request) {

        ListStoreItemsResponse response = new ListStoreItemsResponse();

        try {
            response = storeMgmt.listStoreItemsV1(ServiceUtils.createAuthObj(authToken), request, language, page);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List Store Items", e);
        }

        return response;
    }

    // 12/10/2021 (4.0.10) and previous releases
    @Deprecated
    @GetMapping(value = "/get/{storeId}")
    public GetStoreDetailResponse getStoreDetails(@RequestHeader("authToken") String authToken,
                                                  @PathVariable("storeId") long storeId) {

        GetStoreDetailResponse response = new GetStoreDetailResponse();

        try {
            response = storeMgmt.getStoreDetails(ServiceUtils.createAuthObj(authToken), storeId);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get Store Details", e);
        }

        return response;
    }

    // available for exploring without login
    // 2/11/2021 release (4.0.11)
    @PutMapping(value = "/get/v1")
    public GetStoreDetailResponse getStoreDetailsV1(@RequestHeader(value = "authToken", required = false) String authToken,
                                                    @RequestParam(value = "language", defaultValue = "english") String language,
                                                    @RequestBody GetStoreDetailRequest request) {

        GetStoreDetailResponse response = new GetStoreDetailResponse();

        try {
            response = storeMgmt.getStoreDetailsV1(ServiceUtils.createAuthObj(authToken), request, language);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get Store Details", e);
        }

        return response;
    }

    // 30/12/2021 release (4.0.15)
    @PutMapping(value = "/catalogue/add")
    public ItemResponse addCatalogue(@RequestHeader("authToken") String authToken, @RequestBody ItemsRequest request) {

        ItemResponse response = new ItemResponse();

        try {
            response = storeMgmt.addCatalogue(ServiceUtils.createAuthObj(authToken), request, false);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Add Catalogue", e);
        }

        return response;
    }

    // 14/12/2021 and previous releases (4.0.14)
    @Deprecated
    @PutMapping(value = "/catalogue")
    public ItemResponse updateCatalogue(@RequestHeader("authToken") String authToken,
                                        @RequestBody ItemsRequest request) {

        ItemResponse response = new ItemResponse();

        try {
            response = storeMgmt.updateCatalogue(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Update Catalogue", e);
        }

        return response;
    }

    // 30/12/2021 release (4.0.15)
    @PutMapping(value = "/catalogue/update/v1")
    public ItemResponse updateCatalogueV1(@RequestHeader("authToken") String authToken,
                                          @RequestBody ItemsRequest request) {

        ItemResponse response = new ItemResponse();

        try {
            response = storeMgmt.updateCatalogueV1(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Update Catalogue", e);
        }

        return response;
    }

    @PutMapping(value = "/removeItems")
    public ItemResponse removeItems(@RequestHeader("authToken") String authToken,
                                    @RequestBody ItemsRequest request) {

        ItemResponse response = new ItemResponse();

        try {
            response = storeMgmt.removeItems(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Remove Items", e);
        }

        return response;
    }

    // 14/12/2021 release (14.0.14)
    @PutMapping(value = "/khata/add")
    public KhataResponse addKhata(@RequestHeader("authToken") String authToken,
                                  @RequestBody AddKhataRequest request) {

        KhataResponse response = new KhataResponse();

        try {
            response = storeMgmt.addKhata(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Add Khata", e);
        }

        return response;
    }

    @PutMapping(value = "/addReferralCode")
    public SuccessResponse addReferralCode(HttpServletRequest servletRequest) {
        SuccessResponse response = new SuccessResponse();
        String password = servletRequest.getHeader("password");

        try {
            response = storeMgmt.addReferralCode(password);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Add Referral Code", e);
        }

        return response;
    }

    // endpoint to set specific language name and description of products in store catalog
    @PutMapping(value = "/catalogue/nameAndDescription")
    public SuccessResponse setLanguageNameAndDescription(@RequestParam("language") String language, HttpServletRequest servletRequest) {
        logger.info("Request on endpoint /store/catalogue/nameAndDescription");
        String password = servletRequest.getHeader("password");
        SuccessResponse response = new SuccessResponse();

        try {
            response = storeMgmt.setLanguageNameAndDescription(password, language);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Set Store Products Language Name and Description", e);
        }

        logger.info("Exiting /store/catalogue/nameAndDescription");
        return response;
    }

    @GetMapping(value = "/get/favourite")
    public ListStoreResponse getFavouriteStores(@RequestParam("page") int page, HttpServletRequest servletRequest) {
        ListStoreResponse response = new ListStoreResponse();
        Authenticate auth = ServiceUtils.createAuthObj(servletRequest.getHeader("authToken"));

        try {
            response = storeMgmt.getFavouriteStores(auth, page);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get Favourite Stores", e);
        }

        return response;
    }

}
