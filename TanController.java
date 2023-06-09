package com.qaddoo.controller;

import com.qaddoo.pojo.entity.*;
import com.qaddoo.service.authenticate.Authenticate;
import com.qaddoo.service.manager.TanMgmt;
import com.qaddoo.service.util.ServiceUtils;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping(value = "/tan")
public class TanController {

    private final TanMgmt tanManager;

    public TanController(TanMgmt tanManager) {
        this.tanManager = tanManager;
    }

    // add tan
    @PostMapping(value = "/add")
    public CreateTanResponse createTan(@RequestHeader("authToken") String authToken,
                                       @RequestBody CreateTanRequest request) {
        CreateTanResponse response = new CreateTanResponse();
        try {
            response = tanManager.createTan(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Add Group", e);
        }
        return response;
    }

    // available for exploring without login
    //GetGroupDetail
    @GetMapping(value = "/get/{groupId}")
    public GroupDetailResponse getGroupDetails(@RequestHeader(value = "authToken", required = false) String authToken,
                                               @PathVariable("groupId") long groupId) {

        GroupDetailResponse response = new GroupDetailResponse();

        try {
            response = tanManager.getGroupDetails(ServiceUtils.createAuthObj(authToken), groupId);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get Group Details", e);
        }

        return response;
    }

    // update tan
    @PutMapping(value = "/update")
    public UpdateTanResponse updateTan(@RequestHeader("authToken") String authToken,
                                       @RequestBody UpdateTanRequest request) {
        UpdateTanResponse response = new UpdateTanResponse();
        try {
            response = tanManager.updateTan(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Update Group", e);
        }
        return response;
    }

    // join tan
    @PutMapping(value = "/join")
    public JoinTanResponse joinTan(@RequestHeader("authToken") String authToken,
                                   @RequestBody JoinTanRequest request) {
        JoinTanResponse response = new JoinTanResponse();
        try {
            response = tanManager.joinTan(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Join Tan", e);
        }
        return response;
    }

    // join private group
    @PutMapping(value = "/join/private")
    public JoinTanResponse joinPrivateGroup(@RequestHeader("authToken") String authToken,
                                            @RequestBody JoinPrivateGroupRequest request) {
        JoinTanResponse response = new JoinTanResponse();
        try {
            response = tanManager.joinPrivateGroup(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Join Private Group", e);
        }
        return response;
    }

    // available for exploring without login
    @Deprecated
    @PutMapping(value = "/list")
    public ListTanResponse listTan(@RequestHeader(value = "authToken", required = false) String authToken,
                                   @RequestBody ListTanRequest request) {
        ListTanResponse response = new ListTanResponse();
        try {
            response = tanManager.listTanByDistance(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List Groups", e);
        }
        return response;
    }

    // available for exploring without login
    @PutMapping(value = "/list/v1")
    public ListTanResponse listTanV1(@RequestHeader(value = "authToken", required = false) String authToken,
                                     @RequestParam("page") int page, @RequestBody ExploreMediaRequest request) {
        ListTanResponse response = new ListTanResponse();
        try {
            response = tanManager.listTanByDistanceV1(ServiceUtils.createAuthObj(authToken), request, page);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List Groups", e);
        }
        return response;
    }

    @GetMapping(value = "/mytans")
    public ListTanResponse listMyTans(@RequestHeader("authToken") String authToken) {
        ListTanResponse response = new ListTanResponse();

        try {
            response = tanManager.listMyTans(ServiceUtils.createAuthObj(authToken));
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List my Groups", e);
        }

        return response;
    }

    @GetMapping(value = "/visited")
    public ListTanResponse listVisitedTans(@RequestHeader("authToken") String authToken) {
        ListTanResponse response = new ListTanResponse();

        try {
            response = tanManager.listVisitedTans(ServiceUtils.createAuthObj(authToken));
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List visited Groups", e);
        }

        return response;
    }

    @PostMapping(value = "/addAdmin")
    public AdminResponse addAdmin(@RequestHeader("authToken") String authToken,
                                  @RequestBody AdminRequest request) {

        AdminResponse response = new AdminResponse();

        try {
            response = tanManager.addAdmin(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Add Admin", e);
        }

        return response;
    }

    @PostMapping(value = "/removeAdmin")
    public AdminResponse removeAdmin(@RequestHeader("authToken") String authToken,
                                     @RequestBody AdminRequest request) {

        AdminResponse response = new AdminResponse();

        try {
            response = tanManager.removeAdmin(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Remove Admin", e);
        }

        return response;
    }

    // this endpoint was used to update the location of existing store/groups in the database as a point
    // was used only once, no longer used and required
    @Deprecated
    @PutMapping(value = "/add/location")
    public SuccessResponse addTanLocation(@RequestParam(value = "password") String password) {
        SuccessResponse response = new SuccessResponse();
        response.setMessage("No operation performed");

//        try {
//            response = tanManager.addTanLocation(password);
//        } catch (Exception e) {
//            return ServiceUtils.setResponse(response, false, "Add tan location", e);
//        }

        return response;
    }

    @CrossOrigin(origins = {"https://www.qaddoo.com", "https://www.qaddoo.com/staging/2751/qaddoowebsites"})
    @PutMapping(value = "/list/explore/media")
    public ExploreMediaResponse listExploreMedia(@RequestHeader(value = "authToken", required = false) String authToken,
                                                 @RequestParam(value = "exploreUser", required = false) boolean exploreUser,
                                                 @RequestParam(value = "city", defaultValue = "") String city,
                                                 @RequestParam(value = "state", defaultValue = "") String state,
                                                 @RequestParam(value = "country", defaultValue = "") String country,
                                                 @RequestParam(value = "pincode", defaultValue = "") String pincode,
                                                 @RequestParam(value = "language", defaultValue = "english") String language,
                                                 @RequestParam("page") int page, @RequestBody ExploreMediaRequest request) {
        ExploreMediaResponse response = new ExploreMediaResponse();

        try {
            response = tanManager.listExploreMedia(ServiceUtils.createAuthObj(authToken), request, language,
                    exploreUser, page, city, state, country, pincode);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List Explore Media", e);
        }

        return response;
    }

    @PutMapping(value = "/markFavourite")
    public SuccessResponse markFavourite(@RequestParam("tanId") long tanId, @RequestParam("isFavourite") boolean isFavourite,
                                         HttpServletRequest servletRequest) {
        SuccessResponse response = new SuccessResponse();
        Authenticate auth = ServiceUtils.createAuthObj(servletRequest.getHeader("authToken"));

        try {
            response = tanManager.markFavourite(auth, tanId, isFavourite);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Mark Favourite", e);
        }

        return response;
    }

    @GetMapping(value = "/get/favourite")
    public ListTanResponse getFavouriteGroups(@RequestParam("page") int page, HttpServletRequest servletRequest) {
        ListTanResponse response = new ListTanResponse();
        Authenticate auth = ServiceUtils.createAuthObj(servletRequest.getHeader("authToken"));

        try {
            response = tanManager.getFavouriteGroups(auth, page);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get Favourite Groups", e);
        }

        return response;
    }

}
